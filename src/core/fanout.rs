use std::{collections::HashMap, fmt, task::Poll, time::Instant};

use futures::{Stream, StreamExt};
use futures_util::{pending, poll};
use indexmap::IndexMap;
use tokio::sync::mpsc;
use tokio_util::sync::ReusableBoxFuture;
use crate::buffers::topology::channel::{BufferSender};
use agent_lib::{config::ComponentKey, event::EventArray};
/// doc
pub enum ControlMessage {
    /// Adds a new sink to the fanout.
    Add(ComponentKey, BufferSender<EventArray>),

    /// Removes a sink from the fanout.
    Remove(ComponentKey),

    /// Pauses a sink in the fanout.
    ///
    /// If a fanout has any paused sinks, subsequent sends cannot proceed until all paused sinks
    /// have been replaced.
    Pause(ComponentKey),

    /// Replaces a paused sink with its new sender.
    Replace(ComponentKey, BufferSender<EventArray>),
}

impl fmt::Debug for ControlMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ControlMessage::")?;
        match self {
            Self::Add(id, _) => write!(f, "Add({id:?})"),
            Self::Remove(id) => write!(f, "Remove({id:?})"),
            Self::Pause(id) => write!(f, "Pause({id:?})"),
            Self::Replace(id, _) => write!(f, "Replace({id:?})"),
        }
    }
}
/// doc
// TODO: We should really wrap this in a custom type that has dedicated methods for each operation
// so that high-lever components don't need to do the raw channel sends, etc.
pub type ControlChannel = mpsc::UnboundedSender<ControlMessage>;
/// doc
pub struct Fanout {
    senders: IndexMap<ComponentKey, Option<Sender>>,
    control_channel: mpsc::UnboundedReceiver<ControlMessage>,
}

impl Fanout {
    /// doc
    pub fn new() -> (Self, ControlChannel) {
        let (control_tx, control_rx) = mpsc::unbounded_channel();

        let fanout = Self {
            senders: Default::default(),
            control_channel: control_rx,
        };

        (fanout, control_tx)
    }

    /// Add a new sink as an output.
    ///
    /// # Panics
    ///
    /// Function will panic if a sink with the same ID is already present.
    pub fn add(&mut self, id: ComponentKey, sink: BufferSender<EventArray>) {
        assert!(
            !self.senders.contains_key(&id),
            "Adding duplicate output id to fanout: {id}"
        );
        self.senders.insert(id, Some(Sender::new(sink)));
    }

    fn remove(&mut self, id: &ComponentKey) {
        assert!(
            self.senders.shift_remove(id).is_some(),
            "Removing nonexistent sink from fanout: {id}"
        );
    }

    fn replace(&mut self, id: &ComponentKey, sink: BufferSender<EventArray>) {
        match self.senders.get_mut(id) {
            Some(sender) => {
                // While a sink must be _known_ to be replaced, it must also be empty (previously
                // paused or consumed when the `SendGroup` was created), otherwise an invalid
                // sequence of control operations has been applied.
                assert!(
                    sender.replace(Sender::new(sink)).is_none(),
                    "Replacing existing sink is not valid: {id}"
                );
            }
            None => panic!("Replacing unknown sink from fanout: {id}"),
        }
    }

    fn pause(&mut self, id: &ComponentKey) {
        match self.senders.get_mut(id) {
            Some(sender) => {
                // A sink must be known and present to be replaced, otherwise an invalid sequence of
                // control operations has been applied.
                assert!(
                    sender.take().is_some(),
                    "Pausing nonexistent sink is not valid: {id}"
                );
            }
            None => panic!("Pausing unknown sink from fanout: {id}"),
        }
    }

    /// Apply a control message directly against this instance.
    ///
    /// This method should not be used if there is an active `SendGroup` being processed.
    fn apply_control_message(&mut self, message: ControlMessage) {
        trace!("Processing control message outside of send: {:?}", message);

        match message {
            ControlMessage::Add(id, sink) => self.add(id, sink),
            ControlMessage::Remove(id) => self.remove(&id),
            ControlMessage::Pause(id) => self.pause(&id),
            ControlMessage::Replace(id, sink) => self.replace(&id, sink),
        }
    }

    /// Waits for all paused sinks to be replaced.
    ///
    /// Control messages are processed until all senders have been replaced, so it is guaranteed
    /// that when this method returns, all senders are ready for the next send to be triggered.
    async fn wait_for_replacements(&mut self) {
        while self.senders.values().any(Option::is_none) {
            if let Some(msg) = self.control_channel.recv().await {
                self.apply_control_message(msg);
            } else {
                // If the control channel is closed, there's nothing else we can do.

                // TODO: It _seems_ like we should probably panic here, or at least return.
                //
                // Essentially, we should only land here if the control channel is closed but we
                // haven't yet replaced all of the paused sinks... and we shouldn't have any paused
                // sinks if Vector is stopping normally/gracefully, so like... we'd only get
                // here during a configuration reload where we panicked in another thread due to
                // an error of some sort, and the control channel got dropped, closed itself, and
                // we're never going to be able to recover.
                //
                // The flipside is that by leaving it as-is, in the above hypothesized scenario,
                // we'd avoid emitting additional panics/error logging when the root cause error was
                // already doing so, like there's little value in knowing the fanout also hit an
                // unrecoverable state if the whole process is about to come crashing down
                // anyways... but it still does feel weird to have that encoded here by virtue of
                // only a comment, and not an actual terminating expression. *shrug*
            }
        }
    }

    /// Send a stream of events to all connected sinks.
    ///
    /// This function will send events until the provided stream finishes. It will also block on the
    /// resolution of any pending reload before proceeding with a send operation, similar to `send`.
    ///
    /// # Panics
    ///
    /// This method can panic if the fanout receives a control message that violates some invariant
    /// about its current state (e.g. remove a nonexistent sink, etc.). This would imply a bug in
    /// Vector's config reloading logic.
    ///
    /// # Errors
    ///
    /// If an error occurs while sending events to any of the connected sinks, an error variant will be
    /// returned detailing the cause.
    pub async fn send_stream(
        &mut self,
        events: impl Stream<Item = (EventArray, Instant)>,
    ) -> crate::Result<()> {
        tokio::pin!(events);
        while let Some((event_array, send_reference)) = events.next().await {
            self.send(event_array, Some(send_reference)).await?;
        }
        Ok(())
    }

    /// Send a batch of events to all connected sinks.
    ///
    /// This will block on the resolution of any pending reload before proceeding with the send
    /// operation.
    ///
    /// # Panics
    ///
    /// This method can panic if the fanout receives a control message that violates some invariant
    /// about its current state (e.g. remove a nonexistent sink, etc). This would imply a bug in
    /// Vector's config reloading logic.
    ///
    /// # Errors
    ///
    /// If an error occurs while sending events to any of the connected sinks, an error variant will be
    /// returned detailing the cause.
    pub async fn send(
        &mut self,
        events: EventArray,
        send_reference: Option<Instant>,
    ) -> crate::Result<()> {
        // First, process any available control messages in a non-blocking fashion.
        while let Ok(message) = self.control_channel.try_recv() {
            self.apply_control_message(message);
        }

        // Wait for any senders that are paused to be replaced first before continuing with the send.
        self.wait_for_replacements().await;

        // Nothing to send if we have no sender.
        if self.senders.is_empty() {
            trace!("No senders present.");
            return Ok(());
        }

        // Keep track of whether the control channel has returned `Ready(None)`, and stop polling
        // it once it has. If we don't do this check, it will continue to return `Ready(None)` any
        // time it is polled, which can lead to a busy loop below.
        //
        // In real life this is likely a non-issue, but it can lead to strange behavior in tests if
        // left unhandled.
        let mut control_channel_open = true;

        // Create our send group which arms all senders to send the given events, and handles
        // adding/removing/replacing senders while the send is in-flight.
        let mut send_group = SendGroup::new(&mut self.senders, events, send_reference);

        loop {
            tokio::select! {
                // Semantically, it's not hugely important that this select is biased. It does,
                // however, make testing simpler when you can count on control messages being
                // processed first.
                biased;

                maybe_msg = self.control_channel.recv(), if control_channel_open => {
                    trace!("Processing control message inside of send: {:?}", maybe_msg);

                    // During a send operation, control messages must be applied via the
                    // `SendGroup`, since it has exclusive access to the senders.
                    match maybe_msg {
                        Some(ControlMessage::Add(id, sink)) => {
                            send_group.add(id, sink);
                        },
                        Some(ControlMessage::Remove(id)) => {
                            send_group.remove(&id);
                        },
                        Some(ControlMessage::Pause(id)) => {
                            send_group.pause(&id);
                        },
                        Some(ControlMessage::Replace(id, sink)) => {
                            send_group.replace(&id, Sender::new(sink));
                        },
                        None => {
                            // Control channel is closed, which means Vector is shutting down.
                            control_channel_open = false;
                        }
                    }
                }

                result = send_group.send() => match result {
                    Ok(()) => {
                        trace!("Sent item to fanout.");
                        break;
                    },
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(())
    }
}

struct SendGroup<'a> {
    senders: &'a mut IndexMap<ComponentKey, Option<Sender>>,
    sends: HashMap<ComponentKey, ReusableBoxFuture<'static, crate::Result<Sender>>>,
}

impl<'a> SendGroup<'a> {
    fn new(
        senders: &'a mut IndexMap<ComponentKey, Option<Sender>>,
        events: EventArray,
        send_reference: Option<Instant>,
    ) -> Self {
        // If we don't have a valid `Sender` for all sinks, then something went wrong in our logic
        // to ensure we were starting with all valid/idle senders prior to initiating the send.
        debug_assert!(senders.values().all(Option::is_some));

        let last_sender_idx = senders.len().saturating_sub(1);
        let mut events = Some(events);

        // We generate a send future for each sender we have, which arms them with the events to
        // send but also takes ownership of the sender itself, which we give back when the sender completes.
        let mut sends = HashMap::new();
        for (i, (key, sender)) in senders.iter_mut().enumerate() {
            let mut sender = sender
                .take()
                .expect("sender must be present to initialize SendGroup");

            // First, arm each sender with the item to actually send.
            if i == last_sender_idx {
                sender.input = events.take();
            } else {
                sender.input.clone_from(&events);
            }
            sender.send_reference = send_reference;

            // Now generate a send for that sender which we'll drive to completion.
            let send = async move {
                sender.flush().await?;
                Ok(sender)
            };

            sends.insert(key.clone(), ReusableBoxFuture::new(send));
        }

        Self { senders, sends }
    }

    fn try_detach_send(&mut self, id: &ComponentKey) -> bool {
        if let Some(send) = self.sends.remove(id) {
            tokio::spawn(async move {
                if let Err(e) = send.await {
                    warn!(
                        cause = %e,
                        message = "Encountered error writing to component after detaching from topology.",
                    );
                }
            });
            true
        } else {
            false
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn add(&mut self, id: ComponentKey, sink: BufferSender<EventArray>) {
        // When we're in the middle of a send, we can only keep track of the new sink, but can't
        // actually send to it, as we don't have the item to send... so only add it to `senders`.
        assert!(
            self.senders
                .insert(id.clone(), Some(Sender::new(sink)))
                .is_none(),
            "Adding duplicate output id to fanout: {id}"
        );
    }

    fn remove(&mut self, id: &ComponentKey) {
        // We may or may not be removing a sender that we're try to drive a send against, so we have
        // to also detach the send future for the sender if it exists, otherwise we'd be hanging
        // around still trying to send to it.
        assert!(
            self.senders.shift_remove(id).is_some(),
            "Removing nonexistent sink from fanout: {id}"
        );

        // Now try and detach the in-flight send, if it exists.
        //
        // We don't ensure that a send was or wasn't detached because this could be called either
        // during an in-flight send _or_ after the send has completed.
        self.try_detach_send(id);
    }

    fn replace(&mut self, id: &ComponentKey, sink: Sender) {
        match self.senders.get_mut(id) {
            Some(sender) => {
                // While a sink must be _known_ to be replaced, it must also be empty (previously
                // paused or consumed when the `SendGroup` was created), otherwise an invalid
                // sequence of control operations has been applied.
                assert!(
                    sender.replace(sink).is_none(),
                    "Replacing existing sink is not valid: {id}"
                );
            }
            None => panic!("Replacing unknown sink from fanout: {id}"),
        }
    }

    fn pause(&mut self, id: &ComponentKey) {
        match self.senders.get_mut(id) {
            Some(sender) => {
                // If we don't currently own the `Sender` for the given component, that implies
                // there is an in-flight send: a `SendGroup` cannot be created without all
                // participating components having a send operation triggered.
                //
                // As such, `try_detach_send` should always succeed here, as pausing only occurs
                // when a component is being _replaced_, and should not be called multiple times.
                if sender.take().is_none() {
                    assert!(
                        self.try_detach_send(id),
                        "Pausing already-paused sink is invalid: {id}"
                    );
                }
            }
            None => panic!("Pausing unknown sink from fanout: {id}"),
        }
    }

    async fn send(&mut self) -> crate::Result<()> {
        // Right now, we do a linear scan of all sends, polling each send once in order to avoid
        // waiting forever, such that we can let our control messages get picked up while sends are
        // waiting.
        loop {
            if self.sends.is_empty() {
                break;
            }

            let mut done = Vec::new();
            for (key, send) in &mut self.sends {
                if let Poll::Ready(result) = poll!(send.get_pin()) {
                    let sender = result?;

                    // The send completed, so we restore the sender and mark ourselves so that this
                    // future gets dropped.
                    done.push((key.clone(), sender));
                }
            }

            for (key, sender) in done {
                self.sends.remove(&key);
                self.replace(&key, sender);
            }

            if !self.sends.is_empty() {
                // We manually yield ourselves because we've polled all of the sends at this point,
                // so if any are left, then we're scheduled for a wake-up... this is a really poor
                // approximation of what `FuturesUnordered` is doing.
                pending!();
            }
        }

        Ok(())
    }
}

struct Sender {
    inner: BufferSender<EventArray>,
    input: Option<EventArray>,
    send_reference: Option<Instant>,
}

impl Sender {
    fn new(inner: BufferSender<EventArray>) -> Self {
        Self {
            inner,
            input: None,
            send_reference: None,
        }
    }

    async fn flush(&mut self) -> crate::Result<()> {
        let send_reference = self.send_reference.take();
        if let Some(input) = self.input.take() {
            self.inner.send(input, send_reference).await?;
            self.inner.flush().await?;
        }

        Ok(())
    }
}