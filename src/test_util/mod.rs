#![allow(missing_docs)]
use std::{
    collections::HashMap,
    convert::Infallible,
    fs::File,
    future::{ready, Future},
    io::Read,
    iter,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use chrono::{DateTime, SubsecRound, Utc};
use flate2::read::MultiGzDecoder;
use futures::{stream, task::noop_waker_ref, FutureExt, SinkExt, Stream, StreamExt, TryStreamExt};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use portpicker::pick_unused_port;
use rand::{rng, Rng};
use rand_distr::Alphanumeric;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, Result as IoResult},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    runtime,
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tokio_stream::wrappers::TcpListenerStream;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::codec::{Encoder, FramedRead, FramedWrite, LinesCodec};
use vector_lib::event::{BatchNotifier, Event, EventArray, LogEvent, MetricTags, MetricValue};
use vector_lib::{
    buffers::topology::channel::LimitedReceiver,
    event::{Metric, MetricKind},
};

use crate::{
    config::{Config, GenerateConfig},
    topology::{RunningTopology, ShutdownErrorReceiver},
    trace,
};

const WAIT_FOR_SECS: u64 = 5; // The default time to wait in `wait_for`
const WAIT_FOR_MIN_MILLIS: u64 = 5; // The minimum time to pause before retrying
const WAIT_FOR_MAX_MILLIS: u64 = 500; // The maximum time to pause before retrying





pub fn next_addr_for_ip(ip: IpAddr) -> SocketAddr {
    let port = pick_unused_port(ip);
    SocketAddr::new(ip, port)
}

pub fn next_addr() -> SocketAddr {
    next_addr_for_ip(IpAddr::V4(Ipv4Addr::LOCALHOST))
}

pub fn next_addr_any() -> SocketAddr {
    next_addr_for_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub fn next_addr_v6() -> SocketAddr {
    next_addr_for_ip(IpAddr::V6(Ipv6Addr::LOCALHOST))
}