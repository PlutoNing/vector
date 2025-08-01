use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use tower::{Service};
pub struct Map<S, R1, R2> {
    f: Arc<dyn Fn(R1) -> R2 + Send + Sync + 'static>,
    pub(crate) inner: S,
}

impl<S, R1, R2> Service<R1> for Map<S, R1, R2>
where
    S: Service<R2>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R1) -> Self::Future {
        let req = (self.f)(req);
        self.inner.call(req)
    }
}

impl<S, R1, R2> Clone for Map<S, R1, R2>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            f: Arc::clone(&self.f),
            inner: self.inner.clone(),
        }
    }
}

impl<S, R1, R2> fmt::Debug for Map<S, R1, R2>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Map").field("inner", &self.inner).finish()
    }
}
