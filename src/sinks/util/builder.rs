use futures_util::{Stream};
use pin_project::pin_project;

use std::{
    convert::Infallible,




    pin::Pin,

    task::{Context, Poll},
};













    


#[pin_project]
pub struct UnwrapInfallible<St> {
    #[pin]
    st: St,
}

impl<St, T> Stream for UnwrapInfallible<St>
where
    St: Stream<Item = Result<T, Infallible>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.st
            .poll_next(cx)
            .map(|maybe| maybe.map(|result| result.unwrap()))
    }
}
