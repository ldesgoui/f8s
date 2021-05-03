// Stolen from https://github.com/ubnt-intrepid/polyfuse/blob/v0.4.1/examples/with-tokio/src/main.rs#L187-L223

use futures::{future::poll_fn, ready, task::Poll};
use polyfuse::{KernelConfig, Request, Session};
use std::{
    io::{self},
    path::PathBuf,
};
use tokio::io::{unix::AsyncFd, Interest};

pub struct AsyncSession {
    inner: AsyncFd<Session>,
}

impl AsyncSession {
    pub async fn mount(mountpoint: PathBuf, config: KernelConfig) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let session = polyfuse::Session::mount(mountpoint, config)?;
            Ok(Self {
                inner: AsyncFd::with_interest(session, Interest::READABLE)?,
            })
        })
        .await
        .expect("join error")
    }

    pub async fn next_request(&self) -> io::Result<Option<Request>> {
        poll_fn(|cx| {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;
            match guard.try_io(|inner| inner.get_ref().next_request()) {
                Err(_would_block) => Poll::Pending,
                Ok(res) => Poll::Ready(res),
            }
        })
        .await
    }
}
