use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::{Mutex, broadcast as tokio_broadcast, mpsc};

use crate::error::{Result, SpectrumError};

pub struct ManagedStream<T> {
    receiver: mpsc::Receiver<T>,
    closed: Arc<AtomicBool>,
}

impl<T> ManagedStream<T> {
    pub fn new(receiver: mpsc::Receiver<T>, closed: Arc<AtomicBool>) -> Self {
        Self { receiver, closed }
    }

    pub async fn close(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.receiver.close();
    }

    pub async fn next(&mut self) -> Option<T> {
        self.receiver.recv().await
    }
}

impl<T> Stream for ManagedStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

pub fn stream<T, F, Fut>(producer: F) -> ManagedStream<T>
where
    T: Send + 'static,
    F: FnOnce(mpsc::Sender<T>, Arc<AtomicBool>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel(64);
    let closed = Arc::new(AtomicBool::new(false));
    let producer_closed = closed.clone();
    tokio::spawn(async move {
        let _ = producer(tx, producer_closed.clone()).await;
        producer_closed.store(true, Ordering::SeqCst);
    });
    ManagedStream::new(rx, closed)
}

pub fn merge_streams<T>(streams: Vec<ManagedStream<T>>) -> ManagedStream<T>
where
    T: Send + 'static,
{
    stream(move |tx, closed| async move {
        for mut source in streams {
            let tx = tx.clone();
            let closed = closed.clone();
            tokio::spawn(async move {
                while !closed.load(Ordering::SeqCst) {
                    let Some(value) = source.next().await else {
                        break;
                    };
                    if tx.send(value).await.is_err() {
                        break;
                    }
                }
            });
        }
        Ok(())
    })
}

#[derive(Clone)]
pub struct Broadcaster<T> {
    sender: tokio_broadcast::Sender<T>,
    source: Arc<Mutex<Option<ManagedStream<T>>>>,
    pumping: Arc<AtomicBool>,
}

impl<T> Broadcaster<T>
where
    T: Clone + Send + 'static,
{
    pub fn subscribe(&self) -> ManagedStream<T> {
        self.ensure_pump();
        let mut rx = self.sender.subscribe();
        stream(move |tx, closed| async move {
            while !closed.load(Ordering::SeqCst) {
                match rx.recv().await {
                    Ok(value) => {
                        if tx.send(value).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio_broadcast::error::RecvError::Closed) => break,
                    Err(tokio_broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
            Ok(())
        })
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(mut source) = self.source.lock().await.take() {
            source.close().await;
        }
        Ok(())
    }

    fn ensure_pump(&self) {
        if self.pumping.swap(true, Ordering::SeqCst) {
            return;
        }
        let sender = self.sender.clone();
        let source = self.source.clone();
        tokio::spawn(async move {
            let Some(mut source) = source.lock().await.take() else {
                return;
            };
            while let Some(value) = source.next().await {
                let _ = sender.send(value);
            }
        });
    }
}

pub fn broadcast<T>(source: ManagedStream<T>) -> Broadcaster<T>
where
    T: Clone + Send + 'static,
{
    let (sender, _) = tokio_broadcast::channel(64);
    Broadcaster {
        sender,
        source: Arc::new(Mutex::new(Some(source))),
        pumping: Arc::new(AtomicBool::new(false)),
    }
}

impl From<tokio::sync::mpsc::error::SendError<String>> for SpectrumError {
    fn from(value: tokio::sync::mpsc::error::SendError<String>) -> Self {
        Self::msg(value.to_string())
    }
}
