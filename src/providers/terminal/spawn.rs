use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use super::protocol::RpcSession;
use super::resolve_binary::{ResolveTuichatOptions, resolve_tuichat_binary};
use super::runtime::{TerminalClient, TerminalCommand, TerminalProvider};
use crate::error::{Result, SpectrumError};
use crate::platform::{Message, Space};
use crate::spectrum::SpectrumProvider;
use crate::stream::ManagedStream;

const SPAWN_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Default)]
pub struct TerminalConfig {
    pub commands: Option<Vec<TerminalCommand>>,
    pub binary: Option<std::path::PathBuf>,
    pub resolve: ResolveTuichatOptions,
}

pub async fn terminal(config: TerminalConfig) -> Result<Arc<SpawnedTerminalProvider>> {
    SpawnedTerminalProvider::spawn(config).await.map(Arc::new)
}

pub struct SpawnedTerminalProvider {
    inner: TerminalProvider<tokio::net::tcp::OwnedWriteHalf>,
    child: Mutex<Child>,
}

impl SpawnedTerminalProvider {
    pub async fn spawn(config: TerminalConfig) -> Result<Self> {
        let binary = match config.binary {
            Some(binary) => binary,
            None => resolve_tuichat_binary(config.resolve).await?,
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let mut child = Command::new(&binary)
            .arg("--connect")
            .arg(format!("127.0.0.1:{}", addr.port()))
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| {
                SpectrumError::msg(format!(
                    "tuichat: failed to spawn {}: {err}",
                    binary.display()
                ))
            })?;

        let accepted = tokio::time::timeout(SPAWN_CONNECT_TIMEOUT, listener.accept()).await;
        let (socket, _) = match accepted {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                let _ = child.kill().await;
                return Err(err.into());
            }
            Err(_) => {
                let _ = child.kill().await;
                return Err(SpectrumError::msg(format!(
                    "tuichat: subprocess did not connect within {}ms",
                    SPAWN_CONNECT_TIMEOUT.as_millis()
                )));
            }
        };

        let (reader, writer) = socket.into_split();
        let (session, notifications) = RpcSession::split(reader, writer);
        let client = TerminalClient::new(session, notifications);
        if let Err(err) = client.initialize(config.commands).await {
            let _ = child.kill().await;
            return Err(err);
        }
        Ok(Self {
            inner: TerminalProvider::new(client),
            child: Mutex::new(child),
        })
    }

    pub fn inner(&self) -> &TerminalProvider<tokio::net::tcp::OwnedWriteHalf> {
        &self.inner
    }
}

#[async_trait]
impl SpectrumProvider for SpawnedTerminalProvider {
    async fn messages(&self) -> Result<ManagedStream<(Arc<dyn Space>, Message)>> {
        self.inner.messages().await
    }

    async fn stop(&self) -> Result<()> {
        self.inner.stop().await?;
        let mut child = self.child.lock().await;
        let _ = child.kill().await;
        Ok(())
    }
}
