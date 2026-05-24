use std::collections::BTreeSet;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::io::AsyncWrite;
use tokio::sync::{Mutex, mpsc};

use super::protocol::{
    ProtocolContent, ProtocolMessageNotification, ProtocolReactionNotification, RpcNotification,
    RpcSession, protocol_to_spectrum, spectrum_to_protocol,
};
use crate::content::{Content, ContentInput, Custom, Reaction, TypingState};
use crate::error::Result;
use crate::platform::{Message, MessageDirection, Space, SpaceRef, User};
use crate::spectrum::SpectrumProvider;
use crate::stream::{ManagedStream, stream};

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalCommand {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SendResult {
    id: String,
    #[serde(rename = "timestamp")]
    _timestamp: String,
}

pub struct TerminalClient<W> {
    session: RpcSession<W>,
    notifications: Mutex<Option<mpsc::Receiver<RpcNotification>>>,
    known_chats: Mutex<BTreeSet<String>>,
    next_chat_index: AtomicU64,
}

pub struct TerminalProvider<W> {
    client: Arc<TerminalClient<W>>,
}

impl<W> TerminalProvider<W> {
    pub fn new(client: Arc<TerminalClient<W>>) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &Arc<TerminalClient<W>> {
        &self.client
    }
}

#[async_trait]
impl<W> SpectrumProvider for TerminalProvider<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    async fn messages(&self) -> Result<ManagedStream<(Arc<dyn Space>, Message)>> {
        self.client.messages().await
    }

    async fn stop(&self) -> Result<()> {
        self.client.shutdown().await;
        Ok(())
    }
}

impl<W> TerminalClient<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(
        session: RpcSession<W>,
        notifications: mpsc::Receiver<RpcNotification>,
    ) -> Arc<Self> {
        Arc::new(Self {
            session,
            notifications: Mutex::new(Some(notifications)),
            known_chats: Mutex::new(BTreeSet::new()),
            next_chat_index: AtomicU64::new(1),
        })
    }

    pub async fn initialize(&self, commands: Option<Vec<TerminalCommand>>) -> Result<()> {
        self.session
            .request::<Value>(
                "initialize",
                Some(json!({
                    "commands": commands,
                    "clientInfo": { "name": "spectrum-rs", "version": "terminal-provider" }
                })),
                Some(DEFAULT_RPC_TIMEOUT),
            )
            .await?;
        Ok(())
    }

    pub async fn space(self: &Arc<Self>, id: Option<String>) -> Result<TerminalSpace<W>> {
        let id = match id {
            Some(id) => id,
            None => self.generate_chat_id().await,
        };
        self.known_chats.lock().await.insert(id.clone());
        self.session
            .request::<Value>(
                "ensureSpace",
                Some(json!({ "id": id })),
                Some(DEFAULT_RPC_TIMEOUT),
            )
            .await?;
        Ok(TerminalSpace::new(self.clone(), id))
    }

    pub async fn messages(self: &Arc<Self>) -> Result<ManagedStream<(Arc<dyn Space>, Message)>> {
        let receiver = self.notifications.lock().await.take().ok_or_else(|| {
            crate::error::SpectrumError::msg("terminal messages already subscribed")
        })?;
        let client = self.clone();
        Ok(stream(move |tx, closed| async move {
            let mut receiver = receiver;
            while !closed.load(std::sync::atomic::Ordering::SeqCst) {
                let Some(notification) = receiver.recv().await else {
                    break;
                };
                if notification.method == "streamEnd" {
                    break;
                }
                let Some((space_id, message)) =
                    client.notification_to_message(notification).await?
                else {
                    continue;
                };
                client.known_chats.lock().await.insert(space_id.clone());
                let space: Arc<dyn Space> = Arc::new(TerminalSpace::new(client.clone(), space_id));
                if tx.send((space, message)).await.is_err() {
                    break;
                }
            }
            Ok(())
        }))
    }

    pub async fn shutdown(&self) {
        let _ = self
            .session
            .request::<Value>("shutdown", None, Some(Duration::from_secs(2)))
            .await;
        self.session.close().await;
    }

    async fn generate_chat_id(&self) -> String {
        loop {
            let index = self.next_chat_index.fetch_add(1, Ordering::SeqCst);
            let id = format!("chat-{index}");
            if !self.known_chats.lock().await.contains(&id) {
                return id;
            }
        }
    }

    async fn notification_to_message(
        &self,
        notification: RpcNotification,
    ) -> Result<Option<(String, Message)>> {
        match notification.method.as_str() {
            "message" => {
                let params = notification.params.unwrap_or(Value::Null);
                let msg: ProtocolMessageNotification = serde_json::from_value(params)
                    .map_err(|err| crate::error::SpectrumError::msg(err.to_string()))?;
                let content = protocol_to_spectrum(&msg.content)?;
                let mut extra = Map::new();
                if let Some(reply_to) = msg.reply_to {
                    extra.insert(
                        "replyTo".to_string(),
                        serde_json::to_value(reply_to).unwrap_or(Value::Null),
                    );
                }
                let space = SpaceRef {
                    id: msg.space_id.clone(),
                    platform: "terminal".to_string(),
                    extra: Map::new(),
                };
                let sender = User {
                    id: msg.sender_id,
                    platform: "terminal".to_string(),
                    kind: None,
                    extra: Map::new(),
                };
                let message = Message {
                    id: msg.id,
                    content,
                    direction: MessageDirection::Inbound,
                    platform: "terminal".to_string(),
                    sender: Some(sender),
                    space,
                    extra,
                };
                Ok(Some((msg.space_id, message)))
            }
            "reaction" => {
                let params = notification.params.unwrap_or(Value::Null);
                let reaction: ProtocolReactionNotification = serde_json::from_value(params)
                    .map_err(|err| crate::error::SpectrumError::msg(err.to_string()))?;
                let content = reaction_content_from_protocol(&reaction);
                let space = SpaceRef {
                    id: reaction.space_id.clone(),
                    platform: "terminal".to_string(),
                    extra: Map::new(),
                };
                let sender = User {
                    id: reaction.sender_id.clone(),
                    platform: "terminal".to_string(),
                    kind: None,
                    extra: Map::new(),
                };
                let message = Message {
                    id: format!(
                        "reaction:{}:{}:{}",
                        reaction.message_id, reaction.reaction, reaction.timestamp
                    ),
                    content,
                    direction: MessageDirection::Inbound,
                    platform: "terminal".to_string(),
                    sender: Some(sender),
                    space,
                    extra: Map::new(),
                };
                Ok(Some((reaction.space_id, message)))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct TerminalSpace<W> {
    client: Arc<TerminalClient<W>>,
    space_ref: SpaceRef,
}

impl<W> TerminalSpace<W> {
    fn new(client: Arc<TerminalClient<W>>, id: String) -> Self {
        Self {
            client,
            space_ref: SpaceRef {
                id,
                platform: "terminal".to_string(),
                extra: Map::new(),
            },
        }
    }

    pub fn space_ref(&self) -> &SpaceRef {
        &self.space_ref
    }

    pub async fn send(&self, content: impl Into<ContentInput>) -> Result<Option<Message>>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        send_terminal_content(&self.client.session, &self.space_ref, content.into()).await
    }
}

#[async_trait]
impl<W> Space for TerminalSpace<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    fn id(&self) -> &str {
        &self.space_ref.id
    }

    fn platform(&self) -> &str {
        "terminal"
    }

    async fn send(&self, content: ContentInput) -> Result<Option<Message>> {
        send_terminal_content(&self.client.session, &self.space_ref, content).await
    }
}

async fn send_terminal_content<W>(
    session: &RpcSession<W>,
    space: &SpaceRef,
    content: ContentInput,
) -> Result<Option<Message>>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let content = content.resolve().await?;
    match content {
        Content::Reply(reply) => {
            let protocol = spectrum_to_protocol(&reply.content)?;
            let result = session
                .request::<SendResult>(
                    "replyToMessage",
                    Some(json!({
                        "spaceId": space.id,
                        "messageId": reply.target.id,
                        "content": protocol,
                    })),
                    Some(DEFAULT_RPC_TIMEOUT),
                )
                .await?;
            Ok(Some(outbound_message(result, *reply.content, space)))
        }
        Content::Reaction(reaction) => {
            session
                .request::<Value>(
                    "reactToMessage",
                    Some(json!({
                        "spaceId": space.id,
                        "messageId": reaction.target.id,
                        "reaction": reaction.emoji,
                    })),
                    Some(DEFAULT_RPC_TIMEOUT),
                )
                .await?;
            Ok(None)
        }
        Content::Typing(typing) => {
            let method = match typing.state {
                TypingState::Start => "startTyping",
                TypingState::Stop => "stopTyping",
            };
            session
                .request::<Value>(
                    method,
                    Some(json!({ "spaceId": space.id })),
                    Some(DEFAULT_RPC_TIMEOUT),
                )
                .await?;
            Ok(None)
        }
        content => {
            let protocol: ProtocolContent = spectrum_to_protocol(&content)?;
            let result = session
                .request::<SendResult>(
                    "send",
                    Some(json!({ "spaceId": space.id, "content": protocol })),
                    Some(DEFAULT_RPC_TIMEOUT),
                )
                .await?;
            Ok(Some(outbound_message(result, content, space)))
        }
    }
}

fn outbound_message(result: SendResult, content: Content, space: &SpaceRef) -> Message {
    Message {
        id: result.id,
        content,
        direction: MessageDirection::Outbound,
        platform: "terminal".to_string(),
        sender: None,
        space: space.clone(),
        extra: Map::new(),
    }
}

fn reaction_content_from_protocol(reaction: &ProtocolReactionNotification) -> Content {
    let target = Message {
        id: reaction.message_id.clone(),
        content: Content::Custom(Custom {
            raw: json!({ "terminal_type": "reaction-target", "stub": true }),
        }),
        direction: MessageDirection::Inbound,
        platform: "terminal".to_string(),
        sender: Some(User {
            id: "__unknown__".to_string(),
            platform: "terminal".to_string(),
            kind: None,
            extra: Map::new(),
        }),
        space: SpaceRef {
            id: reaction.space_id.clone(),
            platform: "terminal".to_string(),
            extra: Map::new(),
        },
        extra: Map::new(),
    };
    Content::Reaction(Reaction {
        emoji: reaction.reaction.clone(),
        target: Box::new(target),
    })
}

#[allow(dead_code)]
fn timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{ContentBuilder, Text, reaction, text, typing};
    use crate::providers::terminal::protocol::{
        JsonRpcVersion, ProtocolContent, RpcDecoder, RpcMessage, RpcResponse, encode_rpc_message,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    async fn read_request(
        stream: &mut tokio::io::DuplexStream,
    ) -> crate::providers::terminal::protocol::RpcRequest {
        let mut buf = [0_u8; 4096];
        let read = stream.read(&mut buf).await.unwrap();
        let message = RpcDecoder::default().push(&buf[..read]).unwrap().remove(0);
        let RpcMessage::Request(request) = message else {
            panic!("expected request");
        };
        request
    }

    async fn write_response(
        stream: &mut tokio::io::DuplexStream,
        id: crate::providers::terminal::protocol::RpcId,
        result: Value,
    ) {
        let response = RpcMessage::Response(RpcResponse {
            jsonrpc: JsonRpcVersion,
            id,
            result: Some(result),
            error: None,
        });
        stream
            .write_all(&encode_rpc_message(&response).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn terminal_space_sends_text_via_send_rpc() {
        let (client_io, mut server) = duplex(8192);
        let (reader, writer) = tokio::io::split(client_io);
        let (session, notifications) = RpcSession::split(reader, writer);
        let client = TerminalClient::new(session, notifications);
        let space = TerminalSpace::new(client, "chat-1".to_string());

        let server_task = tokio::spawn(async move {
            let request = read_request(&mut server).await;
            assert_eq!(request.method, "send");
            assert_eq!(request.params.as_ref().unwrap()["spaceId"], "chat-1");
            assert_eq!(request.params.as_ref().unwrap()["content"]["type"], "text");
            write_response(
                &mut server,
                request.id,
                json!({ "id": "out-1", "timestamp": "2026-05-24T00:00:00.000Z" }),
            )
            .await;
        });

        let sent = space.send(text("hello")).await.unwrap().unwrap();
        assert_eq!(sent.id, "out-1");
        assert_eq!(
            sent.content,
            Content::Text(Text {
                text: "hello".to_string()
            })
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn terminal_space_maps_typing_to_start_typing_rpc() {
        let (client_io, mut server) = duplex(8192);
        let (reader, writer) = tokio::io::split(client_io);
        let (session, notifications) = RpcSession::split(reader, writer);
        let client = TerminalClient::new(session, notifications);
        let space = TerminalSpace::new(client, "chat-1".to_string());

        let server_task = tokio::spawn(async move {
            let request = read_request(&mut server).await;
            assert_eq!(request.method, "startTyping");
            write_response(&mut server, request.id, Value::Null).await;
        });

        assert!(
            space
                .send(typing(TypingState::Start))
                .await
                .unwrap()
                .is_none()
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn terminal_space_maps_reaction_to_react_rpc() {
        let (client_io, mut server) = duplex(8192);
        let (reader, writer) = tokio::io::split(client_io);
        let (session, notifications) = RpcSession::split(reader, writer);
        let client = TerminalClient::new(session, notifications);
        let space = TerminalSpace::new(client, "chat-1".to_string());
        let target = Message {
            id: "in-1".to_string(),
            content: text("target").build().await.unwrap(),
            direction: MessageDirection::Inbound,
            platform: "terminal".to_string(),
            sender: None,
            space: space.space_ref().clone(),
            extra: Map::new(),
        };

        let server_task = tokio::spawn(async move {
            let request = read_request(&mut server).await;
            assert_eq!(request.method, "reactToMessage");
            assert_eq!(request.params.as_ref().unwrap()["messageId"], "in-1");
            assert_eq!(request.params.as_ref().unwrap()["reaction"], "👍");
            write_response(&mut server, request.id, Value::Null).await;
        });

        assert!(space.send(reaction("👍", target)).await.unwrap().is_none());
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn terminal_provider_yields_message_notifications() {
        let (client_io, mut server) = duplex(8192);
        let (reader, writer) = tokio::io::split(client_io);
        let (session, notifications) = RpcSession::split(reader, writer);
        let client = TerminalClient::new(session, notifications);
        let provider = TerminalProvider::new(client);
        let mut messages = provider.messages().await.unwrap();

        let notification = crate::providers::terminal::protocol::RpcMessage::Notification(
            crate::providers::terminal::protocol::RpcNotification {
                jsonrpc: JsonRpcVersion,
                method: "message".to_string(),
                params: Some(json!({
                    "id": "in-1",
                    "content": ProtocolContent::Text { text: "hello".to_string() },
                    "senderId": "user-1",
                    "spaceId": "chat-1",
                    "timestamp": "2026-05-24T00:00:00.000Z"
                })),
            },
        );
        server
            .write_all(&encode_rpc_message(&notification).unwrap())
            .await
            .unwrap();

        let (space, message) = messages.next().await.unwrap();
        assert_eq!(space.id(), "chat-1");
        assert_eq!(message.id, "in-1");
        assert_eq!(message.sender.unwrap().id, "user-1");
        assert_eq!(
            message.content,
            Content::Text(Text {
                text: "hello".to_string()
            })
        );
    }
}
