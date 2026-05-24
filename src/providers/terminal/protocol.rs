use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::content::{Attachment, Contact, Content, Custom, Text, Voice};
use crate::error::{Result, SpectrumError, UnsupportedError};
use crate::utils::{from_vcard, to_vcard};

pub const PROTOCOL_VERSION: &str = "1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtocolContent {
    Text { text: String },
    Attachment(ProtocolAttachmentContent),
    Voice(ProtocolVoiceContent),
    Contact(ProtocolContactContent),
    Custom { raw: Value },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProtocolAttachmentContent {
    pub name: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProtocolVoiceContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProtocolContactContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<ProtocolContactName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vcard: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolContactName {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolReplyRef {
    #[serde(rename = "messageId")]
    pub message_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProtocolMessageNotification {
    pub content: ProtocolContent,
    pub id: String,
    #[serde(rename = "replyTo", skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<ProtocolReplyRef>,
    #[serde(rename = "senderId")]
    pub sender_id: String,
    #[serde(rename = "spaceId")]
    pub space_id: String,
    pub timestamp: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolReactionNotification {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub reaction: String,
    #[serde(rename = "senderId")]
    pub sender_id: String,
    #[serde(rename = "spaceId")]
    pub space_id: String,
    pub timestamp: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: JsonRpcVersion,
    pub id: RpcId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcNotification {
    pub jsonrpc: JsonRpcVersion,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: JsonRpcVersion,
    pub id: RpcId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    Number(i64),
    String(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JsonRpcVersion;

impl Serialize for JsonRpcVersion {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("2.0")
    }
}

impl<'de> Deserialize<'de> for JsonRpcVersion {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value == "2.0" {
            Ok(Self)
        } else {
            Err(serde::de::Error::custom("jsonrpc must be \"2.0\""))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcMessage {
    Request(RpcRequest),
    Notification(RpcNotification),
    Response(RpcResponse),
}

const HEADER_TERMINATOR: &[u8] = b"\r\n\r\n";
const CONTENT_LENGTH: &str = "content-length:";

pub fn encode_rpc_message(message: &RpcMessage) -> Result<Vec<u8>> {
    let body = serde_json::to_vec(message).map_err(|err| SpectrumError::msg(err.to_string()))?;
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    let mut out = Vec::with_capacity(header.len() + body.len());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

#[derive(Default)]
pub struct RpcDecoder {
    buf: Vec<u8>,
}

impl RpcDecoder {
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<RpcMessage>> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(message) = self.read_one()? {
            out.push(message);
        }
        Ok(out)
    }

    fn read_one(&mut self) -> Result<Option<RpcMessage>> {
        let Some(header_end) = find_bytes(&self.buf, HEADER_TERMINATOR) else {
            return Ok(None);
        };
        let header = std::str::from_utf8(&self.buf[..header_end])
            .map_err(|err| SpectrumError::msg(err.to_string()))?;
        let mut len = None;
        for line in header.split("\r\n") {
            let lower = line.to_ascii_lowercase();
            if lower.starts_with(CONTENT_LENGTH) {
                let raw = line[CONTENT_LENGTH.len()..].trim();
                let parsed = raw
                    .parse::<usize>()
                    .map_err(|_| SpectrumError::msg("invalid Content-Length"))?;
                len = Some(parsed);
            }
        }
        let len = len.ok_or_else(|| SpectrumError::msg("missing Content-Length header"))?;
        let body_start = header_end + HEADER_TERMINATOR.len();
        let body_end = body_start + len;
        if self.buf.len() < body_end {
            return Ok(None);
        }
        let body = self.buf[body_start..body_end].to_vec();
        self.buf.drain(..body_end);
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|err| SpectrumError::msg(err.to_string()))
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub fn spectrum_to_protocol(content: &Content) -> Result<ProtocolContent> {
    match content {
        Content::Text(Text { text }) => Ok(ProtocolContent::Text { text: text.clone() }),
        Content::Custom(Custom { raw }) => Ok(ProtocolContent::Custom { raw: raw.clone() }),
        Content::Attachment(Attachment {
            name,
            mime_type,
            size,
            data,
        }) => Ok(ProtocolContent::Attachment(ProtocolAttachmentContent {
            name: name.clone(),
            mime_type: mime_type.clone(),
            size: *size,
            bytes: Some(base64::engine::general_purpose::STANDARD.encode(data)),
            path: None,
        })),
        Content::Voice(Voice {
            name,
            mime_type,
            size,
            data,
            ..
        }) => Ok(ProtocolContent::Voice(ProtocolVoiceContent {
            name: name.clone(),
            mime_type: mime_type.clone(),
            size: *size,
            bytes: Some(base64::engine::general_purpose::STANDARD.encode(data)),
            path: None,
        })),
        Content::Contact(contact) => Ok(ProtocolContent::Contact(ProtocolContactContent {
            name: contact.name.as_ref().map(|name| ProtocolContactName {
                formatted: name.formatted.clone(),
                first: name.first.clone(),
                last: name.last.clone(),
            }),
            vcard: Some(to_vcard(contact)),
        })),
        other => {
            Err(
                UnsupportedError::content(other.content_type(), Some("terminal".to_string()), None)
                    .into(),
            )
        }
    }
}

pub fn protocol_to_spectrum(content: &ProtocolContent) -> Result<Content> {
    match content {
        ProtocolContent::Text { text } => Ok(Content::Text(Text { text: text.clone() })),
        ProtocolContent::Custom { raw } => Ok(Content::Custom(Custom { raw: raw.clone() })),
        ProtocolContent::Attachment(value) => {
            let data = protocol_bytes(value.bytes.as_deref(), value.path.as_deref(), "attachment")?;
            Ok(Content::Attachment(Attachment {
                name: value.name.clone(),
                mime_type: value.mime_type.clone(),
                size: value.size.or(Some(data.len() as u64)),
                data,
            }))
        }
        ProtocolContent::Voice(value) => {
            let data = protocol_bytes(value.bytes.as_deref(), value.path.as_deref(), "voice")?;
            Ok(Content::Voice(Voice {
                name: value.name.clone(),
                mime_type: value.mime_type.clone(),
                duration: None,
                size: value.size.or(Some(data.len() as u64)),
                data,
            }))
        }
        ProtocolContent::Contact(value) => {
            if let Some(vcard) = &value.vcard
                && let Ok(contact) = from_vcard(vcard)
            {
                return Ok(Content::Contact(Box::new(contact)));
            }
            Ok(Content::Contact(Box::new(Contact {
                user: None,
                name: value.name.as_ref().map(|name| crate::content::ContactName {
                    formatted: name.formatted.clone(),
                    first: name.first.clone(),
                    last: name.last.clone(),
                    ..Default::default()
                }),
                phones: Vec::new(),
                emails: Vec::new(),
                addresses: Vec::new(),
                org: None,
                urls: Vec::new(),
                birthday: None,
                note: None,
                photo: None,
                raw: value.vcard.clone(),
            })))
        }
    }
}

fn protocol_bytes(bytes_b64: Option<&str>, path: Option<&str>, label: &str) -> Result<Bytes> {
    if let Some(bytes_b64) = bytes_b64 {
        let data = base64::engine::general_purpose::STANDARD
            .decode(bytes_b64)
            .map_err(|err| SpectrumError::msg(err.to_string()))?;
        return Ok(data.into());
    }
    if let Some(path) = path {
        let data = std::fs::read(path)?;
        return Ok(data.into());
    }
    Err(SpectrumError::msg(format!(
        "{label} has neither path nor bytes"
    )))
}

type PendingMap = Arc<Mutex<BTreeMap<RpcId, oneshot::Sender<Result<Value>>>>>;

#[derive(Clone)]
pub struct RpcSession<W> {
    writer: Arc<Mutex<W>>,
    next_id: Arc<AtomicI64>,
    pending: PendingMap,
    notifications: mpsc::Sender<RpcNotification>,
    closed: Arc<AtomicBool>,
}

impl<W> RpcSession<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub fn split<R>(reader: R, writer: W) -> (Self, mpsc::Receiver<RpcNotification>)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let (notifications_tx, notifications_rx) = mpsc::channel(64);
        let session = Self {
            writer: Arc::new(Mutex::new(writer)),
            next_id: Arc::new(AtomicI64::new(1)),
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            notifications: notifications_tx,
            closed: Arc::new(AtomicBool::new(false)),
        };
        spawn_reader(
            reader,
            session.pending.clone(),
            session.notifications.clone(),
            session.closed.clone(),
        );
        (session, notifications_rx)
    }

    pub async fn request<T>(
        &self,
        method: impl Into<String>,
        params: Option<Value>,
        timeout: Option<Duration>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        if self.closed.load(Ordering::SeqCst) {
            return Err(SpectrumError::msg("session closed"));
        }
        let method = method.into();
        let id = RpcId::Number(self.next_id.fetch_add(1, Ordering::SeqCst));
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);

        let message = RpcMessage::Request(RpcRequest {
            jsonrpc: JsonRpcVersion,
            id: id.clone(),
            method: method.clone(),
            params,
        });

        if let Err(err) = self.write_message(&message).await {
            self.pending.lock().await.remove(&id);
            return Err(err);
        }

        let result = if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, rx).await {
                Ok(result) => result,
                Err(_) => {
                    self.pending.lock().await.remove(&id);
                    return Err(SpectrumError::msg(format!(
                        "rpc {method} timed out after {}ms",
                        timeout.as_millis()
                    )));
                }
            }
        } else {
            rx.await
        };

        let value = result.map_err(|_| SpectrumError::msg("session closed"))??;
        serde_json::from_value(value).map_err(|err| SpectrumError::msg(err.to_string()))
    }

    pub async fn notify(&self, method: impl Into<String>, params: Option<Value>) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.write_message(&RpcMessage::Notification(RpcNotification {
            jsonrpc: JsonRpcVersion,
            method: method.into(),
            params,
        }))
        .await
    }

    pub async fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let pending = std::mem::take(&mut *self.pending.lock().await);
        for (_, sender) in pending {
            let _ = sender.send(Err(SpectrumError::msg("session closed")));
        }
    }

    async fn write_message(&self, message: &RpcMessage) -> Result<()> {
        let frame = encode_rpc_message(message)?;
        let mut writer = self.writer.lock().await;
        writer.write_all(&frame).await?;
        writer.flush().await?;
        Ok(())
    }
}

fn spawn_reader<R>(
    mut reader: R,
    pending: PendingMap,
    notifications: mpsc::Sender<RpcNotification>,
    closed: Arc<AtomicBool>,
) where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut decoder = RpcDecoder::default();
        let mut buf = [0_u8; 8192];
        loop {
            let read = match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(read) => read,
                Err(_) => break,
            };
            let messages = match decoder.push(&buf[..read]) {
                Ok(messages) => messages,
                Err(_) => break,
            };
            for message in messages {
                match message {
                    RpcMessage::Response(response) => {
                        let sender = pending.lock().await.remove(&response.id);
                        if let Some(sender) = sender {
                            let result = match response.error {
                                Some(error) => Err(SpectrumError::msg(error.message)),
                                None => Ok(response.result.unwrap_or(Value::Null)),
                            };
                            let _ = sender.send(result);
                        }
                    }
                    RpcMessage::Notification(notification) => {
                        let _ = notifications.send(notification).await;
                    }
                    RpcMessage::Request(_) => {}
                }
            }
        }

        closed.store(true, Ordering::SeqCst);
        let pending = std::mem::take(&mut *pending.lock().await);
        for (_, sender) in pending {
            let _ = sender.send(Err(SpectrumError::msg("session closed")));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{ContactName, ContentBuilder, attachment, contact};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[test]
    fn rpc_decoder_handles_split_frames() {
        let message = RpcMessage::Request(RpcRequest {
            jsonrpc: JsonRpcVersion,
            id: RpcId::Number(7),
            method: "initialize".to_string(),
            params: Some(serde_json::json!({"protocolVersion": PROTOCOL_VERSION})),
        });
        let encoded = encode_rpc_message(&message).unwrap();
        let split = encoded.len() / 2;
        let mut decoder = RpcDecoder::default();
        assert!(decoder.push(&encoded[..split]).unwrap().is_empty());
        assert_eq!(decoder.push(&encoded[split..]).unwrap(), vec![message]);
    }

    #[test]
    fn rpc_decoder_handles_multiple_frames() {
        let first = RpcMessage::Notification(RpcNotification {
            jsonrpc: JsonRpcVersion,
            method: "streamEnd".to_string(),
            params: None,
        });
        let second = RpcMessage::Response(RpcResponse {
            jsonrpc: JsonRpcVersion,
            id: RpcId::String("abc".to_string()),
            result: Some(serde_json::json!({"ok": true})),
            error: None,
        });
        let mut bytes = encode_rpc_message(&first).unwrap();
        bytes.extend_from_slice(&encode_rpc_message(&second).unwrap());
        assert_eq!(
            RpcDecoder::default().push(&bytes).unwrap(),
            vec![first, second]
        );
    }

    #[tokio::test]
    async fn spectrum_attachment_encodes_base64_protocol_content() {
        let content = attachment(Vec::from("hello"))
            .options(crate::content::AttachmentOptions {
                name: Some("hello.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
            })
            .build()
            .await
            .unwrap();
        let protocol = spectrum_to_protocol(&content).unwrap();
        assert_eq!(
            protocol,
            ProtocolContent::Attachment(ProtocolAttachmentContent {
                name: "hello.txt".to_string(),
                mime_type: "text/plain".to_string(),
                size: Some(5),
                bytes: Some("aGVsbG8=".to_string()),
                path: None,
            })
        );
        assert_eq!(protocol_to_spectrum(&protocol).unwrap(), content);
    }

    #[tokio::test]
    async fn contact_protocol_prefers_vcard_roundtrip() {
        let content = contact(Contact {
            user: None,
            name: Some(ContactName {
                formatted: Some("Ada Lovelace".to_string()),
                first: Some("Ada".to_string()),
                last: Some("Lovelace".to_string()),
                ..Default::default()
            }),
            phones: Vec::new(),
            emails: Vec::new(),
            addresses: Vec::new(),
            org: None,
            urls: vec!["https://example.com".to_string()],
            birthday: None,
            note: None,
            photo: None,
            raw: None,
        })
        .build()
        .await
        .unwrap();
        let protocol = spectrum_to_protocol(&content).unwrap();
        let restored = protocol_to_spectrum(&protocol).unwrap();
        let Content::Contact(restored) = restored else {
            panic!("expected contact");
        };
        assert_eq!(restored.urls, vec!["https://example.com"]);
    }

    #[test]
    fn unsupported_content_returns_terminal_unsupported_error() {
        let err = spectrum_to_protocol(&crate::content::typing(crate::content::TypingState::Start))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("terminal does not support content type")
        );
    }

    #[tokio::test]
    async fn rpc_session_matches_response_to_pending_request() {
        let (client, mut server) = duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (session, _notifications) = RpcSession::split(reader, writer);

        let server_task = tokio::spawn(async move {
            let mut buf = [0_u8; 1024];
            let read = server.read(&mut buf).await.unwrap();
            let request = RpcDecoder::default().push(&buf[..read]).unwrap().remove(0);
            let RpcMessage::Request(request) = request else {
                panic!("expected request");
            };
            assert_eq!(request.method, "initialize");
            let response = RpcMessage::Response(RpcResponse {
                jsonrpc: JsonRpcVersion,
                id: request.id,
                result: Some(serde_json::json!({"ok": true})),
                error: None,
            });
            server
                .write_all(&encode_rpc_message(&response).unwrap())
                .await
                .unwrap();
        });

        let result: Value = session
            .request("initialize", None, Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(result, serde_json::json!({"ok": true}));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn rpc_session_surfaces_notifications() {
        let (client, mut server) = duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (_session, mut notifications) = RpcSession::split(reader, writer);
        let notification = RpcMessage::Notification(RpcNotification {
            jsonrpc: JsonRpcVersion,
            method: "streamEnd".to_string(),
            params: None,
        });
        server
            .write_all(&encode_rpc_message(&notification).unwrap())
            .await
            .unwrap();
        let received = notifications.recv().await.unwrap();
        assert_eq!(received.method, "streamEnd");
    }
}
