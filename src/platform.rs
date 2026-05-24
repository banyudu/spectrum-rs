use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::content::{
    Content, ContentInput, TypingState, edit as edit_content, rename as rename_content,
    typing as typing_content,
};
use crate::error::{Result, SpectrumError, UnsupportedError};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSender {
    pub id: String,
    pub platform: String,
    pub kind: String,
}

impl AgentSender {
    pub fn new(platform: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            platform: platform.into(),
            kind: "agent".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpaceRef {
    pub id: String,
    pub platform: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub content: Content,
    pub direction: MessageDirection,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<User>,
    pub space: SpaceRef,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

impl Message {
    pub fn inbound(
        platform: impl Into<String>,
        id: impl Into<String>,
        space: SpaceRef,
        sender: User,
        content: Content,
    ) -> Self {
        Self {
            id: id.into(),
            content,
            direction: MessageDirection::Inbound,
            platform: platform.into(),
            sender: Some(sender),
            space,
            extra: Map::new(),
        }
    }

    pub fn outbound(
        platform: impl Into<String>,
        id: impl Into<String>,
        space: SpaceRef,
        sender: Option<User>,
        content: Content,
    ) -> Self {
        Self {
            id: id.into(),
            content,
            direction: MessageDirection::Outbound,
            platform: platform.into(),
            sender,
            space,
            extra: Map::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlatformMessageRecord {
    pub id: String,
    pub content: Content,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<User>,
    pub space: SpaceRef,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[async_trait]
pub trait Space: Send + Sync {
    fn id(&self) -> &str;
    fn platform(&self) -> &str;
    async fn send(&self, content: ContentInput) -> Result<Option<Message>>;
    async fn send_many(&self, content: Vec<ContentInput>) -> Result<Vec<Message>> {
        let mut sent = Vec::new();
        for item in content {
            if let Some(message) = self.send(item).await? {
                sent.push(message);
            }
        }
        Ok(sent)
    }
}

#[async_trait]
pub trait PlatformRuntime: Send + Sync {
    fn name(&self) -> &str;

    async fn send(
        &self,
        space: &SpaceRef,
        content: Content,
    ) -> Result<Option<PlatformMessageRecord>>;

    async fn get_message(
        &self,
        _space: &SpaceRef,
        _message_id: &str,
    ) -> Result<Option<PlatformMessageRecord>> {
        Err(UnsupportedError::action("getMessage", Some(self.name().to_string()), None).into())
    }
}

#[derive(Clone)]
pub struct BuiltSpace {
    space_ref: SpaceRef,
    runtime: Arc<dyn PlatformRuntime>,
}

impl BuiltSpace {
    pub fn new(space_ref: SpaceRef, runtime: Arc<dyn PlatformRuntime>) -> Self {
        Self { space_ref, runtime }
    }

    pub fn space_ref(&self) -> &SpaceRef {
        &self.space_ref
    }

    pub async fn send(&self, content: impl Into<ContentInput>) -> Result<Option<Message>> {
        self.dispatch_send(content.into()).await
    }

    pub async fn get_message(&self, id: &str) -> Result<Option<Message>> {
        let raw = match self.runtime.get_message(&self.space_ref, id).await {
            Ok(raw) => raw,
            Err(SpectrumError::Unsupported(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        raw.map(|record| {
            wrap_provider_message(record, self.runtime.name(), MessageDirection::Inbound)
        })
        .transpose()
    }

    pub async fn edit(&self, message: Message, new_content: impl Into<ContentInput>) -> Result<()> {
        self.send(edit_content(new_content, message)).await?;
        Ok(())
    }

    pub async fn rename(&self, display_name: impl Into<String>) -> Result<()> {
        self.send(rename_content(display_name)?).await?;
        Ok(())
    }

    pub async fn start_typing(&self) -> Result<()> {
        self.send(typing_content(TypingState::Start)).await?;
        Ok(())
    }

    pub async fn stop_typing(&self) -> Result<()> {
        self.send(typing_content(TypingState::Stop)).await?;
        Ok(())
    }

    pub async fn responding<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
        T: Send,
    {
        self.start_typing().await?;
        let result = f().await;
        let stop_result = self.stop_typing().await;
        match (result, stop_result) {
            (Ok(value), Ok(())) | (Ok(value), Err(_)) => Ok(value),
            (Err(err), _) => Err(err),
        }
    }

    async fn dispatch_send(&self, content: ContentInput) -> Result<Option<Message>> {
        let content = content.resolve().await?;
        let raw = self.runtime.send(&self.space_ref, content.clone()).await?;
        let Some(raw) = raw else {
            if content.is_fire_and_forget() {
                return Ok(None);
            }
            return Err(SpectrumError::msg(format!(
                "Platform \"{}\" send did not return a message id",
                self.runtime.name()
            )));
        };
        if raw.id.is_empty() {
            if content.is_fire_and_forget() {
                return Ok(None);
            }
            return Err(SpectrumError::msg(format!(
                "Platform \"{}\" send did not return a message id",
                self.runtime.name()
            )));
        }
        wrap_provider_message(raw, self.runtime.name(), MessageDirection::Outbound).map(Some)
    }
}

#[async_trait]
impl Space for BuiltSpace {
    fn id(&self) -> &str {
        &self.space_ref.id
    }

    fn platform(&self) -> &str {
        &self.space_ref.platform
    }

    async fn send(&self, content: ContentInput) -> Result<Option<Message>> {
        self.dispatch_send(content).await
    }
}

pub fn wrap_provider_message(
    mut raw: PlatformMessageRecord,
    platform: &str,
    direction: MessageDirection,
) -> Result<Message> {
    raw.space.platform = platform.to_string();
    let sender = match (direction.clone(), raw.sender) {
        (MessageDirection::Inbound, Some(mut sender)) => {
            sender.platform = platform.to_string();
            Some(sender)
        }
        (MessageDirection::Inbound, None) => {
            return Err(SpectrumError::msg(format!(
                "Inbound provider message missing sender (platform \"{platform}\", id \"{}\")",
                raw.id
            )));
        }
        (MessageDirection::Outbound, Some(mut sender)) => {
            sender.platform = platform.to_string();
            sender.kind = Some("agent".to_string());
            Some(sender)
        }
        (MessageDirection::Outbound, None) => None,
    };

    Ok(Message {
        id: raw.id,
        content: raw.content,
        direction,
        platform: platform.to_string(),
        sender,
        space: raw.space,
        extra: raw.extra,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{Text, text, typing};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct FakeRuntime {
        sent: Mutex<Vec<Content>>,
        omit_send_id: bool,
    }

    #[async_trait]
    impl PlatformRuntime for FakeRuntime {
        fn name(&self) -> &str {
            "fake"
        }

        async fn send(
            &self,
            space: &SpaceRef,
            content: Content,
        ) -> Result<Option<PlatformMessageRecord>> {
            self.sent.lock().await.push(content.clone());
            if self.omit_send_id || content.is_fire_and_forget() {
                return Ok(None);
            }
            Ok(Some(PlatformMessageRecord {
                id: "out-1".to_string(),
                content,
                sender: Some(User {
                    id: "agent".to_string(),
                    platform: String::new(),
                    kind: None,
                    extra: Map::new(),
                }),
                space: space.clone(),
                extra: Map::new(),
            }))
        }
    }

    fn fake_space(runtime: Arc<dyn PlatformRuntime>) -> BuiltSpace {
        BuiltSpace::new(
            SpaceRef {
                id: "space-1".to_string(),
                platform: "fake".to_string(),
                extra: Map::new(),
            },
            runtime,
        )
    }

    #[tokio::test]
    async fn built_space_wraps_outbound_send() {
        let space = fake_space(Arc::new(FakeRuntime::default()));
        let sent = space.send(text("hello")).await.unwrap().unwrap();
        assert_eq!(sent.id, "out-1");
        assert_eq!(sent.platform, "fake");
        assert_eq!(sent.sender.unwrap().kind.as_deref(), Some("agent"));
    }

    #[tokio::test]
    async fn built_space_allows_fire_and_forget_without_id() {
        let space = fake_space(Arc::new(FakeRuntime::default()));
        let sent = space.send(typing(TypingState::Start)).await.unwrap();
        assert!(sent.is_none());
    }

    #[tokio::test]
    async fn built_space_requires_id_for_normal_content() {
        let space = fake_space(Arc::new(FakeRuntime {
            sent: Mutex::new(Vec::new()),
            omit_send_id: true,
        }));
        let err = space.send(text("hello")).await.unwrap_err();
        assert!(err.to_string().contains("did not return a message id"));
    }

    #[test]
    fn wrap_inbound_requires_sender() {
        let record = PlatformMessageRecord {
            id: "in-1".to_string(),
            content: Content::Text(Text {
                text: "hello".to_string(),
            }),
            sender: None,
            space: SpaceRef {
                id: "space-1".to_string(),
                platform: String::new(),
                extra: Map::new(),
            },
            extra: Map::new(),
        };
        let err = wrap_provider_message(record, "fake", MessageDirection::Inbound).unwrap_err();
        assert!(err.to_string().contains("missing sender"));
    }
}
