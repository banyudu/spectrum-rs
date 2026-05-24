use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::error::{Result, SpectrumError};
use crate::platform::{Message, User};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Content {
    Text(Text),
    Custom(Custom),
    Attachment(Attachment),
    Contact(Box<Contact>),
    Voice(Voice),
    Richlink(Richlink),
    Reaction(Reaction),
    Group(Group),
    Poll(Poll),
    PollOption(PollOption),
    Effect(MessageEffect),
    Typing(Typing),
    Rename(Rename),
    Avatar(Avatar),
    Reply(Reply),
    Edit(Edit),
}

impl Content {
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Text(_) => "text",
            Self::Custom(_) => "custom",
            Self::Attachment(_) => "attachment",
            Self::Contact(_) => "contact",
            Self::Voice(_) => "voice",
            Self::Richlink(_) => "richlink",
            Self::Reaction(_) => "reaction",
            Self::Group(_) => "group",
            Self::Poll(_) => "poll",
            Self::PollOption(_) => "poll_option",
            Self::Effect(_) => "effect",
            Self::Typing(_) => "typing",
            Self::Rename(_) => "rename",
            Self::Avatar(_) => "avatar",
            Self::Reply(_) => "reply",
            Self::Edit(_) => "edit",
        }
    }

    pub fn is_fire_and_forget(&self) -> bool {
        matches!(
            self,
            Self::Reaction(_) | Self::Typing(_) | Self::Edit(_) | Self::Rename(_) | Self::Avatar(_)
        )
    }
}

#[async_trait]
pub trait ContentBuilder: Send + Sync {
    async fn build(&self) -> Result<Content>;
}

pub enum ContentInput {
    Text(String),
    Content(Box<Content>),
    Builder(Arc<dyn ContentBuilder>),
}

impl ContentInput {
    pub async fn resolve(self) -> Result<Content> {
        match self {
            Self::Text(value) => text(value).build().await,
            Self::Content(content) => Ok(*content),
            Self::Builder(builder) => builder.build().await,
        }
    }
}

impl From<String> for ContentInput {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for ContentInput {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<Content> for ContentInput {
    fn from(value: Content) -> Self {
        Self::Content(Box::new(value))
    }
}

macro_rules! impl_builder_input {
    ($($builder:ty),+ $(,)?) => {
        $(
            impl From<$builder> for ContentInput {
                fn from(value: $builder) -> Self {
                    Self::Builder(Arc::new(value))
                }
            }
        )+
    };
}

pub async fn resolve_contents(items: Vec<ContentInput>) -> Result<Vec<Content>> {
    let mut resolved = Vec::with_capacity(items.len());
    for item in items {
        resolved.push(item.resolve().await?);
    }
    Ok(resolved)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Text {
    pub text: String,
}

pub fn as_text(value: impl Into<String>) -> Result<Content> {
    let text = value.into();
    if text.is_empty() {
        return Err(SpectrumError::msg("text content must be non-empty"));
    }
    Ok(Content::Text(Text { text }))
}

pub fn text(value: impl Into<String>) -> TextBuilder {
    TextBuilder { text: value.into() }
}

pub struct TextBuilder {
    text: String,
}

#[async_trait]
impl ContentBuilder for TextBuilder {
    async fn build(&self) -> Result<Content> {
        as_text(self.text.clone())
    }
}

impl_builder_input!(TextBuilder);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Custom {
    pub raw: Value,
}

pub fn custom(raw: impl Into<Value>) -> CustomBuilder {
    CustomBuilder { raw: raw.into() }
}

pub struct CustomBuilder {
    raw: Value,
}

#[async_trait]
impl ContentBuilder for CustomBuilder {
    async fn build(&self) -> Result<Content> {
        Ok(Content::Custom(Custom {
            raw: self.raw.clone(),
        }))
    }
}

impl_builder_input!(CustomBuilder);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub name: String,
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    pub data: Bytes,
}

#[derive(Clone, Debug, Default)]
pub struct AttachmentOptions {
    pub name: Option<String>,
    pub mime_type: Option<String>,
}

pub enum AttachmentSource {
    Path(PathBuf),
    Bytes(Bytes),
}

impl From<PathBuf> for AttachmentSource {
    fn from(value: PathBuf) -> Self {
        Self::Path(value)
    }
}

impl From<&Path> for AttachmentSource {
    fn from(value: &Path) -> Self {
        Self::Path(value.to_path_buf())
    }
}

impl From<&str> for AttachmentSource {
    fn from(value: &str) -> Self {
        Self::Path(PathBuf::from(value))
    }
}

impl From<Vec<u8>> for AttachmentSource {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value.into())
    }
}

impl From<Bytes> for AttachmentSource {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

pub fn attachment(source: impl Into<AttachmentSource>) -> AttachmentBuilder {
    AttachmentBuilder {
        source: source.into(),
        options: AttachmentOptions::default(),
    }
}

pub struct AttachmentBuilder {
    source: AttachmentSource,
    options: AttachmentOptions,
}

impl AttachmentBuilder {
    pub fn options(mut self, options: AttachmentOptions) -> Self {
        self.options = options;
        self
    }
}

#[async_trait]
impl ContentBuilder for AttachmentBuilder {
    async fn build(&self) -> Result<Content> {
        let (name, data) = match &self.source {
            AttachmentSource::Path(path) => {
                let name = self
                    .options
                    .name
                    .clone()
                    .or_else(|| path.file_name().map(|n| n.to_string_lossy().into_owned()))
                    .unwrap_or_else(|| "attachment".to_string());
                (name, Bytes::from(tokio::fs::read(path).await?))
            }
            AttachmentSource::Bytes(data) => (
                self.options
                    .name
                    .clone()
                    .unwrap_or_else(|| "attachment".to_string()),
                data.clone(),
            ),
        };
        if name.is_empty() {
            return Err(SpectrumError::msg("attachment name must be non-empty"));
        }
        let mime_type = resolve_mime_type(&name, self.options.mime_type.as_deref(), "attachment")?;
        Ok(Content::Attachment(Attachment {
            name,
            mime_type,
            size: Some(data.len() as u64),
            data,
        }))
    }
}

impl_builder_input!(AttachmentBuilder);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Voice {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    pub data: Bytes,
}

#[derive(Clone, Debug, Default)]
pub struct VoiceOptions {
    pub name: Option<String>,
    pub mime_type: Option<String>,
    pub duration: Option<u64>,
}

pub fn voice(source: impl Into<AttachmentSource>) -> VoiceBuilder {
    VoiceBuilder {
        source: source.into(),
        options: VoiceOptions::default(),
    }
}

pub struct VoiceBuilder {
    source: AttachmentSource,
    options: VoiceOptions,
}

impl VoiceBuilder {
    pub fn options(mut self, options: VoiceOptions) -> Self {
        self.options = options;
        self
    }
}

#[async_trait]
impl ContentBuilder for VoiceBuilder {
    async fn build(&self) -> Result<Content> {
        let (name, data) = match &self.source {
            AttachmentSource::Path(path) => {
                let name = self
                    .options
                    .name
                    .clone()
                    .or_else(|| path.file_name().map(|n| n.to_string_lossy().into_owned()));
                (name, Bytes::from(tokio::fs::read(path).await?))
            }
            AttachmentSource::Bytes(data) => (self.options.name.clone(), data.clone()),
        };
        let mime_hint = name.as_deref().unwrap_or("voice");
        let mime_type = resolve_mime_type(mime_hint, self.options.mime_type.as_deref(), "voice")?;
        if !mime_type.to_ascii_lowercase().starts_with("audio/") {
            return Err(SpectrumError::msg(format!(
                "voice content requires an audio/* MIME type, got \"{mime_type}\""
            )));
        }
        Ok(Content::Voice(Voice {
            name,
            mime_type,
            duration: self.options.duration,
            size: Some(data.len() as u64),
            data,
        }))
    }
}

impl_builder_input!(VoiceBuilder);

fn resolve_mime_type(name: &str, explicit: Option<&str>, label: &str) -> Result<String> {
    if let Some(mime_type) = explicit {
        if mime_type.is_empty() {
            return Err(SpectrumError::msg(format!(
                "{label} MIME type must be non-empty"
            )));
        }
        return Ok(mime_type.to_string());
    }
    let guessed = mime_guess::from_path(name).first();
    guessed
        .map(|mime| mime.essence_str().to_string())
        .ok_or_else(|| {
            SpectrumError::msg(format!(
                "Unable to resolve MIME type for {label}. Pass options.mime_type explicitly."
            ))
        })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Contact {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<User>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<ContactName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phones: Vec<ContactPhone>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emails: Vec<ContactEmail>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<ContactAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<ContactOrg>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birthday: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub photo: Option<ContactPhoto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactName {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatted: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub middle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactPhone {
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phone_type: Option<ContactPointType>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactEmail {
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_type: Option<ContactPointType>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postal_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_type: Option<ContactPointType>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactOrg {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub department: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactPhoto {
    pub mime_type: String,
    pub data: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContactPointType {
    Mobile,
    Home,
    Work,
    Other,
}

pub fn contact(input: Contact) -> ContactBuilder {
    ContactBuilder { input }
}

pub struct ContactBuilder {
    input: Contact,
}

#[async_trait]
impl ContentBuilder for ContactBuilder {
    async fn build(&self) -> Result<Content> {
        Ok(Content::Contact(Box::new(self.input.clone())))
    }
}

impl_builder_input!(ContactBuilder);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Richlink {
    pub url: String,
}

pub fn richlink(url: impl Into<String>) -> RichlinkBuilder {
    RichlinkBuilder { url: url.into() }
}

pub struct RichlinkBuilder {
    url: String,
}

#[async_trait]
impl ContentBuilder for RichlinkBuilder {
    async fn build(&self) -> Result<Content> {
        Url::parse(&self.url)?;
        Ok(Content::Richlink(Richlink {
            url: self.url.clone(),
        }))
    }
}

impl_builder_input!(RichlinkBuilder);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reaction {
    pub emoji: String,
    pub target: Box<Message>,
}

pub fn reaction(emoji: impl Into<String>, target: Message) -> ReactionBuilder {
    ReactionBuilder {
        emoji: emoji.into(),
        target,
    }
}

pub struct ReactionBuilder {
    emoji: String,
    target: Message,
}

#[async_trait]
impl ContentBuilder for ReactionBuilder {
    async fn build(&self) -> Result<Content> {
        if self.emoji.is_empty() {
            return Err(SpectrumError::msg("reaction emoji must be non-empty"));
        }
        if matches!(self.target.content, Content::Reaction(_)) {
            return Err(SpectrumError::msg(
                "reaction() cannot target \"reaction\" content",
            ));
        }
        Ok(Content::Reaction(Reaction {
            emoji: self.emoji.clone(),
            target: Box::new(self.target.clone()),
        }))
    }
}

impl_builder_input!(ReactionBuilder);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reply {
    pub content: Box<Content>,
    pub target: Box<Message>,
}

pub fn reply(content: impl Into<ContentInput>, target: Message) -> ReplyBuilder {
    ReplyBuilder {
        content: Some(content.into()),
        target,
    }
}

pub struct ReplyBuilder {
    content: Option<ContentInput>,
    target: Message,
}

#[async_trait]
impl ContentBuilder for ReplyBuilder {
    async fn build(&self) -> Result<Content> {
        let content = self
            .content
            .as_ref()
            .ok_or_else(|| SpectrumError::msg("reply() requires content"))?
            .clone_input()
            .resolve()
            .await?;
        if matches!(
            content,
            Content::Reply(_)
                | Content::Edit(_)
                | Content::Reaction(_)
                | Content::Group(_)
                | Content::Typing(_)
                | Content::Rename(_)
                | Content::Avatar(_)
        ) {
            return Err(SpectrumError::msg(format!(
                "reply() cannot wrap \"{}\" content",
                content.content_type()
            )));
        }
        Ok(Content::Reply(Reply {
            content: Box::new(content),
            target: Box::new(self.target.clone()),
        }))
    }
}

trait CloneInput {
    fn clone_input(&self) -> ContentInput;
}

impl CloneInput for ContentInput {
    fn clone_input(&self) -> ContentInput {
        match self {
            ContentInput::Text(value) => ContentInput::Text(value.clone()),
            ContentInput::Content(content) => ContentInput::Content(content.clone()),
            ContentInput::Builder(builder) => ContentInput::Builder(builder.clone()),
        }
    }
}

impl_builder_input!(ReplyBuilder);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Edit {
    pub content: Box<Content>,
    pub target: Box<Message>,
}

pub fn edit(content: impl Into<ContentInput>, target: Message) -> EditBuilder {
    EditBuilder {
        content: Some(content.into()),
        target,
    }
}

pub struct EditBuilder {
    content: Option<ContentInput>,
    target: Message,
}

#[async_trait]
impl ContentBuilder for EditBuilder {
    async fn build(&self) -> Result<Content> {
        if self.target.direction != crate::platform::MessageDirection::Outbound {
            return Err(SpectrumError::msg(format!(
                "edit() target must be an outbound message (message id \"{}\")",
                self.target.id
            )));
        }
        let content = self
            .content
            .as_ref()
            .ok_or_else(|| SpectrumError::msg("edit() requires content"))?
            .clone_input()
            .resolve()
            .await?;
        if matches!(
            content,
            Content::Edit(_)
                | Content::Reply(_)
                | Content::Reaction(_)
                | Content::Group(_)
                | Content::Typing(_)
                | Content::Rename(_)
                | Content::Avatar(_)
        ) {
            return Err(SpectrumError::msg(format!(
                "edit() cannot wrap \"{}\" content",
                content.content_type()
            )));
        }
        Ok(Content::Edit(Edit {
            content: Box::new(content),
            target: Box::new(self.target.clone()),
        }))
    }
}

impl_builder_input!(EditBuilder);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Group {
    pub items: Vec<Message>,
}

pub fn group(items: Vec<Content>) -> Result<Content> {
    if items.len() < 2 {
        return Err(SpectrumError::msg("group() requires at least two items"));
    }
    let mut messages = Vec::with_capacity(items.len());
    for content in items {
        if matches!(content, Content::Group(_) | Content::Reaction(_)) {
            return Err(SpectrumError::msg(format!(
                "group() cannot contain \"{}\" items",
                content.content_type()
            )));
        }
        messages.push(Message::outbound(
            "",
            "",
            crate::platform::SpaceRef {
                id: String::new(),
                platform: String::new(),
                extra: serde_json::Map::new(),
            },
            None,
            content,
        ));
    }
    Ok(Content::Group(Group { items: messages }))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollChoice {
    pub title: String,
}

pub fn option(title: impl Into<String>) -> PollChoice {
    PollChoice {
        title: title.into(),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Poll {
    pub title: String,
    pub options: Vec<PollChoice>,
}

pub fn poll(title: impl Into<String>, options: Vec<PollChoice>) -> Result<Content> {
    let poll = Poll {
        title: title.into(),
        options,
    };
    validate_poll(&poll)?;
    Ok(Content::Poll(poll))
}

fn validate_poll(poll: &Poll) -> Result<()> {
    if poll.title.is_empty() || poll.title.len() > 300 {
        return Err(SpectrumError::msg("poll title must be 1..=300 characters"));
    }
    if poll.options.len() < 2 || poll.options.len() > 10 {
        return Err(SpectrumError::msg(
            "poll options must contain 2..=10 choices",
        ));
    }
    if poll.options.iter().any(|choice| choice.title.is_empty()) {
        return Err(SpectrumError::msg("poll option titles must be non-empty"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollOption {
    pub option: PollChoice,
    pub poll: Poll,
    pub selected: bool,
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MessageEffect {
    pub content: Box<EffectContent>,
    pub effect: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectContent {
    Text(Text),
    Attachment(Attachment),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Typing {
    pub state: TypingState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypingState {
    Start,
    Stop,
}

pub fn typing(state: TypingState) -> Content {
    Content::Typing(Typing { state })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rename {
    pub display_name: String,
}

pub fn rename(display_name: impl Into<String>) -> Result<Content> {
    let display_name = display_name.into();
    if display_name.is_empty() {
        return Err(SpectrumError::msg(
            "rename() display_name must be non-empty",
        ));
    }
    Ok(Content::Rename(Rename { display_name }))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Avatar {
    pub action: AvatarAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AvatarAction {
    Set { mime_type: String, data: Bytes },
    Clear,
}

pub fn avatar(action: AvatarAction) -> Content {
    Content::Avatar(Avatar { action })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::{Message, MessageDirection, SpaceRef};

    fn outbound_message(content: Content) -> Message {
        Message {
            id: "m1".to_string(),
            content,
            direction: MessageDirection::Outbound,
            platform: "test".to_string(),
            sender: None,
            space: SpaceRef {
                id: "s1".to_string(),
                platform: "test".to_string(),
                extra: serde_json::Map::new(),
            },
            extra: serde_json::Map::new(),
        }
    }

    #[tokio::test]
    async fn text_builder_rejects_empty_text() {
        assert!(text("").build().await.is_err());
        assert_eq!(
            text("hello").build().await.unwrap(),
            Content::Text(Text {
                text: "hello".to_string()
            })
        );
    }

    #[tokio::test]
    async fn reply_rejects_nested_control_content() {
        let target = outbound_message(text("target").build().await.unwrap());
        let err = reply(rename("new").unwrap(), target)
            .build()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("reply() cannot wrap"));
    }

    #[tokio::test]
    async fn edit_requires_outbound_target() {
        let mut target = outbound_message(text("target").build().await.unwrap());
        target.direction = MessageDirection::Inbound;
        let err = edit("new", target).build().await.unwrap_err();
        assert!(err.to_string().contains("outbound message"));
    }

    #[test]
    fn poll_validates_option_count() {
        assert!(poll("pick one", vec![option("a")]).is_err());
        assert!(poll("pick one", vec![option("a"), option("b")]).is_ok());
    }
}
