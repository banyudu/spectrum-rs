use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;
use url::Url;

use crate::audio::ensure_m4a;
use crate::content::{
    Attachment, Avatar, AvatarAction, Contact, Content, ContentInput, EffectContent, Group, Poll,
    PollChoice, PollOption, Reaction, Rename, Typing, TypingState, Voice,
};
use crate::error::{Result, UnsupportedError};
use crate::platform::{Message, PlatformMessageRecord, PlatformRuntime, SpaceRef};
use crate::utils::{from_vcard, to_vcard};

pub const IMESSAGE_PLATFORM: &str = "iMessage";
const PART_PREFIX: &str = "p:";
const GROUP_ITEM_ALLOWED: [&str; 4] = ["text", "attachment", "contact", "voice"];
const MAX_GROUP_TEXT_ITEMS: usize = 1;
const URL_BALLOON_BUNDLE_ID: &str = "com.apple.messages.URLBalloonProvider";
const DEFAULT_CACHE_MAX: usize = 1000;
pub const SHARED_PHONE: &str = "shared";

pub type AttachmentGuid = String;
pub type ChatGuid = String;
pub type MessageGuid = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplyTarget {
    Message(MessageGuid),
    Part {
        guid: MessageGuid,
        part_index: usize,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendOptions {
    pub reply_to: Option<ReplyTarget>,
    pub effect: Option<String>,
    pub enable_link_preview: bool,
    pub is_audio_message: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImessageSentMessage {
    pub guid: MessageGuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImessagePoll {
    pub poll_message_guid: MessageGuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImessagePollInfo {
    pub poll_message_guid: MessageGuid,
    pub title: String,
    pub options: Vec<ImessagePollOptionInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImessagePollOptionInfo {
    pub option_identifier: Option<String>,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImessageUpload {
    pub attachment_guid: AttachmentGuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImessagePart {
    pub text: Option<String>,
    pub attachment_guid: Option<AttachmentGuid>,
    pub attachment_name: Option<String>,
    pub bubble_index: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppleMessageContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balloon_bundle_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AppleAttachment>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub raw: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppleAttachment {
    pub guid: String,
    pub file_name: String,
    pub mime_type: String,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppleMessage {
    pub guid: String,
    #[serde(default)]
    pub is_from_me: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_address: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chat_guids: Vec<String>,
    pub content: AppleMessageContent,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub raw: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceivedEvent {
    pub message: AppleMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_guid: Option<String>,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImessageActor {
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReactionEventKind {
    Love,
    Like,
    Dislike,
    Laugh,
    Emphasize,
    Question,
    Emoji(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionAddedEvent {
    pub chat_guid: String,
    pub message_guid: String,
    pub target_part_index: Option<usize>,
    pub reaction: ReactionEventKind,
    pub actor: Option<ImessageActor>,
    #[serde(default)]
    pub is_from_me: bool,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollEvent {
    pub chat_guid: String,
    pub poll_message_guid: String,
    pub actor: Option<ImessageActor>,
    #[serde(default)]
    pub is_from_me: bool,
    pub sequence: u64,
    pub delta: PollDelta,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PollDelta {
    Created {
        title: String,
        options: Vec<PollEventOption>,
    },
    OptionAdded {
        title: String,
        options: Vec<PollEventOption>,
    },
    Voted {
        option_identifier: String,
    },
    Unvoted {
        option_identifier: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollEventOption {
    pub option_identifier: Option<String>,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImessageStreamEvent {
    MessageReceived(ReceivedEvent),
    ReactionAdded(ReactionAddedEvent),
    PollChanged(PollEvent),
    CatchupComplete {
        head_sequence: u64,
    },
    Unknown {
        event_type: String,
        sequence: u64,
        message_guid: Option<String>,
    },
}

impl ImessageStreamEvent {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::MessageReceived(event) => event.sequence,
            Self::ReactionAdded(event) => event.sequence,
            Self::PollChanged(event) => event.sequence,
            Self::CatchupComplete { head_sequence } => *head_sequence,
            Self::Unknown { sequence, .. } => *sequence,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ImessageStreamItem {
    pub cursor: String,
    pub id: String,
    pub values: Vec<PlatformMessageRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImessageReaction {
    Love,
    Like,
    Dislike,
    Laugh,
    Emphasize,
    Question,
    Emoji(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhotoAction {
    Set { mime_type: String, data: Bytes },
    Clear,
}

#[async_trait]
pub trait ImessageRemoteApi: Send + Sync {
    async fn upload_attachment(&self, data: Bytes, file_name: &str) -> Result<ImessageUpload>;

    async fn download_attachment(&self, attachment_guid: &str) -> Result<Bytes>;

    async fn get_message(&self, chat: &str, message_guid: &str) -> Result<Option<AppleMessage>>;

    async fn get_poll(&self, poll_message_guid: &str) -> Result<Option<ImessagePollInfo>>;

    async fn send_text(
        &self,
        chat: &str,
        text: &str,
        options: SendOptions,
    ) -> Result<ImessageSentMessage>;

    async fn send_attachment(
        &self,
        chat: &str,
        attachment_guid: &str,
        options: SendOptions,
    ) -> Result<ImessageSentMessage>;

    async fn send_multipart(
        &self,
        chat: &str,
        parts: Vec<ImessagePart>,
    ) -> Result<ImessageSentMessage>;

    async fn create_poll(
        &self,
        chat: &str,
        title: &str,
        options: Vec<String>,
    ) -> Result<ImessagePoll>;

    async fn edit_message(
        &self,
        chat: &str,
        message_guid: &str,
        text: &str,
        part_index: Option<usize>,
    ) -> Result<()>;

    async fn set_reaction(
        &self,
        chat: &str,
        message_guid: &str,
        reaction: ImessageReaction,
        part_index: Option<usize>,
    ) -> Result<()>;

    async fn set_typing(&self, chat: &str, typing: bool) -> Result<()>;

    async fn mark_read(&self, chat: &str) -> Result<()>;

    async fn set_display_name(&self, chat: &str, display_name: &str) -> Result<()>;

    async fn set_icon(&self, chat: &str, data: Bytes) -> Result<()>;

    async fn remove_icon(&self, chat: &str) -> Result<()>;

    async fn set_background(&self, chat: &str, data: Bytes) -> Result<()>;

    async fn remove_background(&self, chat: &str) -> Result<()>;
}

#[derive(Clone)]
pub struct ImessageClient<A> {
    api: Arc<A>,
    phone: String,
    message_cache: Arc<Mutex<ImessageMessageCache>>,
    poll_cache: Arc<Mutex<ImessagePollCache>>,
}

impl<A> ImessageClient<A> {
    pub fn new(api: Arc<A>, phone: impl Into<String>) -> Self {
        Self {
            api,
            phone: phone.into(),
            message_cache: Arc::new(Mutex::new(ImessageMessageCache::default())),
            poll_cache: Arc::new(Mutex::new(ImessagePollCache::default())),
        }
    }

    pub fn phone(&self) -> &str {
        &self.phone
    }
}

impl<A> ImessageClient<A>
where
    A: ImessageRemoteApi + 'static,
{
    pub async fn send(
        &self,
        space: &SpaceRef,
        content: impl Into<ContentInput>,
    ) -> Result<Option<PlatformMessageRecord>> {
        dispatch_imessage_content(
            self.api.as_ref(),
            &space.id,
            content.into().resolve().await?,
        )
        .await
    }

    pub async fn get_message(
        &self,
        space: &SpaceRef,
        message_id: &str,
    ) -> Result<Option<PlatformMessageRecord>> {
        let mut cache = self.message_cache.lock().await;
        get_imessage_message(
            self.api.as_ref(),
            &mut cache,
            &space.id,
            message_id,
            &self.phone,
        )
        .await
    }

    pub async fn inbound_messages(
        &self,
        event: ReceivedEvent,
    ) -> Result<Vec<PlatformMessageRecord>> {
        let mut cache = self.message_cache.lock().await;
        to_imessage_inbound_messages(self.api.as_ref(), &mut cache, event, &self.phone).await
    }

    pub async fn reaction_messages(
        &self,
        event: ReactionAddedEvent,
    ) -> Result<Vec<PlatformMessageRecord>> {
        let mut cache = self.message_cache.lock().await;
        to_imessage_reaction_messages(self.api.as_ref(), &mut cache, event, &self.phone).await
    }

    pub async fn poll_delta_messages(
        &self,
        event: PollEvent,
    ) -> Result<Vec<PlatformMessageRecord>> {
        let mut cache = self.poll_cache.lock().await;
        to_imessage_poll_delta_messages(self.api.as_ref(), &mut cache, event, &self.phone).await
    }

    pub async fn process_stream_event(
        &self,
        event: ImessageStreamEvent,
    ) -> Result<ImessageStreamItem> {
        let cursor = event.sequence().to_string();
        match event {
            ImessageStreamEvent::MessageReceived(event) => {
                let id = event.message.guid.clone();
                let values = if event.message.is_from_me {
                    Vec::new()
                } else {
                    self.inbound_messages(event).await?
                };
                Ok(ImessageStreamItem { cursor, id, values })
            }
            ImessageStreamEvent::ReactionAdded(event) => {
                let id = format!("{}:reaction:{}", event.message_guid, event.sequence);
                let values = if is_event_from_current_account(
                    event.is_from_me,
                    event.actor.as_ref(),
                    &self.phone,
                ) {
                    Vec::new()
                } else {
                    self.reaction_messages(event).await?
                };
                Ok(ImessageStreamItem { cursor, id, values })
            }
            ImessageStreamEvent::PollChanged(event) => {
                let id = format!("{}:poll:{}", event.poll_message_guid, event.sequence);
                let values = if is_event_from_current_account(
                    event.is_from_me,
                    event.actor.as_ref(),
                    &self.phone,
                ) {
                    let mut cache = self.poll_cache.lock().await;
                    cache_imessage_poll_event(&mut cache, &event);
                    Vec::new()
                } else {
                    self.poll_delta_messages(event).await?
                };
                Ok(ImessageStreamItem { cursor, id, values })
            }
            ImessageStreamEvent::CatchupComplete { head_sequence } => Ok(ImessageStreamItem {
                cursor: head_sequence.to_string(),
                id: format!("catchup.complete:{head_sequence}"),
                values: Vec::new(),
            }),
            ImessageStreamEvent::Unknown {
                event_type,
                sequence,
                message_guid,
            } => Ok(ImessageStreamItem {
                cursor,
                id: format!(
                    "{}:{}:{}",
                    event_type,
                    message_guid.as_deref().unwrap_or("unknown"),
                    sequence
                ),
                values: Vec::new(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct ImessageRuntime<A> {
    client: ImessageClient<A>,
}

impl<A> ImessageRuntime<A> {
    pub fn new(client: ImessageClient<A>) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &ImessageClient<A> {
        &self.client
    }
}

#[async_trait]
impl<A> PlatformRuntime for ImessageRuntime<A>
where
    A: ImessageRemoteApi + 'static,
{
    fn name(&self) -> &str {
        IMESSAGE_PLATFORM
    }

    async fn send(
        &self,
        space: &SpaceRef,
        content: Content,
    ) -> Result<Option<PlatformMessageRecord>> {
        dispatch_imessage_content(self.client.api.as_ref(), &space.id, content).await
    }

    async fn get_message(
        &self,
        space: &SpaceRef,
        message_id: &str,
    ) -> Result<Option<PlatformMessageRecord>> {
        self.client.get_message(space, message_id).await
    }
}

pub fn dm_chat_guid(address: &str) -> ChatGuid {
    format!("any;-;{address}")
}

pub fn to_chat_guid(value: &str) -> ChatGuid {
    value.to_string()
}

pub fn to_message_guid(value: &str) -> MessageGuid {
    value.to_string()
}

pub fn format_child_id(part_index: usize, parent_guid: &str) -> String {
    format!("{PART_PREFIX}{part_index}/{parent_guid}")
}

pub fn parse_tapback_target(target: &str) -> (String, usize) {
    parse_child_id(target)
        .map(|child| (child.parent_guid, child.part_index))
        .unwrap_or_else(|| (target.to_string(), 0))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildId {
    pub parent_guid: String,
    pub part_index: usize,
}

pub fn parse_child_id(id: &str) -> Option<ChildId> {
    let rest = id.strip_prefix(PART_PREFIX)?;
    let (part_index, parent_guid) = rest.split_once('/')?;
    Some(ChildId {
        parent_guid: parent_guid.to_string(),
        part_index: part_index.parse().ok()?,
    })
}

pub fn validate_group_content(content: &Group) -> Result<()> {
    let mut text_count = 0;
    for sub in &content.items {
        let item_type = sub.content.content_type();
        if !GROUP_ITEM_ALLOWED.contains(&item_type) {
            return Err(unsupported_remote_content(
                "group",
                Some(format!(
                    "\"{item_type}\" items are not supported inside a group"
                )),
            )
            .into());
        }
        if item_type == "text" {
            text_count += 1;
            if text_count > MAX_GROUP_TEXT_ITEMS {
                return Err(unsupported_remote_content(
                    "group",
                    Some(format!(
                        "groups can contain at most {MAX_GROUP_TEXT_ITEMS} text item"
                    )),
                )
                .into());
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ImessageMessageCache {
    max: usize,
    order: VecDeque<String>,
    messages: HashMap<String, PlatformMessageRecord>,
}

#[derive(Clone, Debug)]
pub struct CachedImessagePoll {
    pub poll: Poll,
    pub options_by_identifier: HashMap<String, PollChoice>,
}

#[derive(Clone, Debug)]
pub struct ImessagePollCache {
    max: usize,
    order: VecDeque<String>,
    polls: HashMap<String, CachedImessagePoll>,
}

impl Default for ImessagePollCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_MAX)
    }
}

impl ImessagePollCache {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            order: VecDeque::new(),
            polls: HashMap::new(),
        }
    }

    pub fn get(&self, id: &str) -> Option<&CachedImessagePoll> {
        self.polls.get(id)
    }

    pub fn set(&mut self, id: impl Into<String>, poll: CachedImessagePoll) {
        let id = id.into();
        if self.polls.contains_key(&id) {
            self.order.retain(|candidate| candidate != &id);
        }
        self.order.push_back(id.clone());
        self.polls.insert(id, poll);
        while self.polls.len() > self.max {
            if let Some(oldest) = self.order.pop_front() {
                self.polls.remove(&oldest);
            } else {
                break;
            }
        }
    }

    pub fn clear(&mut self) {
        self.order.clear();
        self.polls.clear();
    }
}

impl Default for ImessageMessageCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_MAX)
    }
}

impl ImessageMessageCache {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            order: VecDeque::new(),
            messages: HashMap::new(),
        }
    }

    pub fn get(&self, id: &str) -> Option<&PlatformMessageRecord> {
        self.messages.get(id)
    }

    pub fn set(&mut self, id: impl Into<String>, message: PlatformMessageRecord) {
        let id = id.into();
        if self.messages.contains_key(&id) {
            self.order.retain(|candidate| candidate != &id);
        }
        self.order.push_back(id.clone());
        self.messages.insert(id, message);
        while self.messages.len() > self.max {
            if let Some(oldest) = self.order.pop_front() {
                self.messages.remove(&oldest);
            } else {
                break;
            }
        }
    }

    pub fn clear(&mut self) {
        self.order.clear();
        self.messages.clear();
    }
}

pub fn cache_imessage_message(cache: &mut ImessageMessageCache, message: PlatformMessageRecord) {
    cache.set(message.id.clone(), message.clone());
    if let Content::Group(group) = &message.content {
        for item in &group.items {
            cache.set(
                item.id.clone(),
                PlatformMessageRecord {
                    id: item.id.clone(),
                    content: item.content.clone(),
                    sender: item.sender.clone(),
                    space: item.space.clone(),
                    extra: item.extra.clone(),
                },
            );
        }
    }
}

pub async fn to_imessage_inbound_messages(
    remote: &dyn ImessageRemoteApi,
    cache: &mut ImessageMessageCache,
    event: ReceivedEvent,
    phone: &str,
) -> Result<Vec<PlatformMessageRecord>> {
    if event.message.is_from_me {
        return Ok(Vec::new());
    }
    let message =
        rebuild_from_apple_message(remote, &event.message, phone, event.chat_guid.as_deref())
            .await?;
    cache_imessage_message(cache, message.clone());
    Ok(vec![message])
}

pub async fn get_imessage_message(
    remote: &dyn ImessageRemoteApi,
    cache: &mut ImessageMessageCache,
    space_id: &str,
    message_id: &str,
    phone: &str,
) -> Result<Option<PlatformMessageRecord>> {
    if let Some(cached) = cache.get(message_id) {
        return Ok(Some(cached.clone()));
    }

    if let Some(child) = parse_child_id(message_id) {
        let Some(parent) = remote
            .get_message(
                &to_chat_guid(space_id),
                &to_message_guid(&child.parent_guid),
            )
            .await?
        else {
            return Ok(None);
        };
        let rebuilt = rebuild_from_apple_message(remote, &parent, phone, Some(space_id)).await?;
        cache_imessage_message(cache, rebuilt);
        return Ok(cache.get(message_id).cloned());
    }

    let Some(fetched) = remote
        .get_message(&to_chat_guid(space_id), &to_message_guid(message_id))
        .await?
    else {
        return Ok(None);
    };
    let rebuilt = rebuild_from_apple_message(remote, &fetched, phone, Some(space_id)).await?;
    cache_imessage_message(cache, rebuilt.clone());
    Ok(Some(rebuilt))
}

pub async fn to_imessage_reaction_messages(
    remote: &dyn ImessageRemoteApi,
    cache: &mut ImessageMessageCache,
    event: ReactionAddedEvent,
    phone: &str,
) -> Result<Vec<PlatformMessageRecord>> {
    if event.is_from_me {
        return Ok(Vec::new());
    }
    let Some(actor) = event.actor else {
        return Ok(Vec::new());
    };
    let Some(emoji) = reaction_event_emoji(&event.reaction) else {
        return Ok(Vec::new());
    };
    let Some(target) = resolve_reaction_target(
        remote,
        cache,
        &event.chat_guid,
        &event.message_guid,
        event.target_part_index,
        phone,
    )
    .await?
    else {
        return Ok(Vec::new());
    };

    let suffix = event
        .target_part_index
        .map(|idx| format!(":{idx}"))
        .unwrap_or_default();
    Ok(vec![PlatformMessageRecord {
        id: format!(
            "{}:reaction:{}{}",
            event.message_guid, event.sequence, suffix
        ),
        content: Content::Reaction(Reaction {
            emoji,
            target: Box::new(target),
        }),
        sender: Some(crate::platform::User {
            id: actor.address,
            platform: IMESSAGE_PLATFORM.to_string(),
            kind: None,
            extra: Map::new(),
        }),
        space: imessage_space_ref(&event.chat_guid, phone),
        extra: Map::new(),
    }])
}

pub fn cache_imessage_poll_event(
    cache: &mut ImessagePollCache,
    event: &PollEvent,
) -> Option<CachedImessagePoll> {
    match &event.delta {
        PollDelta::Created { title, options } | PollDelta::OptionAdded { title, options } => {
            let cached = cached_poll_from_options(title, options);
            cache.set(event.poll_message_guid.clone(), cached.clone());
            Some(cached)
        }
        PollDelta::Voted { .. } | PollDelta::Unvoted { .. } => None,
    }
}

pub async fn to_imessage_poll_delta_messages(
    remote: &dyn ImessageRemoteApi,
    cache: &mut ImessagePollCache,
    event: PollEvent,
    phone: &str,
) -> Result<Vec<PlatformMessageRecord>> {
    cache_imessage_poll_event(cache, &event);
    if event.is_from_me {
        return Ok(Vec::new());
    }
    let (option_identifier, selected) = match &event.delta {
        PollDelta::Voted { option_identifier } => (option_identifier.as_str(), true),
        PollDelta::Unvoted { option_identifier } => (option_identifier.as_str(), false),
        PollDelta::Created { .. } | PollDelta::OptionAdded { .. } => return Ok(Vec::new()),
    };
    let Some(actor) = event.actor else {
        return Ok(Vec::new());
    };

    if cache.get(&event.poll_message_guid).is_none()
        && let Some(info) = remote.get_poll(&event.poll_message_guid).await?
    {
        cache.set(
            event.poll_message_guid.clone(),
            cached_poll_from_info(&info),
        );
    }
    if cache
        .get(&event.poll_message_guid)
        .is_some_and(|cached| !cached.options_by_identifier.contains_key(option_identifier))
        && let Some(info) = remote.get_poll(&event.poll_message_guid).await?
    {
        cache.set(
            event.poll_message_guid.clone(),
            cached_poll_from_info(&info),
        );
    }

    let Some(cached) = cache.get(&event.poll_message_guid) else {
        return Ok(Vec::new());
    };
    let Some(option) = cached.options_by_identifier.get(option_identifier) else {
        return Ok(Vec::new());
    };
    let action = if selected { "selected" } else { "deselected" };

    Ok(vec![PlatformMessageRecord {
        id: format!(
            "{}:{}:{}:{}:{}",
            event.poll_message_guid, actor.address, option_identifier, action, event.sequence
        ),
        content: Content::PollOption(PollOption {
            option: option.clone(),
            poll: cached.poll.clone(),
            selected,
            title: option.title.clone(),
        }),
        sender: Some(crate::platform::User {
            id: actor.address,
            platform: IMESSAGE_PLATFORM.to_string(),
            kind: None,
            extra: Map::new(),
        }),
        space: imessage_space_ref(&event.chat_guid, phone),
        extra: Map::new(),
    }])
}

pub async fn rebuild_from_apple_message(
    remote: &dyn ImessageRemoteApi,
    message: &AppleMessage,
    phone: &str,
    chat_guid_hint: Option<&str>,
) -> Result<PlatformMessageRecord> {
    let base = inbound_base(message, chat_guid_hint, phone);
    let attachments = &message.content.attachments;

    if attachments.len() == 1 {
        return build_attachment_record(
            remote,
            &base,
            &attachments[0],
            message.guid.clone(),
            0,
            None,
        )
        .await;
    }

    if attachments.len() > 1 {
        let mut items = Vec::with_capacity(attachments.len());
        for (idx, attachment) in attachments.iter().enumerate() {
            let id = format_child_id(idx, &message.guid);
            items.push(
                build_attachment_message(
                    remote,
                    &base,
                    attachment,
                    id,
                    idx,
                    Some(message.guid.as_str()),
                )
                .await?,
            );
        }
        return Ok(inbound_record(
            &base,
            message.guid.clone(),
            Content::Group(Group { items }),
            Map::new(),
        ));
    }

    if message.content.balloon_bundle_id.as_deref() == Some(URL_BALLOON_BUNDLE_ID) {
        return Ok(to_richlink_record(message, &base, message.guid.clone()));
    }

    let content = message
        .content
        .text
        .as_ref()
        .filter(|text| !text.is_empty())
        .map(|text| Content::Text(crate::content::Text { text: text.clone() }))
        .unwrap_or_else(|| {
            Content::Custom(crate::content::Custom {
                raw: serde_json::to_value(message).unwrap_or_else(|_| Value::Object(Map::new())),
            })
        });
    Ok(inbound_record(
        &base,
        message.guid.clone(),
        content,
        Map::new(),
    ))
}

pub async fn send_imessage_content(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    content: Content,
) -> Result<PlatformMessageRecord> {
    let chat = to_chat_guid(space_id);

    if let Content::Group(group_content) = content {
        validate_group_content(&group_content)?;
        let mut resolved = Vec::with_capacity(group_content.items.len());
        for (idx, sub) in group_content.items.iter().enumerate() {
            let mut part = resolve_part(remote, &sub.content).await?;
            part.bubble_index = idx;
            resolved.push(part);
        }
        let message = remote.send_multipart(&chat, resolved).await?;
        let items = group_content
            .items
            .into_iter()
            .enumerate()
            .map(|(idx, sub)| {
                let id = format_child_id(idx, &message.guid);
                let mut item = outbound_group_item(space_id, id, sub.content, idx, &message.guid);
                item.platform = IMESSAGE_PLATFORM.to_string();
                item
            })
            .collect();
        return Ok(outbound_record(
            space_id,
            message.guid,
            Content::Group(Group { items }),
        ));
    }

    send_single_content(remote, space_id, &chat, content, SendOptions::default()).await
}

pub async fn dispatch_imessage_content(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    content: Content,
) -> Result<Option<PlatformMessageRecord>> {
    match content {
        Content::Reply(reply) => {
            reply_to_imessage_message(remote, space_id, &reply.target.id, *reply.content)
                .await
                .map(Some)
        }
        Content::Edit(edit) => {
            edit_imessage_message(remote, space_id, &edit.target.id, *edit.content).await?;
            Ok(None)
        }
        Content::Reaction(reaction) => {
            react_to_imessage_message(remote, space_id, &reaction).await?;
            Ok(None)
        }
        Content::Typing(typing) => {
            set_imessage_typing(remote, space_id, &typing).await?;
            Ok(None)
        }
        Content::Rename(rename) => {
            set_imessage_display_name(remote, space_id, &rename).await?;
            Ok(None)
        }
        Content::Avatar(avatar) => {
            set_imessage_icon(remote, space_id, &avatar).await?;
            Ok(None)
        }
        content => send_imessage_content(remote, space_id, content)
            .await
            .map(Some),
    }
}

pub async fn reply_to_imessage_message(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    message_id: &str,
    content: Content,
) -> Result<PlatformMessageRecord> {
    let chat = to_chat_guid(space_id);
    let options = SendOptions {
        reply_to: Some(reply_target_from_id(message_id)),
        ..SendOptions::default()
    };
    send_single_content(remote, space_id, &chat, content, options).await
}

pub async fn edit_imessage_message(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    message_id: &str,
    content: Content,
) -> Result<()> {
    let Content::Text(text) = content else {
        return Err(unsupported_remote_content(
            content.content_type(),
            Some("only text content can be edited".to_string()),
        )
        .into());
    };
    let child = parse_child_id(message_id);
    let guid = child
        .as_ref()
        .map(|value| value.parent_guid.as_str())
        .unwrap_or(message_id);
    remote
        .edit_message(
            &to_chat_guid(space_id),
            &to_message_guid(guid),
            &text.text,
            child.map(|value| value.part_index),
        )
        .await
}

pub async fn react_to_imessage_message(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    content: &Reaction,
) -> Result<()> {
    let target = &content.target;
    let parent_guid = target
        .extra
        .get("parentId")
        .and_then(Value::as_str)
        .unwrap_or(&target.id);
    let part_index = target
        .extra
        .get("partIndex")
        .and_then(Value::as_u64)
        .map(|value| value as usize);
    remote
        .set_reaction(
            &to_chat_guid(space_id),
            &to_message_guid(parent_guid),
            reaction_to_imessage(&content.emoji),
            part_index,
        )
        .await
}

pub async fn set_imessage_typing(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    content: &Typing,
) -> Result<()> {
    remote
        .set_typing(
            &to_chat_guid(space_id),
            matches!(content.state, TypingState::Start),
        )
        .await
}

pub async fn mark_imessage_read(remote: &dyn ImessageRemoteApi, space_id: &str) -> Result<()> {
    remote.mark_read(&to_chat_guid(space_id)).await
}

pub async fn set_imessage_display_name(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    content: &Rename,
) -> Result<()> {
    remote
        .set_display_name(&to_chat_guid(space_id), &content.display_name)
        .await
}

pub async fn set_imessage_icon(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    content: &Avatar,
) -> Result<()> {
    match &content.action {
        AvatarAction::Set { data, .. } => {
            remote.set_icon(&to_chat_guid(space_id), data.clone()).await
        }
        AvatarAction::Clear => remote.remove_icon(&to_chat_guid(space_id)).await,
    }
}

pub async fn set_imessage_background(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    action: &PhotoAction,
) -> Result<()> {
    match action {
        PhotoAction::Set { data, .. } => {
            remote
                .set_background(&to_chat_guid(space_id), data.clone())
                .await
        }
        PhotoAction::Clear => remote.remove_background(&to_chat_guid(space_id)).await,
    }
}

async fn send_single_content(
    remote: &dyn ImessageRemoteApi,
    space_id: &str,
    chat: &str,
    content: Content,
    options: SendOptions,
) -> Result<PlatformMessageRecord> {
    match content {
        Content::Effect(effect) => {
            let content = effect_content_to_content(*effect.content);
            let options = SendOptions {
                effect: Some(effect.effect),
                ..options
            };
            Box::pin(send_single_content(
                remote, space_id, chat, content, options,
            ))
            .await
        }
        Content::Text(text) => {
            let message = remote.send_text(chat, &text.text, options).await?;
            Ok(outbound_record(space_id, message.guid, Content::Text(text)))
        }
        Content::Richlink(richlink) => {
            let options = SendOptions {
                enable_link_preview: true,
                ..options
            };
            let message = remote.send_text(chat, &richlink.url, options).await?;
            Ok(outbound_record(
                space_id,
                message.guid,
                Content::Richlink(richlink),
            ))
        }
        Content::Attachment(attachment) => {
            let upload = upload_attachment(remote, &attachment).await?;
            let message = remote
                .send_attachment(chat, &upload.attachment_guid, options)
                .await?;
            Ok(outbound_record(
                space_id,
                message.guid,
                Content::Attachment(attachment),
            ))
        }
        Content::Contact(contact) => {
            let (upload, _) = send_contact_attachment(remote, &contact).await?;
            let message = remote
                .send_attachment(chat, &upload.attachment_guid, options)
                .await?;
            Ok(outbound_record(
                space_id,
                message.guid,
                Content::Contact(contact),
            ))
        }
        Content::Voice(voice) => {
            let (upload, _) = upload_voice(remote, &voice).await?;
            let options = SendOptions {
                is_audio_message: true,
                ..options
            };
            let message = remote
                .send_attachment(chat, &upload.attachment_guid, options)
                .await?;
            Ok(outbound_record(
                space_id,
                message.guid,
                Content::Voice(voice),
            ))
        }
        Content::Poll(poll) => {
            if options.reply_to.is_some() {
                return Err(unsupported_remote_content(
                    "poll",
                    Some("polls cannot be sent as replies".to_string()),
                )
                .into());
            }
            let created = remote
                .create_poll(
                    chat,
                    &poll.title,
                    poll.options
                        .iter()
                        .map(|option| option.title.clone())
                        .collect(),
                )
                .await?;
            Ok(outbound_record(
                space_id,
                created.poll_message_guid,
                Content::Poll(poll),
            ))
        }
        other => Err(unsupported_remote_content(other.content_type(), None).into()),
    }
}

async fn resolve_part(remote: &dyn ImessageRemoteApi, content: &Content) -> Result<ImessagePart> {
    match content {
        Content::Text(text) => Ok(ImessagePart {
            text: Some(text.text.clone()),
            attachment_guid: None,
            attachment_name: None,
            bubble_index: 0,
        }),
        Content::Attachment(attachment) => {
            let upload = upload_attachment(remote, attachment).await?;
            Ok(attachment_part(
                upload.attachment_guid,
                attachment.name.clone(),
            ))
        }
        Content::Contact(contact) => {
            let (upload, name) = send_contact_attachment(remote, contact).await?;
            Ok(attachment_part(upload.attachment_guid, name))
        }
        Content::Voice(voice) => {
            let (upload, name) = upload_voice(remote, voice).await?;
            Ok(attachment_part(upload.attachment_guid, name))
        }
        other => Err(unsupported_remote_content(other.content_type(), None).into()),
    }
}

#[derive(Clone, Debug)]
struct InboundBase {
    sender: crate::platform::User,
    space: SpaceRef,
}

fn inbound_base(message: &AppleMessage, chat_guid_hint: Option<&str>, phone: &str) -> InboundBase {
    let chat = chat_guid_hint
        .map(str::to_string)
        .or_else(|| message.chat_guids.first().cloned())
        .unwrap_or_default();
    let mut space_extra = Map::new();
    space_extra.insert(
        "type".to_string(),
        Value::String(if chat.contains(";+;") { "group" } else { "dm" }.to_string()),
    );
    space_extra.insert("phone".to_string(), Value::String(phone.to_string()));
    InboundBase {
        sender: crate::platform::User {
            id: message.sender_address.clone().unwrap_or_default(),
            platform: IMESSAGE_PLATFORM.to_string(),
            kind: None,
            extra: Map::new(),
        },
        space: SpaceRef {
            id: chat,
            platform: IMESSAGE_PLATFORM.to_string(),
            extra: space_extra,
        },
    }
}

async fn build_attachment_record(
    remote: &dyn ImessageRemoteApi,
    base: &InboundBase,
    info: &AppleAttachment,
    id: String,
    part_index: usize,
    parent_id: Option<&str>,
) -> Result<PlatformMessageRecord> {
    let content = attachment_content(remote, info).await;
    let mut extra = Map::new();
    extra.insert("partIndex".to_string(), json!(part_index));
    if let Some(parent_id) = parent_id {
        extra.insert("parentId".to_string(), Value::String(parent_id.to_string()));
    }
    Ok(inbound_record(base, id, content, extra))
}

async fn build_attachment_message(
    remote: &dyn ImessageRemoteApi,
    base: &InboundBase,
    info: &AppleAttachment,
    id: String,
    part_index: usize,
    parent_id: Option<&str>,
) -> Result<Message> {
    let record = build_attachment_record(remote, base, info, id, part_index, parent_id).await?;
    Ok(Message {
        id: record.id,
        content: record.content,
        direction: crate::platform::MessageDirection::Inbound,
        platform: IMESSAGE_PLATFORM.to_string(),
        sender: record.sender,
        space: record.space,
        extra: record.extra,
    })
}

async fn attachment_content(remote: &dyn ImessageRemoteApi, info: &AppleAttachment) -> Content {
    if is_vcard_attachment(Some(&info.mime_type), Some(&info.file_name))
        && let Ok(data) = remote.download_attachment(&info.guid).await
        && let Ok(text) = std::str::from_utf8(&data)
        && let Ok(contact) = from_vcard(text)
    {
        return Content::Contact(Box::new(contact));
    }

    let data = remote
        .download_attachment(&info.guid)
        .await
        .unwrap_or_else(|_| Bytes::new());
    Content::Attachment(Attachment {
        name: info.file_name.clone(),
        mime_type: info.mime_type.clone(),
        size: Some(info.total_bytes),
        data,
    })
}

fn to_richlink_record(
    message: &AppleMessage,
    base: &InboundBase,
    id: String,
) -> PlatformMessageRecord {
    let url = message.content.text.clone().unwrap_or_default();
    let content = if Url::parse(&url).is_ok() {
        Content::Richlink(crate::content::Richlink { url })
    } else if !url.is_empty() {
        Content::Text(crate::content::Text { text: url })
    } else {
        Content::Custom(crate::content::Custom {
            raw: serde_json::to_value(message).unwrap_or_else(|_| Value::Object(Map::new())),
        })
    };
    inbound_record(base, id, content, Map::new())
}

fn inbound_record(
    base: &InboundBase,
    id: String,
    content: Content,
    extra: Map<String, Value>,
) -> PlatformMessageRecord {
    PlatformMessageRecord {
        id,
        content,
        sender: Some(base.sender.clone()),
        space: base.space.clone(),
        extra,
    }
}

async fn resolve_reaction_target(
    remote: &dyn ImessageRemoteApi,
    cache: &mut ImessageMessageCache,
    chat: &str,
    target_guid: &str,
    part_index: Option<usize>,
    phone: &str,
) -> Result<Option<Message>> {
    if cache.get(target_guid).is_none()
        && let Some(fetched) = remote
            .get_message(&to_chat_guid(chat), &to_message_guid(target_guid))
            .await?
    {
        let rebuilt = rebuild_from_apple_message(remote, &fetched, phone, Some(chat)).await?;
        cache_imessage_message(cache, rebuilt);
    }
    let Some(candidate) = cache.get(target_guid) else {
        return Ok(None);
    };
    if let Content::Group(group) = &candidate.content
        && let Some(item) = group.items.get(part_index.unwrap_or(0))
    {
        return Ok(Some(item.clone()));
    }
    Ok(Some(record_to_message(candidate)))
}

fn record_to_message(record: &PlatformMessageRecord) -> Message {
    Message {
        id: record.id.clone(),
        content: record.content.clone(),
        direction: crate::platform::MessageDirection::Inbound,
        platform: IMESSAGE_PLATFORM.to_string(),
        sender: record.sender.clone(),
        space: record.space.clone(),
        extra: record.extra.clone(),
    }
}

fn reaction_event_emoji(reaction: &ReactionEventKind) -> Option<String> {
    Some(match reaction {
        ReactionEventKind::Love => "❤️".to_string(),
        ReactionEventKind::Like => "👍".to_string(),
        ReactionEventKind::Dislike => "👎".to_string(),
        ReactionEventKind::Laugh => "😂".to_string(),
        ReactionEventKind::Emphasize => "‼️".to_string(),
        ReactionEventKind::Question => "❓".to_string(),
        ReactionEventKind::Emoji(value) if !value.is_empty() => value.clone(),
        ReactionEventKind::Emoji(_) => return None,
    })
}

fn cached_poll_from_options(title: &str, options: &[PollEventOption]) -> CachedImessagePoll {
    let choices: Vec<PollChoice> = options
        .iter()
        .map(|option| PollChoice {
            title: option.text.clone(),
        })
        .collect();
    let poll = Poll {
        title: title.to_string(),
        options: choices.clone(),
    };
    let options_by_identifier = options
        .iter()
        .zip(choices)
        .filter_map(|(info, choice)| {
            info.option_identifier
                .as_ref()
                .map(|id| (id.clone(), choice))
        })
        .collect();
    CachedImessagePoll {
        poll,
        options_by_identifier,
    }
}

fn cached_poll_from_info(info: &ImessagePollInfo) -> CachedImessagePoll {
    let options: Vec<PollEventOption> = info
        .options
        .iter()
        .map(|option| PollEventOption {
            option_identifier: option.option_identifier.clone(),
            text: option.text.clone(),
        })
        .collect();
    cached_poll_from_options(&info.title, &options)
}

fn imessage_space_ref(chat_guid: &str, phone: &str) -> SpaceRef {
    let mut extra = Map::new();
    extra.insert(
        "type".to_string(),
        Value::String(
            if chat_guid.contains(";+;") {
                "group"
            } else {
                "dm"
            }
            .to_string(),
        ),
    );
    extra.insert("phone".to_string(), Value::String(phone.to_string()));
    SpaceRef {
        id: chat_guid.to_string(),
        platform: IMESSAGE_PLATFORM.to_string(),
        extra,
    }
}

fn is_event_from_current_account(
    is_from_me: bool,
    actor: Option<&ImessageActor>,
    phone: &str,
) -> bool {
    is_from_me
        || (phone != SHARED_PHONE
            && actor
                .map(|actor| actor.address.as_str() == phone)
                .unwrap_or(false))
}

fn effect_content_to_content(content: EffectContent) -> Content {
    match content {
        EffectContent::Text(text) => Content::Text(text),
        EffectContent::Attachment(attachment) => Content::Attachment(attachment),
    }
}

async fn upload_attachment(
    remote: &dyn ImessageRemoteApi,
    content: &Attachment,
) -> Result<ImessageUpload> {
    remote
        .upload_attachment(content.data.clone(), &content.name)
        .await
}

async fn send_contact_attachment(
    remote: &dyn ImessageRemoteApi,
    content: &Contact,
) -> Result<(ImessageUpload, String)> {
    let vcf = to_vcard(content);
    let name = vcard_file_name(content);
    let upload = remote
        .upload_attachment(Bytes::from(vcf.into_bytes()), &name)
        .await?;
    Ok((upload, name))
}

async fn upload_voice(
    remote: &dyn ImessageRemoteApi,
    content: &Voice,
) -> Result<(ImessageUpload, String)> {
    let audio = ensure_m4a(content.data.clone(), &content.mime_type).await?;
    let name = content
        .name
        .clone()
        .unwrap_or_else(|| "voice.m4a".to_string());
    let upload = remote.upload_attachment(audio.buffer, &name).await?;
    Ok((upload, name))
}

fn attachment_part(attachment_guid: String, attachment_name: String) -> ImessagePart {
    ImessagePart {
        text: None,
        attachment_guid: Some(attachment_guid),
        attachment_name: Some(attachment_name),
        bubble_index: 0,
    }
}

fn reply_target_from_id(message_id: &str) -> ReplyTarget {
    parse_child_id(message_id)
        .map(|child| ReplyTarget::Part {
            guid: to_message_guid(&child.parent_guid),
            part_index: child.part_index,
        })
        .unwrap_or_else(|| ReplyTarget::Message(to_message_guid(message_id)))
}

fn reaction_to_imessage(emoji: &str) -> ImessageReaction {
    match emoji {
        "❤️" => ImessageReaction::Love,
        "👍" => ImessageReaction::Like,
        "👎" => ImessageReaction::Dislike,
        "😂" => ImessageReaction::Laugh,
        "‼️" => ImessageReaction::Emphasize,
        "❓" => ImessageReaction::Question,
        other => ImessageReaction::Emoji(other.to_string()),
    }
}

fn outbound_record(space_id: &str, id: String, content: Content) -> PlatformMessageRecord {
    PlatformMessageRecord {
        id,
        content,
        sender: None,
        space: SpaceRef {
            id: space_id.to_string(),
            platform: IMESSAGE_PLATFORM.to_string(),
            extra: Map::new(),
        },
        extra: Map::new(),
    }
}

fn outbound_group_item(
    space_id: &str,
    id: String,
    content: Content,
    part_index: usize,
    parent_id: &str,
) -> Message {
    let mut extra = Map::new();
    extra.insert("partIndex".to_string(), json!(part_index));
    extra.insert("parentId".to_string(), Value::String(parent_id.to_string()));
    Message {
        id,
        content,
        direction: crate::platform::MessageDirection::Outbound,
        platform: IMESSAGE_PLATFORM.to_string(),
        sender: None,
        space: SpaceRef {
            id: space_id.to_string(),
            platform: IMESSAGE_PLATFORM.to_string(),
            extra: Map::new(),
        },
        extra,
    }
}

fn unsupported_remote_content(
    content_type: impl Into<String>,
    detail: Option<String>,
) -> UnsupportedError {
    UnsupportedError::content(content_type, Some(IMESSAGE_PLATFORM.to_string()), detail)
}

pub fn normalize_mime_type(mime_type: &str) -> String {
    mime_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

pub fn is_vcard_attachment(mime_type: Option<&str>, file_name: Option<&str>) -> bool {
    const VCARD_MIME_TYPES: [&str; 5] = [
        "text/vcard",
        "text/x-vcard",
        "text/directory",
        "application/vcard",
        "application/x-vcard",
    ];
    mime_type
        .map(normalize_mime_type)
        .is_some_and(|mime| VCARD_MIME_TYPES.contains(&mime.as_str()))
        || file_name.is_some_and(|name| name.to_ascii_lowercase().ends_with(".vcf"))
}

pub fn vcard_file_name(contact: &Contact) -> String {
    let base = contact
        .name
        .as_ref()
        .and_then(|name| name.formatted.as_deref())
        .or_else(|| contact.user.as_ref().map(|user| user.id.as_str()))
        .unwrap_or("contact");
    let sanitized = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{sanitized}.vcf")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImessageSpaceRef {
    pub id: String,
    pub phone: String,
    pub space_type: ImessageSpaceType,
}

impl ImessageSpaceRef {
    pub fn to_space_ref(&self) -> SpaceRef {
        let mut extra = Map::new();
        extra.insert("phone".to_string(), Value::String(self.phone.clone()));
        extra.insert(
            "type".to_string(),
            Value::String(
                match self.space_type {
                    ImessageSpaceType::Dm => "dm",
                    ImessageSpaceType::Group => "group",
                }
                .to_string(),
            ),
        );
        SpaceRef {
            id: self.id.clone(),
            platform: IMESSAGE_PLATFORM.to_string(),
            extra,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImessageSpaceType {
    Dm,
    Group,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImessageConfig {
    pub local: bool,
    pub clients: Vec<ImessageClientConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImessageClientConfig {
    pub phone: String,
    pub address: String,
    pub token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::content::{
        ContactName, ContentBuilder, Poll, PollChoice, Richlink, Text, attachment, group, text,
        voice,
    };
    use crate::platform::{BuiltSpace, MessageDirection, PlatformRuntime, User};

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Call {
        Upload {
            name: String,
            data: Bytes,
        },
        SendText {
            chat: String,
            text: String,
            options: SendOptions,
        },
        SendAttachment {
            chat: String,
            guid: String,
            options: SendOptions,
        },
        SendMultipart {
            chat: String,
            parts: Vec<ImessagePart>,
        },
        CreatePoll {
            chat: String,
            title: String,
            options: Vec<String>,
        },
        Edit {
            chat: String,
            guid: String,
            text: String,
            part_index: Option<usize>,
        },
        SetReaction {
            chat: String,
            guid: String,
            reaction: ImessageReaction,
            part_index: Option<usize>,
        },
        SetTyping {
            chat: String,
            typing: bool,
        },
        MarkRead {
            chat: String,
        },
        SetDisplayName {
            chat: String,
            display_name: String,
        },
        SetIcon {
            chat: String,
            data: Bytes,
        },
        RemoveIcon {
            chat: String,
        },
        SetBackground {
            chat: String,
            data: Bytes,
        },
        RemoveBackground {
            chat: String,
        },
    }

    #[derive(Default)]
    struct FakeRemote {
        calls: Arc<Mutex<Vec<Call>>>,
        attachments: Arc<Mutex<HashMap<String, Bytes>>>,
        messages: Arc<Mutex<HashMap<String, AppleMessage>>>,
        polls: Arc<Mutex<HashMap<String, ImessagePollInfo>>>,
    }

    impl FakeRemote {
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        fn with_attachment(self, guid: &str, data: Bytes) -> Self {
            self.attachments
                .lock()
                .unwrap()
                .insert(guid.to_string(), data);
            self
        }

        fn with_message(self, guid: &str, message: AppleMessage) -> Self {
            self.messages
                .lock()
                .unwrap()
                .insert(guid.to_string(), message);
            self
        }

        fn with_poll(self, guid: &str, poll: ImessagePollInfo) -> Self {
            self.polls.lock().unwrap().insert(guid.to_string(), poll);
            self
        }
    }

    #[async_trait]
    impl ImessageRemoteApi for FakeRemote {
        async fn upload_attachment(&self, data: Bytes, file_name: &str) -> Result<ImessageUpload> {
            let mut calls = self.calls.lock().unwrap();
            let guid = format!("a{}", calls.len());
            calls.push(Call::Upload {
                name: file_name.to_string(),
                data,
            });
            Ok(ImessageUpload {
                attachment_guid: guid,
            })
        }

        async fn download_attachment(&self, attachment_guid: &str) -> Result<Bytes> {
            Ok(self
                .attachments
                .lock()
                .unwrap()
                .get(attachment_guid)
                .cloned()
                .unwrap_or_default())
        }

        async fn get_message(
            &self,
            _chat: &str,
            message_guid: &str,
        ) -> Result<Option<AppleMessage>> {
            Ok(self.messages.lock().unwrap().get(message_guid).cloned())
        }

        async fn get_poll(&self, poll_message_guid: &str) -> Result<Option<ImessagePollInfo>> {
            Ok(self.polls.lock().unwrap().get(poll_message_guid).cloned())
        }

        async fn send_text(
            &self,
            chat: &str,
            text: &str,
            options: SendOptions,
        ) -> Result<ImessageSentMessage> {
            self.calls.lock().unwrap().push(Call::SendText {
                chat: chat.to_string(),
                text: text.to_string(),
                options,
            });
            Ok(ImessageSentMessage {
                guid: "m-text".to_string(),
            })
        }

        async fn send_attachment(
            &self,
            chat: &str,
            attachment_guid: &str,
            options: SendOptions,
        ) -> Result<ImessageSentMessage> {
            self.calls.lock().unwrap().push(Call::SendAttachment {
                chat: chat.to_string(),
                guid: attachment_guid.to_string(),
                options,
            });
            Ok(ImessageSentMessage {
                guid: "m-attachment".to_string(),
            })
        }

        async fn send_multipart(
            &self,
            chat: &str,
            parts: Vec<ImessagePart>,
        ) -> Result<ImessageSentMessage> {
            self.calls.lock().unwrap().push(Call::SendMultipart {
                chat: chat.to_string(),
                parts,
            });
            Ok(ImessageSentMessage {
                guid: "m-parent".to_string(),
            })
        }

        async fn create_poll(
            &self,
            chat: &str,
            title: &str,
            options: Vec<String>,
        ) -> Result<ImessagePoll> {
            self.calls.lock().unwrap().push(Call::CreatePoll {
                chat: chat.to_string(),
                title: title.to_string(),
                options,
            });
            Ok(ImessagePoll {
                poll_message_guid: "m-poll".to_string(),
            })
        }

        async fn edit_message(
            &self,
            chat: &str,
            message_guid: &str,
            text: &str,
            part_index: Option<usize>,
        ) -> Result<()> {
            self.calls.lock().unwrap().push(Call::Edit {
                chat: chat.to_string(),
                guid: message_guid.to_string(),
                text: text.to_string(),
                part_index,
            });
            Ok(())
        }

        async fn set_reaction(
            &self,
            chat: &str,
            message_guid: &str,
            reaction: ImessageReaction,
            part_index: Option<usize>,
        ) -> Result<()> {
            self.calls.lock().unwrap().push(Call::SetReaction {
                chat: chat.to_string(),
                guid: message_guid.to_string(),
                reaction,
                part_index,
            });
            Ok(())
        }

        async fn set_typing(&self, chat: &str, typing: bool) -> Result<()> {
            self.calls.lock().unwrap().push(Call::SetTyping {
                chat: chat.to_string(),
                typing,
            });
            Ok(())
        }

        async fn mark_read(&self, chat: &str) -> Result<()> {
            self.calls.lock().unwrap().push(Call::MarkRead {
                chat: chat.to_string(),
            });
            Ok(())
        }

        async fn set_display_name(&self, chat: &str, display_name: &str) -> Result<()> {
            self.calls.lock().unwrap().push(Call::SetDisplayName {
                chat: chat.to_string(),
                display_name: display_name.to_string(),
            });
            Ok(())
        }

        async fn set_icon(&self, chat: &str, data: Bytes) -> Result<()> {
            self.calls.lock().unwrap().push(Call::SetIcon {
                chat: chat.to_string(),
                data,
            });
            Ok(())
        }

        async fn remove_icon(&self, chat: &str) -> Result<()> {
            self.calls.lock().unwrap().push(Call::RemoveIcon {
                chat: chat.to_string(),
            });
            Ok(())
        }

        async fn set_background(&self, chat: &str, data: Bytes) -> Result<()> {
            self.calls.lock().unwrap().push(Call::SetBackground {
                chat: chat.to_string(),
                data,
            });
            Ok(())
        }

        async fn remove_background(&self, chat: &str) -> Result<()> {
            self.calls.lock().unwrap().push(Call::RemoveBackground {
                chat: chat.to_string(),
            });
            Ok(())
        }
    }

    #[test]
    fn parses_and_formats_child_ids() {
        assert_eq!(format_child_id(2, "guid"), "p:2/guid");
        assert_eq!(
            parse_child_id("p:2/guid"),
            Some(ChildId {
                parent_guid: "guid".to_string(),
                part_index: 2
            })
        );
        assert_eq!(parse_tapback_target("p:3/root"), ("root".to_string(), 3));
        assert_eq!(parse_tapback_target("root"), ("root".to_string(), 0));
    }

    #[tokio::test]
    async fn sends_text_and_richlink() {
        let remote = FakeRemote::default();
        let record = send_imessage_content(
            &remote,
            "chat1",
            Content::Text(Text {
                text: "hello".to_string(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(record.id, "m-text");

        send_imessage_content(
            &remote,
            "chat1",
            Content::Richlink(Richlink {
                url: "https://example.com".to_string(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            remote.calls(),
            vec![
                Call::SendText {
                    chat: "chat1".to_string(),
                    text: "hello".to_string(),
                    options: SendOptions::default()
                },
                Call::SendText {
                    chat: "chat1".to_string(),
                    text: "https://example.com".to_string(),
                    options: SendOptions {
                        enable_link_preview: true,
                        ..SendOptions::default()
                    }
                }
            ]
        );
    }

    #[tokio::test]
    async fn runtime_sends_fire_and_forget_and_fetches_messages() {
        let apple = AppleMessage {
            guid: "remote-1".to_string(),
            is_from_me: false,
            sender_address: Some("+15550001111".to_string()),
            chat_guids: vec!["chat1".to_string()],
            content: AppleMessageContent {
                text: Some("from remote".to_string()),
                ..AppleMessageContent::default()
            },
            raw: Map::new(),
        };
        let api = Arc::new(FakeRemote::default().with_message("remote-1", apple));
        let client = ImessageClient::new(api.clone(), "+15559990000");
        let runtime = ImessageRuntime::new(client);
        let space = ImessageSpaceRef {
            id: "chat1".to_string(),
            phone: "+15559990000".to_string(),
            space_type: ImessageSpaceType::Dm,
        }
        .to_space_ref();
        let built = BuiltSpace::new(space, Arc::new(runtime) as Arc<dyn PlatformRuntime>);

        let sent = built.send("hello").await.unwrap().unwrap();
        assert_eq!(sent.id, "m-text");
        assert_eq!(sent.direction, MessageDirection::Outbound);

        built.start_typing().await.unwrap();
        assert_eq!(
            api.calls(),
            vec![
                Call::SendText {
                    chat: "chat1".to_string(),
                    text: "hello".to_string(),
                    options: SendOptions::default()
                },
                Call::SetTyping {
                    chat: "chat1".to_string(),
                    typing: true
                }
            ]
        );

        let fetched = built.get_message("remote-1").await.unwrap().unwrap();
        assert_eq!(fetched.id, "remote-1");
        assert_eq!(fetched.direction, MessageDirection::Inbound);
        assert_eq!(fetched.sender.as_ref().unwrap().id, "+15550001111");
    }

    #[tokio::test]
    async fn stream_event_processing_filters_current_account_and_preserves_cursors() {
        let api = Arc::new(FakeRemote::default());
        let client = ImessageClient::new(api, "+15559990000");

        let outbound = client
            .process_stream_event(ImessageStreamEvent::MessageReceived(ReceivedEvent {
                message: AppleMessage {
                    guid: "m-self".to_string(),
                    is_from_me: true,
                    sender_address: Some("+15559990000".to_string()),
                    chat_guids: vec!["chat1".to_string()],
                    content: AppleMessageContent {
                        text: Some("self".to_string()),
                        ..AppleMessageContent::default()
                    },
                    raw: Map::new(),
                },
                chat_guid: Some("chat1".to_string()),
                sequence: 9,
            }))
            .await
            .unwrap();
        assert_eq!(outbound.cursor, "9");
        assert_eq!(outbound.id, "m-self");
        assert!(outbound.values.is_empty());

        let reaction = client
            .process_stream_event(ImessageStreamEvent::ReactionAdded(ReactionAddedEvent {
                chat_guid: "chat1".to_string(),
                message_guid: "m-target".to_string(),
                target_part_index: None,
                reaction: ReactionEventKind::Like,
                actor: Some(ImessageActor {
                    address: "+15559990000".to_string(),
                }),
                is_from_me: false,
                sequence: 10,
            }))
            .await
            .unwrap();
        assert_eq!(reaction.cursor, "10");
        assert_eq!(reaction.id, "m-target:reaction:10");
        assert!(reaction.values.is_empty());

        let catchup = client
            .process_stream_event(ImessageStreamEvent::CatchupComplete { head_sequence: 11 })
            .await
            .unwrap();
        assert_eq!(catchup.cursor, "11");
        assert_eq!(catchup.id, "catchup.complete:11");
        assert!(catchup.values.is_empty());

        let unknown = client
            .process_stream_event(ImessageStreamEvent::Unknown {
                event_type: "message.deleted".to_string(),
                sequence: 12,
                message_guid: Some("m-old".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(unknown.id, "message.deleted:m-old:12");
    }

    #[tokio::test]
    async fn stream_event_processing_emits_inbound_and_poll_values() {
        let api = Arc::new(FakeRemote::default().with_poll(
            "poll-1",
            ImessagePollInfo {
                poll_message_guid: "poll-1".to_string(),
                title: "Lunch?".to_string(),
                options: vec![ImessagePollOptionInfo {
                    option_identifier: Some("a".to_string()),
                    text: "Sushi".to_string(),
                }],
            },
        ));
        let client = ImessageClient::new(api, "+15559990000");

        let inbound = client
            .process_stream_event(ImessageStreamEvent::MessageReceived(ReceivedEvent {
                message: AppleMessage {
                    guid: "m-in".to_string(),
                    is_from_me: false,
                    sender_address: Some("+15550001111".to_string()),
                    chat_guids: vec!["chat1".to_string()],
                    content: AppleMessageContent {
                        text: Some("hello".to_string()),
                        ..AppleMessageContent::default()
                    },
                    raw: Map::new(),
                },
                chat_guid: Some("chat1".to_string()),
                sequence: 20,
            }))
            .await
            .unwrap();
        assert_eq!(inbound.cursor, "20");
        assert_eq!(inbound.values.len(), 1);
        assert!(matches!(inbound.values[0].content, Content::Text(_)));

        let poll = client
            .process_stream_event(ImessageStreamEvent::PollChanged(PollEvent {
                chat_guid: "chat1".to_string(),
                poll_message_guid: "poll-1".to_string(),
                actor: Some(ImessageActor {
                    address: "+15550001111".to_string(),
                }),
                is_from_me: false,
                sequence: 21,
                delta: PollDelta::Voted {
                    option_identifier: "a".to_string(),
                },
            }))
            .await
            .unwrap();
        assert_eq!(poll.cursor, "21");
        assert_eq!(poll.id, "poll-1:poll:21");
        assert_eq!(poll.values.len(), 1);
        assert!(matches!(poll.values[0].content, Content::PollOption(_)));
    }

    #[tokio::test]
    async fn uploads_voice_as_audio_attachment_without_conversion_for_aac() {
        let remote = FakeRemote::default();
        let voice = voice(Bytes::from_static(b"aac bytes"))
            .options(crate::content::VoiceOptions {
                name: None,
                mime_type: Some("audio/aac".to_string()),
                duration: None,
            })
            .build()
            .await
            .unwrap();

        let record = send_imessage_content(&remote, "chat1", voice)
            .await
            .unwrap();
        assert_eq!(record.id, "m-attachment");
        assert_eq!(
            remote.calls(),
            vec![
                Call::Upload {
                    name: "voice.m4a".to_string(),
                    data: Bytes::from_static(b"aac bytes")
                },
                Call::SendAttachment {
                    chat: "chat1".to_string(),
                    guid: "a0".to_string(),
                    options: SendOptions {
                        is_audio_message: true,
                        ..SendOptions::default()
                    }
                }
            ]
        );
    }

    #[tokio::test]
    async fn sends_group_as_multipart_and_returns_child_records() {
        let remote = FakeRemote::default();
        let content = group(vec![
            text("hello").build().await.unwrap(),
            attachment(Bytes::from_static(b"file"))
                .options(crate::content::AttachmentOptions {
                    name: Some("file.txt".to_string()),
                    mime_type: Some("text/plain".to_string()),
                })
                .build()
                .await
                .unwrap(),
        ])
        .unwrap();

        let record = send_imessage_content(&remote, "chat1", content)
            .await
            .unwrap();
        assert_eq!(record.id, "m-parent");
        let Content::Group(group) = record.content else {
            panic!("expected group");
        };
        assert_eq!(group.items[0].id, "p:0/m-parent");
        assert_eq!(group.items[1].id, "p:1/m-parent");
        assert_eq!(group.items[1].extra["parentId"], json!("m-parent"));

        assert_eq!(
            remote.calls(),
            vec![
                Call::Upload {
                    name: "file.txt".to_string(),
                    data: Bytes::from_static(b"file")
                },
                Call::SendMultipart {
                    chat: "chat1".to_string(),
                    parts: vec![
                        ImessagePart {
                            text: Some("hello".to_string()),
                            attachment_guid: None,
                            attachment_name: None,
                            bubble_index: 0
                        },
                        ImessagePart {
                            text: None,
                            attachment_guid: Some("a0".to_string()),
                            attachment_name: Some("file.txt".to_string()),
                            bubble_index: 1
                        }
                    ]
                }
            ]
        );
    }

    #[tokio::test]
    async fn rejects_group_with_multiple_text_items() {
        let remote = FakeRemote::default();
        let content = group(vec![
            text("one").build().await.unwrap(),
            text("two").build().await.unwrap(),
        ])
        .unwrap();
        let err = send_imessage_content(&remote, "chat1", content)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at most 1 text item"));
    }

    #[tokio::test]
    async fn replies_to_child_part_and_rejects_reply_poll() {
        let remote = FakeRemote::default();
        reply_to_imessage_message(
            &remote,
            "chat1",
            "p:4/root",
            Content::Text(Text {
                text: "reply".to_string(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            remote.calls()[0],
            Call::SendText {
                chat: "chat1".to_string(),
                text: "reply".to_string(),
                options: SendOptions {
                    reply_to: Some(ReplyTarget::Part {
                        guid: "root".to_string(),
                        part_index: 4
                    }),
                    ..SendOptions::default()
                }
            }
        );

        let err = reply_to_imessage_message(
            &remote,
            "chat1",
            "root",
            Content::Poll(Poll {
                title: "pick".to_string(),
                options: vec![
                    PollChoice {
                        title: "a".to_string(),
                    },
                    PollChoice {
                        title: "b".to_string(),
                    },
                ],
            }),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("polls cannot be sent as replies"));
    }

    #[tokio::test]
    async fn edits_text_and_maps_child_part() {
        let remote = FakeRemote::default();
        edit_imessage_message(
            &remote,
            "chat1",
            "p:2/root",
            Content::Text(Text {
                text: "new".to_string(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            remote.calls(),
            vec![Call::Edit {
                chat: "chat1".to_string(),
                guid: "root".to_string(),
                text: "new".to_string(),
                part_index: Some(2)
            }]
        );
    }

    #[tokio::test]
    async fn dispatches_reactions_to_parent_or_part() {
        let remote = FakeRemote::default();
        let mut target = Message::outbound(
            IMESSAGE_PLATFORM,
            "p:1/root",
            SpaceRef {
                id: "chat1".to_string(),
                platform: IMESSAGE_PLATFORM.to_string(),
                extra: Map::new(),
            },
            None,
            Content::Text(Text {
                text: "target".to_string(),
            }),
        );
        target
            .extra
            .insert("parentId".to_string(), Value::String("root".to_string()));
        target.extra.insert("partIndex".to_string(), json!(1));

        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Reaction(Reaction {
                emoji: "👍".to_string(),
                target: Box::new(target.clone()),
            }),
        )
        .await
        .unwrap();
        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Reaction(Reaction {
                emoji: "🔥".to_string(),
                target: Box::new(target),
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            remote.calls(),
            vec![
                Call::SetReaction {
                    chat: "chat1".to_string(),
                    guid: "root".to_string(),
                    reaction: ImessageReaction::Like,
                    part_index: Some(1)
                },
                Call::SetReaction {
                    chat: "chat1".to_string(),
                    guid: "root".to_string(),
                    reaction: ImessageReaction::Emoji("🔥".to_string()),
                    part_index: Some(1)
                }
            ]
        );
    }

    #[tokio::test]
    async fn dispatches_typing_rename_avatar_read_and_background_controls() {
        let remote = FakeRemote::default();
        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Typing(Typing {
                state: TypingState::Start,
            }),
        )
        .await
        .unwrap();
        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Typing(Typing {
                state: TypingState::Stop,
            }),
        )
        .await
        .unwrap();
        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Rename(Rename {
                display_name: "New Name".to_string(),
            }),
        )
        .await
        .unwrap();
        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Avatar(Avatar {
                action: AvatarAction::Set {
                    mime_type: "image/png".to_string(),
                    data: Bytes::from_static(b"icon"),
                },
            }),
        )
        .await
        .unwrap();
        dispatch_imessage_content(
            &remote,
            "chat1",
            Content::Avatar(Avatar {
                action: AvatarAction::Clear,
            }),
        )
        .await
        .unwrap();
        mark_imessage_read(&remote, "chat1").await.unwrap();
        set_imessage_background(
            &remote,
            "chat1",
            &PhotoAction::Set {
                mime_type: "image/jpeg".to_string(),
                data: Bytes::from_static(b"background"),
            },
        )
        .await
        .unwrap();
        set_imessage_background(&remote, "chat1", &PhotoAction::Clear)
            .await
            .unwrap();

        assert_eq!(
            remote.calls(),
            vec![
                Call::SetTyping {
                    chat: "chat1".to_string(),
                    typing: true
                },
                Call::SetTyping {
                    chat: "chat1".to_string(),
                    typing: false
                },
                Call::SetDisplayName {
                    chat: "chat1".to_string(),
                    display_name: "New Name".to_string()
                },
                Call::SetIcon {
                    chat: "chat1".to_string(),
                    data: Bytes::from_static(b"icon")
                },
                Call::RemoveIcon {
                    chat: "chat1".to_string()
                },
                Call::MarkRead {
                    chat: "chat1".to_string()
                },
                Call::SetBackground {
                    chat: "chat1".to_string(),
                    data: Bytes::from_static(b"background")
                },
                Call::RemoveBackground {
                    chat: "chat1".to_string()
                }
            ]
        );
    }

    #[tokio::test]
    async fn inbound_text_richlink_and_vcard_messages_are_mapped_and_cached() {
        let vcard = "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Jane Doe\r\nTEL;TYPE=CELL:+15551234567\r\nEND:VCARD\r\n";
        let remote = FakeRemote::default().with_attachment("att-vcf", Bytes::from(vcard));
        let mut cache = ImessageMessageCache::default();

        let text_event = ReceivedEvent {
            message: AppleMessage {
                guid: "m-text".to_string(),
                is_from_me: false,
                sender_address: Some("+15550001111".to_string()),
                chat_guids: vec!["chat1".to_string()],
                content: AppleMessageContent {
                    text: Some("hello".to_string()),
                    ..AppleMessageContent::default()
                },
                raw: Map::new(),
            },
            chat_guid: None,
            sequence: 1,
        };
        let messages =
            to_imessage_inbound_messages(&remote, &mut cache, text_event, "+15559990000")
                .await
                .unwrap();
        assert_eq!(messages[0].id, "m-text");
        assert_eq!(messages[0].sender.as_ref().unwrap().id, "+15550001111");
        assert_eq!(messages[0].space.extra["type"], json!("dm"));
        assert_eq!(
            messages[0].content,
            Content::Text(Text {
                text: "hello".to_string()
            })
        );
        assert!(cache.get("m-text").is_some());

        let richlink = rebuild_from_apple_message(
            &remote,
            &AppleMessage {
                guid: "m-link".to_string(),
                is_from_me: false,
                sender_address: Some("+15550001111".to_string()),
                chat_guids: vec!["chat1".to_string()],
                content: AppleMessageContent {
                    text: Some("https://example.com".to_string()),
                    balloon_bundle_id: Some(URL_BALLOON_BUNDLE_ID.to_string()),
                    ..AppleMessageContent::default()
                },
                raw: Map::new(),
            },
            "+15559990000",
            None,
        )
        .await
        .unwrap();
        assert!(matches!(richlink.content, Content::Richlink(_)));

        let contact = rebuild_from_apple_message(
            &remote,
            &AppleMessage {
                guid: "m-vcard".to_string(),
                is_from_me: false,
                sender_address: Some("+15550001111".to_string()),
                chat_guids: vec!["chat1".to_string()],
                content: AppleMessageContent {
                    attachments: vec![AppleAttachment {
                        guid: "att-vcf".to_string(),
                        file_name: "jane.vcf".to_string(),
                        mime_type: "text/vcard".to_string(),
                        total_bytes: 10,
                    }],
                    ..AppleMessageContent::default()
                },
                raw: Map::new(),
            },
            "+15559990000",
            None,
        )
        .await
        .unwrap();
        assert!(matches!(contact.content, Content::Contact(_)));
        assert_eq!(contact.extra["partIndex"], json!(0));
    }

    #[tokio::test]
    async fn inbound_multi_attachment_message_becomes_group_and_get_child_uses_cache() {
        let apple = AppleMessage {
            guid: "m-parent".to_string(),
            is_from_me: false,
            sender_address: Some("+15550001111".to_string()),
            chat_guids: vec!["chat;+;group".to_string()],
            content: AppleMessageContent {
                attachments: vec![
                    AppleAttachment {
                        guid: "att-1".to_string(),
                        file_name: "one.txt".to_string(),
                        mime_type: "text/plain".to_string(),
                        total_bytes: 3,
                    },
                    AppleAttachment {
                        guid: "att-2".to_string(),
                        file_name: "two.txt".to_string(),
                        mime_type: "text/plain".to_string(),
                        total_bytes: 3,
                    },
                ],
                ..AppleMessageContent::default()
            },
            raw: Map::new(),
        };
        let remote = FakeRemote::default()
            .with_attachment("att-1", Bytes::from_static(b"one"))
            .with_attachment("att-2", Bytes::from_static(b"two"))
            .with_message("m-parent", apple);
        let mut cache = ImessageMessageCache::default();

        let child = get_imessage_message(
            &remote,
            &mut cache,
            "chat;+;group",
            "p:1/m-parent",
            "+15559990000",
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(child.id, "p:1/m-parent");
        assert_eq!(child.extra["parentId"], json!("m-parent"));
        assert_eq!(child.space.extra["type"], json!("group"));
        let Content::Attachment(attachment) = child.content else {
            panic!("expected attachment");
        };
        assert_eq!(attachment.name, "two.txt");
        assert_eq!(attachment.data, Bytes::from_static(b"two"));
        assert!(cache.get("m-parent").is_some());
    }

    #[tokio::test]
    async fn reaction_event_resolves_cached_or_fetched_group_targets() {
        let apple = AppleMessage {
            guid: "m-parent".to_string(),
            is_from_me: false,
            sender_address: Some("+15550001111".to_string()),
            chat_guids: vec!["chat;+;group".to_string()],
            content: AppleMessageContent {
                attachments: vec![
                    AppleAttachment {
                        guid: "att-1".to_string(),
                        file_name: "one.txt".to_string(),
                        mime_type: "text/plain".to_string(),
                        total_bytes: 3,
                    },
                    AppleAttachment {
                        guid: "att-2".to_string(),
                        file_name: "two.txt".to_string(),
                        mime_type: "text/plain".to_string(),
                        total_bytes: 3,
                    },
                ],
                ..AppleMessageContent::default()
            },
            raw: Map::new(),
        };
        let remote = FakeRemote::default()
            .with_attachment("att-1", Bytes::from_static(b"one"))
            .with_attachment("att-2", Bytes::from_static(b"two"))
            .with_message("m-parent", apple);
        let mut cache = ImessageMessageCache::default();

        let messages = to_imessage_reaction_messages(
            &remote,
            &mut cache,
            ReactionAddedEvent {
                chat_guid: "chat;+;group".to_string(),
                message_guid: "m-parent".to_string(),
                target_part_index: Some(1),
                reaction: ReactionEventKind::Like,
                actor: Some(ImessageActor {
                    address: "+15552223333".to_string(),
                }),
                is_from_me: false,
                sequence: 42,
            },
            "+15559990000",
        )
        .await
        .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "m-parent:reaction:42:1");
        let Content::Reaction(reaction) = &messages[0].content else {
            panic!("expected reaction");
        };
        assert_eq!(reaction.emoji, "👍");
        assert_eq!(reaction.target.id, "p:1/m-parent");
        assert_eq!(messages[0].space.extra["type"], json!("group"));
    }

    #[tokio::test]
    async fn poll_events_cache_metadata_and_emit_vote_messages() {
        let remote = FakeRemote::default().with_poll(
            "poll-1",
            ImessagePollInfo {
                poll_message_guid: "poll-1".to_string(),
                title: "Lunch?".to_string(),
                options: vec![
                    ImessagePollOptionInfo {
                        option_identifier: Some("a".to_string()),
                        text: "Sushi".to_string(),
                    },
                    ImessagePollOptionInfo {
                        option_identifier: Some("b".to_string()),
                        text: "Pizza".to_string(),
                    },
                ],
            },
        );
        let mut cache = ImessagePollCache::default();

        let created = to_imessage_poll_delta_messages(
            &remote,
            &mut cache,
            PollEvent {
                chat_guid: "chat1".to_string(),
                poll_message_guid: "poll-1".to_string(),
                actor: Some(ImessageActor {
                    address: "+15552223333".to_string(),
                }),
                is_from_me: false,
                sequence: 1,
                delta: PollDelta::Created {
                    title: "Lunch?".to_string(),
                    options: vec![PollEventOption {
                        option_identifier: Some("a".to_string()),
                        text: "Sushi".to_string(),
                    }],
                },
            },
            "+15559990000",
        )
        .await
        .unwrap();
        assert!(created.is_empty());
        assert!(cache.get("poll-1").is_some());

        let voted = to_imessage_poll_delta_messages(
            &remote,
            &mut cache,
            PollEvent {
                chat_guid: "chat1".to_string(),
                poll_message_guid: "poll-1".to_string(),
                actor: Some(ImessageActor {
                    address: "+15552223333".to_string(),
                }),
                is_from_me: false,
                sequence: 2,
                delta: PollDelta::Voted {
                    option_identifier: "b".to_string(),
                },
            },
            "+15559990000",
        )
        .await
        .unwrap();

        assert_eq!(voted.len(), 1);
        assert_eq!(voted[0].id, "poll-1:+15552223333:b:selected:2");
        let Content::PollOption(option) = &voted[0].content else {
            panic!("expected poll option");
        };
        assert!(option.selected);
        assert_eq!(option.title, "Pizza");
        assert_eq!(option.poll.title, "Lunch?");

        let unvoted = to_imessage_poll_delta_messages(
            &remote,
            &mut cache,
            PollEvent {
                chat_guid: "chat1".to_string(),
                poll_message_guid: "poll-1".to_string(),
                actor: Some(ImessageActor {
                    address: "+15552223333".to_string(),
                }),
                is_from_me: false,
                sequence: 3,
                delta: PollDelta::Unvoted {
                    option_identifier: "b".to_string(),
                },
            },
            "+15559990000",
        )
        .await
        .unwrap();
        let Content::PollOption(option) = &unvoted[0].content else {
            panic!("expected poll option");
        };
        assert!(!option.selected);
    }

    #[test]
    fn message_and_poll_caches_evict_oldest_entries() {
        let mut message_cache = ImessageMessageCache::new(2);
        for id in ["m1", "m2", "m3"] {
            message_cache.set(
                id,
                PlatformMessageRecord {
                    id: id.to_string(),
                    content: Content::Text(Text {
                        text: id.to_string(),
                    }),
                    sender: None,
                    space: SpaceRef {
                        id: "chat1".to_string(),
                        platform: IMESSAGE_PLATFORM.to_string(),
                        extra: Map::new(),
                    },
                    extra: Map::new(),
                },
            );
        }
        assert!(message_cache.get("m1").is_none());
        assert!(message_cache.get("m2").is_some());
        assert!(message_cache.get("m3").is_some());

        let mut poll_cache = ImessagePollCache::new(1);
        for id in ["p1", "p2"] {
            poll_cache.set(
                id,
                CachedImessagePoll {
                    poll: Poll {
                        title: id.to_string(),
                        options: vec![PollChoice {
                            title: "yes".to_string(),
                        }],
                    },
                    options_by_identifier: HashMap::new(),
                },
            );
        }
        assert!(poll_cache.get("p1").is_none());
        assert!(poll_cache.get("p2").is_some());
    }

    #[tokio::test]
    async fn get_message_returns_none_for_missing_parent_or_out_of_range_child() {
        let apple = AppleMessage {
            guid: "m-parent".to_string(),
            is_from_me: false,
            sender_address: Some("+15550001111".to_string()),
            chat_guids: vec!["chat1".to_string()],
            content: AppleMessageContent {
                attachments: vec![AppleAttachment {
                    guid: "att-1".to_string(),
                    file_name: "one.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    total_bytes: 3,
                }],
                ..AppleMessageContent::default()
            },
            raw: Map::new(),
        };
        let remote = FakeRemote::default()
            .with_attachment("att-1", Bytes::from_static(b"one"))
            .with_message("m-parent", apple);
        let mut cache = ImessageMessageCache::default();

        let missing = get_imessage_message(
            &remote,
            &mut cache,
            "chat1",
            "p:0/m-missing",
            "+15559990000",
        )
        .await
        .unwrap();
        assert!(missing.is_none());

        let out_of_range =
            get_imessage_message(&remote, &mut cache, "chat1", "p:2/m-parent", "+15559990000")
                .await
                .unwrap();
        assert!(out_of_range.is_none());
    }

    #[tokio::test]
    async fn reaction_events_without_actor_emoji_or_target_are_ignored() {
        let remote = FakeRemote::default();
        let mut cache = ImessageMessageCache::default();

        let without_actor = to_imessage_reaction_messages(
            &remote,
            &mut cache,
            ReactionAddedEvent {
                chat_guid: "chat1".to_string(),
                message_guid: "m1".to_string(),
                target_part_index: None,
                reaction: ReactionEventKind::Like,
                actor: None,
                is_from_me: false,
                sequence: 1,
            },
            "+15559990000",
        )
        .await
        .unwrap();
        assert!(without_actor.is_empty());

        let empty_emoji = to_imessage_reaction_messages(
            &remote,
            &mut cache,
            ReactionAddedEvent {
                chat_guid: "chat1".to_string(),
                message_guid: "m1".to_string(),
                target_part_index: None,
                reaction: ReactionEventKind::Emoji(String::new()),
                actor: Some(ImessageActor {
                    address: "+15551112222".to_string(),
                }),
                is_from_me: false,
                sequence: 2,
            },
            "+15559990000",
        )
        .await
        .unwrap();
        assert!(empty_emoji.is_empty());

        let missing_target = to_imessage_reaction_messages(
            &remote,
            &mut cache,
            ReactionAddedEvent {
                chat_guid: "chat1".to_string(),
                message_guid: "m1".to_string(),
                target_part_index: None,
                reaction: ReactionEventKind::Like,
                actor: Some(ImessageActor {
                    address: "+15551112222".to_string(),
                }),
                is_from_me: false,
                sequence: 3,
            },
            "+15559990000",
        )
        .await
        .unwrap();
        assert!(missing_target.is_empty());
    }

    #[tokio::test]
    async fn shared_phone_does_not_filter_same_address_reactions() {
        let api = Arc::new(FakeRemote::default());
        let client = ImessageClient::new(api, SHARED_PHONE);

        client
            .process_stream_event(ImessageStreamEvent::MessageReceived(ReceivedEvent {
                message: AppleMessage {
                    guid: "m-target".to_string(),
                    is_from_me: false,
                    sender_address: Some("+15550001111".to_string()),
                    chat_guids: vec!["chat1".to_string()],
                    content: AppleMessageContent {
                        text: Some("target".to_string()),
                        ..AppleMessageContent::default()
                    },
                    raw: Map::new(),
                },
                chat_guid: Some("chat1".to_string()),
                sequence: 1,
            }))
            .await
            .unwrap();

        let reaction = client
            .process_stream_event(ImessageStreamEvent::ReactionAdded(ReactionAddedEvent {
                chat_guid: "chat1".to_string(),
                message_guid: "m-target".to_string(),
                target_part_index: None,
                reaction: ReactionEventKind::Like,
                actor: Some(ImessageActor {
                    address: SHARED_PHONE.to_string(),
                }),
                is_from_me: false,
                sequence: 2,
            }))
            .await
            .unwrap();

        assert_eq!(reaction.values.len(), 1);
        assert!(matches!(reaction.values[0].content, Content::Reaction(_)));
    }

    #[tokio::test]
    async fn current_account_poll_metadata_is_cached_for_later_votes() {
        let api = Arc::new(FakeRemote::default());
        let client = ImessageClient::new(api, "+15559990000");

        let created = client
            .process_stream_event(ImessageStreamEvent::PollChanged(PollEvent {
                chat_guid: "chat1".to_string(),
                poll_message_guid: "poll-1".to_string(),
                actor: Some(ImessageActor {
                    address: "+15559990000".to_string(),
                }),
                is_from_me: false,
                sequence: 1,
                delta: PollDelta::Created {
                    title: "Lunch?".to_string(),
                    options: vec![PollEventOption {
                        option_identifier: Some("a".to_string()),
                        text: "Sushi".to_string(),
                    }],
                },
            }))
            .await
            .unwrap();
        assert!(created.values.is_empty());

        let voted = client
            .process_stream_event(ImessageStreamEvent::PollChanged(PollEvent {
                chat_guid: "chat1".to_string(),
                poll_message_guid: "poll-1".to_string(),
                actor: Some(ImessageActor {
                    address: "+15551112222".to_string(),
                }),
                is_from_me: false,
                sequence: 2,
                delta: PollDelta::Voted {
                    option_identifier: "a".to_string(),
                },
            }))
            .await
            .unwrap();

        assert_eq!(voted.values.len(), 1);
        let Content::PollOption(option) = &voted.values[0].content else {
            panic!("expected poll option");
        };
        assert_eq!(option.title, "Sushi");
    }

    #[test]
    fn detects_vcard_and_sanitizes_contact_filename() {
        assert!(is_vcard_attachment(Some("Text/VCard; charset=utf-8"), None));
        assert!(is_vcard_attachment(None, Some("person.vcf")));
        let contact = Contact {
            user: Some(User {
                id: "fallback".to_string(),
                platform: "test".to_string(),
                kind: None,
                extra: Map::new(),
            }),
            name: Some(ContactName {
                formatted: Some("Jane / Doe".to_string()),
                ..ContactName::default()
            }),
            phones: Vec::new(),
            emails: Vec::new(),
            addresses: Vec::new(),
            org: None,
            urls: Vec::new(),
            birthday: None,
            note: None,
            photo: None,
            raw: None,
        };
        assert_eq!(vcard_file_name(&contact), "Jane___Doe.vcf");
    }

    fn _assert_record_shape(record: PlatformMessageRecord) {
        assert_eq!(record.space.platform, IMESSAGE_PLATFORM);
        assert_eq!(record.sender, None);
        let _direction = MessageDirection::Outbound;
    }
}
