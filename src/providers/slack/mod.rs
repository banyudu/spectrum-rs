use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::content::{Attachment, Content, ContentInput, Custom, Reaction, Text};
use crate::error::{Result, SpectrumError, UnsupportedError};
use crate::platform::{Message, MessageDirection, PlatformMessageRecord, Space, SpaceRef, User};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackTeamMeta {
    pub app_id: String,
    pub bot_user_id: String,
    pub granted_scopes: Vec<String>,
    pub team_name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackConfig {
    pub endpoint: Option<String>,
    pub teams: std::collections::BTreeMap<String, SlackTeamMeta>,
    pub tokens: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackSpaceRef {
    pub id: String,
    pub team_id: String,
}

impl SlackSpaceRef {
    pub fn to_space_ref(&self) -> SpaceRef {
        let mut extra = Map::new();
        extra.insert("teamId".to_string(), Value::String(self.team_id.clone()));
        SpaceRef {
            id: self.id.clone(),
            platform: "Slack".to_string(),
            extra,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackFile {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub size: Option<u64>,
    #[serde(default)]
    pub bytes: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackFileShare {
    pub channel: String,
    pub ts: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackSendResult {
    pub ts: String,
    pub channel: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackUploadResult {
    pub file: SlackFile,
    pub shares: Vec<SlackFileShare>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackInboundMessage {
    pub ts: String,
    pub channel: String,
    pub user: String,
    pub text: Option<String>,
    pub files: Vec<SlackFile>,
    pub is_from_me: bool,
    pub thread_ts: Option<String>,
    pub subtype: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackMention {
    pub ts: String,
    pub channel: String,
    pub user: String,
    pub text: String,
    pub is_from_me: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackReaction {
    pub item_channel: String,
    pub item_ts: String,
    pub name: String,
    pub removed: bool,
    pub user: String,
    pub is_from_me: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SlackEvent {
    Message {
        team_id: String,
        message: SlackInboundMessage,
    },
    Reaction {
        team_id: String,
        reaction: SlackReaction,
    },
    Mention {
        team_id: String,
        mention: SlackMention,
    },
    Custom {
        team_id: String,
        raw: Value,
    },
}

#[async_trait]
pub trait SlackApi: Send + Sync {
    async fn send_message(
        &self,
        team_id: &str,
        channel: &str,
        text: &str,
        thread_ts: Option<&str>,
    ) -> Result<SlackSendResult>;

    async fn upload_file(
        &self,
        team_id: &str,
        channel: &str,
        filename: &str,
        mime_type: &str,
        content: Bytes,
        thread_ts: Option<&str>,
    ) -> Result<SlackUploadResult>;

    async fn send_reaction(
        &self,
        team_id: &str,
        channel: &str,
        item_ts: &str,
        emoji: &str,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct SlackClient<A> {
    api: Arc<A>,
}

impl<A> SlackClient<A> {
    pub fn new(api: Arc<A>) -> Self {
        Self { api }
    }
}

impl<A> SlackClient<A>
where
    A: SlackApi + 'static,
{
    pub async fn send(
        &self,
        space: &SlackSpaceRef,
        content: impl Into<ContentInput>,
    ) -> Result<Option<PlatformMessageRecord>> {
        send_slack_content(self.api.as_ref(), space, content.into().resolve().await?).await
    }
}

#[derive(Clone)]
pub struct SlackSpace<A> {
    client: SlackClient<A>,
    space: SlackSpaceRef,
}

impl<A> SlackSpace<A> {
    pub fn new(client: SlackClient<A>, space: SlackSpaceRef) -> Self {
        Self { client, space }
    }
}

#[async_trait]
impl<A> Space for SlackSpace<A>
where
    A: SlackApi + 'static,
{
    fn id(&self) -> &str {
        &self.space.id
    }

    fn platform(&self) -> &str {
        "Slack"
    }

    async fn send(&self, content: ContentInput) -> Result<Option<Message>> {
        let raw = self.client.send(&self.space, content).await?;
        raw.map(|record| {
            crate::platform::wrap_provider_message(record, "Slack", MessageDirection::Outbound)
        })
        .transpose()
    }
}

pub fn slack_event_to_messages(event: SlackEvent) -> Vec<PlatformMessageRecord> {
    match event {
        SlackEvent::Message { team_id, message } => slack_message_to_records(&team_id, message),
        SlackEvent::Reaction { team_id, reaction } => {
            vec![slack_reaction_to_record(&team_id, reaction)]
        }
        SlackEvent::Mention { team_id, mention } => vec![PlatformMessageRecord {
            id: mention.ts.clone(),
            content: Content::Text(Text { text: mention.text }),
            sender: Some(slack_user(mention.user)),
            space: slack_space(&mention.channel, &team_id),
            extra: slack_message_extra(mention.is_from_me, Some(mention.ts), None, None),
        }],
        SlackEvent::Custom { .. } => Vec::new(),
    }
}

fn slack_message_to_records(
    team_id: &str,
    message: SlackInboundMessage,
) -> Vec<PlatformMessageRecord> {
    let mut records = Vec::new();
    if let Some(text) = &message.text
        && !text.is_empty()
    {
        let id = if message.files.is_empty() {
            message.ts.clone()
        } else {
            format!("{}:text", message.ts)
        };
        records.push(PlatformMessageRecord {
            id,
            content: Content::Text(Text { text: text.clone() }),
            sender: Some(slack_user(message.user.clone())),
            space: slack_space(&message.channel, team_id),
            extra: slack_message_extra(
                message.is_from_me,
                Some(message.ts.clone()),
                message.thread_ts.clone(),
                message.subtype.clone(),
            ),
        });
    }

    for (index, file) in message.files.iter().enumerate() {
        let single_file =
            message.files.len() == 1 && message.text.as_deref().unwrap_or_default().is_empty();
        records.push(PlatformMessageRecord {
            id: if single_file {
                message.ts.clone()
            } else {
                format!("{}:file:{index}", message.ts)
            },
            content: Content::Attachment(Attachment {
                name: file.name.clone(),
                mime_type: file.mime_type.clone(),
                size: file.size,
                data: file.bytes.clone(),
            }),
            sender: Some(slack_user(message.user.clone())),
            space: slack_space(&message.channel, team_id),
            extra: slack_message_extra(
                message.is_from_me,
                Some(message.ts.clone()),
                message.thread_ts.clone(),
                message.subtype.clone(),
            ),
        });
    }

    if records.is_empty() {
        records.push(PlatformMessageRecord {
            id: message.ts.clone(),
            content: Content::Custom(Custom {
                raw: json!({ "slack_type": "empty" }),
            }),
            sender: Some(slack_user(message.user)),
            space: slack_space(&message.channel, team_id),
            extra: slack_message_extra(
                message.is_from_me,
                Some(message.ts),
                message.thread_ts,
                message.subtype,
            ),
        });
    }

    records
}

fn slack_reaction_to_record(team_id: &str, reaction: SlackReaction) -> PlatformMessageRecord {
    let target = Message {
        id: reaction.item_ts.clone(),
        content: Content::Custom(Custom {
            raw: json!({ "slack_type": "reaction-target", "stub": true }),
        }),
        direction: MessageDirection::Inbound,
        platform: "Slack".to_string(),
        sender: Some(slack_user(String::new())),
        space: slack_space(&reaction.item_channel, team_id),
        extra: Map::new(),
    };
    PlatformMessageRecord {
        id: format!(
            "{}:reaction:{}:{}",
            reaction.item_ts, reaction.user, reaction.name
        ),
        content: Content::Reaction(Reaction {
            emoji: reaction.name,
            target: Box::new(target),
        }),
        sender: Some(slack_user(reaction.user)),
        space: slack_space(&reaction.item_channel, team_id),
        extra: slack_message_extra(
            reaction.is_from_me,
            Some(reaction.item_ts),
            None,
            Some(if reaction.removed {
                "reaction_removed".to_string()
            } else {
                "reaction_added".to_string()
            }),
        ),
    }
}

pub async fn send_slack_content<A>(
    api: &A,
    space: &SlackSpaceRef,
    content: Content,
) -> Result<Option<PlatformMessageRecord>>
where
    A: SlackApi,
{
    match content {
        Content::Reply(reply) => {
            let target_ts = slack_message_ts(&reply.target);
            send_regular_content(api, space, *reply.content, Some(&target_ts))
                .await
                .map(Some)
        }
        Content::Reaction(reaction) => {
            let target_ts = slack_message_ts(&reaction.target);
            api.send_reaction(&space.team_id, &space.id, &target_ts, &reaction.emoji)
                .await?;
            Ok(None)
        }
        Content::Typing(_) => Ok(None),
        content => send_regular_content(api, space, content, None)
            .await
            .map(Some),
    }
}

async fn send_regular_content<A>(
    api: &A,
    space: &SlackSpaceRef,
    content: Content,
    thread_ts: Option<&str>,
) -> Result<PlatformMessageRecord>
where
    A: SlackApi,
{
    match content {
        Content::Text(Text { text }) => {
            let result = api
                .send_message(&space.team_id, &space.id, &text, thread_ts)
                .await?;
            Ok(to_record(result, space, Content::Text(Text { text })))
        }
        Content::Attachment(attachment) => {
            let result = api
                .upload_file(
                    &space.team_id,
                    &space.id,
                    &attachment.name,
                    &attachment.mime_type,
                    attachment.data.clone(),
                    thread_ts,
                )
                .await?;
            Ok(to_upload_record(
                result,
                space,
                Content::Attachment(attachment),
            ))
        }
        Content::Voice(voice) => {
            let filename = voice
                .name
                .clone()
                .unwrap_or_else(|| mime_to_media_name(&voice.mime_type, "voice"));
            let result = api
                .upload_file(
                    &space.team_id,
                    &space.id,
                    &filename,
                    &voice.mime_type,
                    voice.data.clone(),
                    thread_ts,
                )
                .await?;
            Ok(to_upload_record(result, space, Content::Voice(voice)))
        }
        other => {
            Err(
                UnsupportedError::content(other.content_type(), Some("Slack".to_string()), None)
                    .into(),
            )
        }
    }
}

fn to_record(
    result: SlackSendResult,
    space: &SlackSpaceRef,
    content: Content,
) -> PlatformMessageRecord {
    PlatformMessageRecord {
        id: result.ts.clone(),
        content,
        sender: None,
        space: slack_space(&result.channel, &space.team_id),
        extra: slack_message_extra(true, Some(result.ts), None, None),
    }
}

fn to_upload_record(
    result: SlackUploadResult,
    space: &SlackSpaceRef,
    content: Content,
) -> PlatformMessageRecord {
    let share_ts = result
        .shares
        .iter()
        .find(|share| share.channel == space.id)
        .map(|share| share.ts.clone());
    PlatformMessageRecord {
        id: share_ts.clone().unwrap_or(result.file.id),
        content,
        sender: None,
        space: slack_space(&space.id, &space.team_id),
        extra: slack_message_extra(true, share_ts, None, None),
    }
}

fn slack_space(channel: &str, team_id: &str) -> SpaceRef {
    SlackSpaceRef {
        id: channel.to_string(),
        team_id: team_id.to_string(),
    }
    .to_space_ref()
}

fn slack_user(id: String) -> User {
    User {
        id,
        platform: "Slack".to_string(),
        kind: None,
        extra: Map::new(),
    }
}

fn slack_message_extra(
    is_from_me: bool,
    ts: Option<String>,
    thread_ts: Option<String>,
    subtype: Option<String>,
) -> Map<String, Value> {
    let mut extra = Map::new();
    extra.insert("isFromMe".to_string(), Value::Bool(is_from_me));
    if let Some(ts) = ts {
        extra.insert("ts".to_string(), Value::String(ts));
    }
    if let Some(thread_ts) = thread_ts {
        extra.insert("threadTs".to_string(), Value::String(thread_ts));
    }
    if let Some(subtype) = subtype {
        extra.insert("subtype".to_string(), Value::String(subtype));
    }
    extra
}

fn slack_message_ts(message: &Message) -> String {
    message
        .extra
        .get("ts")
        .and_then(Value::as_str)
        .unwrap_or(&message.id)
        .to_string()
}

fn mime_to_media_name(mime_type: &str, fallback: &str) -> String {
    mime_type
        .split_once('/')
        .map(|(_, suffix)| format!("{fallback}.{suffix}"))
        .unwrap_or_else(|| fallback.to_string())
}

pub fn resolve_slack_space(
    users: &[User],
    channel: Option<String>,
    team_id: Option<String>,
) -> Result<SlackSpaceRef> {
    let team_id = team_id.ok_or_else(|| {
        SpectrumError::msg(
            "Slack space creation requires a teamId param. Pass it via slack.space({ channel, teamId }) or slack.space([user], { teamId }).",
        )
    })?;
    if let Some(channel) = channel {
        return Ok(SlackSpaceRef {
            id: channel,
            team_id,
        });
    }
    match users {
        [] => Err(SpectrumError::msg(
            "Slack space creation requires either a channel param or at least one user",
        )),
        [user] => Ok(SlackSpaceRef {
            id: user.id.clone(),
            team_id,
        }),
        _ => Err(UnsupportedError::action(
            "createSpace",
            Some("Slack".to_string()),
            Some("group DMs require an explicit channel id (Slack's conversations.open is not exposed); pass `channel` in params".to_string()),
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{ContentBuilder, attachment, reaction, text, typing};
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeSlackApi {
        calls: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl SlackApi for FakeSlackApi {
        async fn send_message(
            &self,
            team_id: &str,
            channel: &str,
            text: &str,
            thread_ts: Option<&str>,
        ) -> Result<SlackSendResult> {
            self.calls.lock().unwrap().push(json!({
                "method": "send",
                "teamId": team_id,
                "channel": channel,
                "text": text,
                "threadTs": thread_ts,
            }));
            Ok(SlackSendResult {
                ts: "1710000000.123456".to_string(),
                channel: channel.to_string(),
            })
        }

        async fn upload_file(
            &self,
            team_id: &str,
            channel: &str,
            filename: &str,
            mime_type: &str,
            content: Bytes,
            thread_ts: Option<&str>,
        ) -> Result<SlackUploadResult> {
            self.calls.lock().unwrap().push(json!({
                "method": "upload",
                "teamId": team_id,
                "channel": channel,
                "filename": filename,
                "mimeType": mime_type,
                "len": content.len(),
                "threadTs": thread_ts,
            }));
            Ok(SlackUploadResult {
                file: SlackFile {
                    id: "file-1".to_string(),
                    name: filename.to_string(),
                    mime_type: mime_type.to_string(),
                    size: Some(content.len() as u64),
                    bytes: content,
                },
                shares: vec![SlackFileShare {
                    channel: channel.to_string(),
                    ts: "1710000001.000001".to_string(),
                }],
            })
        }

        async fn send_reaction(
            &self,
            team_id: &str,
            channel: &str,
            item_ts: &str,
            emoji: &str,
        ) -> Result<()> {
            self.calls.lock().unwrap().push(json!({
                "method": "reaction",
                "teamId": team_id,
                "channel": channel,
                "itemTs": item_ts,
                "emoji": emoji,
            }));
            Ok(())
        }
    }

    #[test]
    fn slack_message_with_text_and_files_fans_out() {
        let records = slack_event_to_messages(SlackEvent::Message {
            team_id: "T1".to_string(),
            message: SlackInboundMessage {
                ts: "1710000000.123456".to_string(),
                channel: "C1".to_string(),
                user: "U1".to_string(),
                text: Some("hello".to_string()),
                files: vec![SlackFile {
                    id: "F1".to_string(),
                    name: "a.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    size: Some(3),
                    bytes: Bytes::from_static(b"abc"),
                }],
                is_from_me: false,
                thread_ts: Some("1709999999.000001".to_string()),
                subtype: None,
            },
        });
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id, "1710000000.123456:text");
        assert_eq!(records[1].id, "1710000000.123456:file:0");
        assert_eq!(records[0].extra["threadTs"], "1709999999.000001");
    }

    #[test]
    fn slack_empty_message_becomes_custom_content() {
        let records = slack_event_to_messages(SlackEvent::Message {
            team_id: "T1".to_string(),
            message: SlackInboundMessage {
                ts: "1.0".to_string(),
                channel: "C1".to_string(),
                user: "U1".to_string(),
                text: None,
                files: Vec::new(),
                is_from_me: false,
                thread_ts: None,
                subtype: Some("bot_message".to_string()),
            },
        });
        assert!(matches!(records[0].content, Content::Custom(_)));
        assert_eq!(records[0].extra["subtype"], "bot_message");
    }

    #[tokio::test]
    async fn slack_send_text_calls_api_and_returns_record() {
        let api = FakeSlackApi::default();
        let space = SlackSpaceRef {
            id: "C1".to_string(),
            team_id: "T1".to_string(),
        };
        let record = send_slack_content(&api, &space, text("hello").build().await.unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.id, "1710000000.123456");
        assert_eq!(api.calls.lock().unwrap()[0]["method"], "send");
    }

    #[tokio::test]
    async fn slack_send_attachment_uploads_file() {
        let api = FakeSlackApi::default();
        let space = SlackSpaceRef {
            id: "C1".to_string(),
            team_id: "T1".to_string(),
        };
        let content = attachment(Vec::from("abc"))
            .options(crate::content::AttachmentOptions {
                name: Some("a.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
            })
            .build()
            .await
            .unwrap();
        let record = send_slack_content(&api, &space, content)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.id, "1710000001.000001");
        assert_eq!(api.calls.lock().unwrap()[0]["method"], "upload");
    }

    #[tokio::test]
    async fn slack_reaction_is_fire_and_forget() {
        let api = FakeSlackApi::default();
        let space = SlackSpaceRef {
            id: "C1".to_string(),
            team_id: "T1".to_string(),
        };
        let target = Message {
            id: "fallback".to_string(),
            content: text("target").build().await.unwrap(),
            direction: MessageDirection::Inbound,
            platform: "Slack".to_string(),
            sender: None,
            space: space.to_space_ref(),
            extra: slack_message_extra(false, Some("171.1".to_string()), None, None),
        };
        let sent = send_slack_content(
            &api,
            &space,
            reaction("eyes", target).build().await.unwrap(),
        )
        .await
        .unwrap();
        assert!(sent.is_none());
        assert_eq!(api.calls.lock().unwrap()[0]["itemTs"], "171.1");
    }

    #[tokio::test]
    async fn slack_typing_noops() {
        let api = FakeSlackApi::default();
        let space = SlackSpaceRef {
            id: "C1".to_string(),
            team_id: "T1".to_string(),
        };
        let sent = send_slack_content(&api, &space, typing(crate::content::TypingState::Start))
            .await
            .unwrap();
        assert!(sent.is_none());
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn resolve_slack_space_requires_team_and_handles_dm() {
        assert!(resolve_slack_space(&[], None, None).is_err());
        let user = User {
            id: "U1".to_string(),
            platform: "Slack".to_string(),
            kind: None,
            extra: Map::new(),
        };
        let space = resolve_slack_space(&[user], None, Some("T1".to_string())).unwrap();
        assert_eq!(space.id, "U1");
    }
}
