use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::content::{
    Attachment, Contact, ContactAddress, ContactEmail, ContactName, ContactOrg, ContactPhone,
    ContactPointType, Content, ContentInput, Custom, Poll, PollOption, Reaction, Text,
};
use crate::error::{Result, SpectrumError, UnsupportedError};
use crate::platform::{Message, MessageDirection, PlatformMessageRecord, Space, SpaceRef, User};

const MAX_POLL_CACHE_SIZE: usize = 1000;
const OPTION_ID_PREFIX: &str = "opt_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppConfig {
    pub access_token: String,
    pub app_secret: Option<String>,
    pub phone_number_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppSpaceRef {
    pub id: String,
}

impl WhatsAppSpaceRef {
    pub fn to_space_ref(&self) -> SpaceRef {
        SpaceRef {
            id: self.id.clone(),
            platform: "WhatsApp Business".to_string(),
            extra: Map::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppSendResult {
    pub message_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppMedia {
    pub id: String,
    pub mime_type: String,
    pub filename: Option<String>,
    #[serde(default)]
    pub bytes: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactCard {
    pub name: WhatsAppContactName,
    pub phones: Vec<WhatsAppContactPhone>,
    pub emails: Vec<WhatsAppContactEmail>,
    pub addresses: Vec<WhatsAppContactAddress>,
    pub urls: Vec<WhatsAppContactUrl>,
    pub org: Option<WhatsAppContactOrg>,
    pub birthday: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactName {
    pub formatted_name: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub middle_name: Option<String>,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactPhone {
    pub phone: String,
    pub phone_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactEmail {
    pub email: String,
    pub email_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactAddress {
    pub street: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub zip: Option<String>,
    pub country: Option<String>,
    pub address_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactOrg {
    pub company: Option<String>,
    pub department: Option<String>,
    pub title: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppContactUrl {
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsAppInboundMessage {
    pub id: String,
    pub from: String,
    pub timestamp_ms: u64,
    pub context_id: Option<String>,
    pub content: WhatsAppInboundContent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WhatsAppInboundContent {
    Text { body: String },
    Image { media: WhatsAppMedia },
    Video { media: WhatsAppMedia },
    Audio { media: WhatsAppMedia },
    Document { media: WhatsAppMedia },
    Contacts { contacts: Vec<WhatsAppContactCard> },
    Sticker { raw: Value },
    Location { raw: Value },
    Reaction { message_id: String, emoji: String },
    Interactive { reply_id: String, raw: Value },
    Button { raw: Value },
    Order { raw: Value },
    System { raw: Value },
    Unknown,
}

#[async_trait]
pub trait WhatsAppApi: Send + Sync {
    async fn send_message(&self, to: &str, payload: Value) -> Result<WhatsAppSendResult>;

    async fn upload_media(&self, file: Bytes, mime_type: &str, filename: &str) -> Result<String>;
}

#[derive(Clone)]
pub struct WhatsAppClient<A> {
    api: Arc<A>,
    poll_cache: Arc<Mutex<PollCache>>,
}

#[derive(Default)]
pub struct PollCache {
    values: BTreeMap<String, Poll>,
    order: VecDeque<String>,
}

impl<A> WhatsAppClient<A> {
    pub fn new(api: Arc<A>) -> Self {
        Self {
            api,
            poll_cache: Arc::new(Mutex::new(PollCache::default())),
        }
    }
}

impl<A> WhatsAppClient<A>
where
    A: WhatsAppApi + 'static,
{
    pub async fn send(
        &self,
        space_id: &str,
        content: impl Into<ContentInput>,
    ) -> Result<Option<PlatformMessageRecord>> {
        send_whatsapp_content(
            self.api.as_ref(),
            &self.poll_cache,
            space_id,
            content.into().resolve().await?,
        )
        .await
    }

    pub fn inbound_messages(&self, message: WhatsAppInboundMessage) -> Vec<PlatformMessageRecord> {
        whatsapp_inbound_to_messages(&self.poll_cache, message)
    }
}

#[derive(Clone)]
pub struct WhatsAppSpace<A> {
    client: WhatsAppClient<A>,
    space: WhatsAppSpaceRef,
}

impl<A> WhatsAppSpace<A> {
    pub fn new(client: WhatsAppClient<A>, space: WhatsAppSpaceRef) -> Self {
        Self { client, space }
    }
}

#[async_trait]
impl<A> Space for WhatsAppSpace<A>
where
    A: WhatsAppApi + 'static,
{
    fn id(&self) -> &str {
        &self.space.id
    }

    fn platform(&self) -> &str {
        "WhatsApp Business"
    }

    async fn send(&self, content: ContentInput) -> Result<Option<Message>> {
        let raw = self.client.send(&self.space.id, content).await?;
        raw.map(|record| {
            crate::platform::wrap_provider_message(
                record,
                "WhatsApp Business",
                MessageDirection::Outbound,
            )
        })
        .transpose()
    }
}

pub fn whatsapp_inbound_to_messages(
    poll_cache: &Arc<Mutex<PollCache>>,
    msg: WhatsAppInboundMessage,
) -> Vec<PlatformMessageRecord> {
    let base_space = WhatsAppSpaceRef {
        id: msg.from.clone(),
    }
    .to_space_ref();
    let sender = Some(User {
        id: msg.from.clone(),
        platform: "WhatsApp Business".to_string(),
        kind: None,
        extra: Map::new(),
    });

    if let WhatsAppInboundContent::Contacts { contacts } = msg.content {
        let multi = contacts.len() > 1;
        return contacts
            .into_iter()
            .enumerate()
            .map(|(index, card)| PlatformMessageRecord {
                id: if multi {
                    format!("{}:{index}", msg.id)
                } else {
                    msg.id.clone()
                },
                content: Content::Contact(Box::new(wa_contact_to_spectrum(card))),
                sender: sender.clone(),
                space: base_space.clone(),
                extra: Map::new(),
            })
            .collect();
    }

    let context_id = msg.context_id.clone();
    vec![PlatformMessageRecord {
        id: msg.id,
        content: map_inbound_content(poll_cache, msg.content, context_id),
        sender,
        space: base_space,
        extra: Map::new(),
    }]
}

fn map_inbound_content(
    poll_cache: &Arc<Mutex<PollCache>>,
    content: WhatsAppInboundContent,
    context_id: Option<String>,
) -> Content {
    match content {
        WhatsAppInboundContent::Text { body } => Content::Text(Text { text: body }),
        WhatsAppInboundContent::Image { media }
        | WhatsAppInboundContent::Video { media }
        | WhatsAppInboundContent::Audio { media }
        | WhatsAppInboundContent::Document { media } => media_to_attachment(media),
        WhatsAppInboundContent::Sticker { raw } => custom_with_type("sticker", raw),
        WhatsAppInboundContent::Location { raw } => custom_with_type("location", raw),
        WhatsAppInboundContent::Reaction { message_id, emoji } => {
            let target = Message {
                id: message_id,
                content: Content::Custom(Custom {
                    raw: json!({ "whatsapp_type": "reaction-target", "stub": true }),
                }),
                direction: MessageDirection::Inbound,
                platform: "WhatsApp Business".to_string(),
                sender: None,
                space: SpaceRef {
                    id: String::new(),
                    platform: "WhatsApp Business".to_string(),
                    extra: Map::new(),
                },
                extra: Map::new(),
            };
            Content::Reaction(Reaction {
                emoji,
                target: Box::new(target),
            })
        }
        WhatsAppInboundContent::Interactive { reply_id, raw } => {
            if let Some(context_id) = context_id
                && let Some(index) = option_index_from_id(&reply_id)
                && let Some(poll) = poll_cache.lock().unwrap().values.get(&context_id)
                && let Some(option) = poll.options.get(index)
            {
                return Content::PollOption(PollOption {
                    option: option.clone(),
                    poll: poll.clone(),
                    selected: true,
                    title: option.title.clone(),
                });
            }
            custom_with_type("interactive", raw)
        }
        WhatsAppInboundContent::Button { raw } => custom_with_type("button", raw),
        WhatsAppInboundContent::Order { raw } => custom_with_type("order", raw),
        WhatsAppInboundContent::System { raw } => custom_with_type("system", raw),
        WhatsAppInboundContent::Unknown | WhatsAppInboundContent::Contacts { .. } => {
            custom_with_type("unknown", Value::Null)
        }
    }
}

pub async fn send_whatsapp_content<A>(
    api: &A,
    poll_cache: &Arc<Mutex<PollCache>>,
    space_id: &str,
    content: Content,
) -> Result<Option<PlatformMessageRecord>>
where
    A: WhatsAppApi,
{
    match content {
        Content::Reply(reply) => send_regular_content(
            api,
            poll_cache,
            space_id,
            *reply.content,
            Some(&reply.target.id),
        )
        .await
        .map(Some),
        Content::Reaction(reaction) => {
            api.send_message(
                space_id,
                json!({ "reaction": { "messageId": reaction.target.id, "emoji": reaction.emoji } }),
            )
            .await?;
            Ok(None)
        }
        Content::Typing(_) => Ok(None),
        content => send_regular_content(api, poll_cache, space_id, content, None)
            .await
            .map(Some),
    }
}

async fn send_regular_content<A>(
    api: &A,
    poll_cache: &Arc<Mutex<PollCache>>,
    space_id: &str,
    content: Content,
    reply_to: Option<&str>,
) -> Result<PlatformMessageRecord>
where
    A: WhatsAppApi,
{
    let mut payload = match &content {
        Content::Text(Text { text }) => json!({ "text": text }),
        Content::Attachment(attachment) => media_payload(api, attachment, false).await?,
        Content::Voice(voice) => {
            let filename = voice
                .name
                .clone()
                .unwrap_or_else(|| voice_filename(&voice.mime_type));
            let media_id = api
                .upload_media(voice.data.clone(), &voice.mime_type, &filename)
                .await?;
            json!({ "audio": { "id": media_id } })
        }
        Content::Contact(contact) => json!({ "contacts": [spectrum_contact_to_wa(contact)] }),
        Content::Poll(poll) => json!({ "interactive": poll_to_interactive(poll) }),
        other => {
            return Err(UnsupportedError::content(
                other.content_type(),
                Some("WhatsApp Business".to_string()),
                None,
            )
            .into());
        }
    };
    if let Some(reply_to) = reply_to {
        payload["replyTo"] = Value::String(reply_to.to_string());
    }
    let result = api.send_message(space_id, payload).await?;
    if let Content::Poll(poll) = &content {
        cache_poll(poll_cache, result.message_id.clone(), poll.clone());
    }
    Ok(PlatformMessageRecord {
        id: result.message_id,
        content,
        sender: None,
        space: WhatsAppSpaceRef {
            id: space_id.to_string(),
        }
        .to_space_ref(),
        extra: Map::new(),
    })
}

async fn media_payload<A>(api: &A, attachment: &Attachment, force_document: bool) -> Result<Value>
where
    A: WhatsAppApi,
{
    let media_id = api
        .upload_media(
            attachment.data.clone(),
            &attachment.mime_type,
            &attachment.name,
        )
        .await?;
    let media_type = if force_document {
        "document"
    } else {
        mime_to_media_type(&attachment.mime_type)
    };
    let payload = if media_type == "document" {
        json!({ media_type: { "id": media_id, "filename": attachment.name } })
    } else {
        json!({ media_type: { "id": media_id } })
    };
    Ok(payload)
}

fn media_to_attachment(media: WhatsAppMedia) -> Content {
    Content::Attachment(Attachment {
        name: media
            .filename
            .unwrap_or_else(|| format!("media-{}", media.id)),
        mime_type: media.mime_type,
        size: Some(media.bytes.len() as u64),
        data: media.bytes,
    })
}

fn custom_with_type(kind: &str, raw: Value) -> Content {
    let mut obj = match raw {
        Value::Object(obj) => obj,
        other => {
            let mut obj = Map::new();
            obj.insert("value".to_string(), other);
            obj
        }
    };
    obj.insert("whatsapp_type".to_string(), Value::String(kind.to_string()));
    Content::Custom(Custom {
        raw: Value::Object(obj),
    })
}

pub fn poll_option_id(index: usize) -> String {
    format!("{OPTION_ID_PREFIX}{index}")
}

fn option_index_from_id(id: &str) -> Option<usize> {
    let raw = id.strip_prefix(OPTION_ID_PREFIX)?;
    let index = raw.parse::<usize>().ok()?;
    (poll_option_id(index) == id).then_some(index)
}

pub fn poll_to_interactive(poll: &Poll) -> Value {
    if poll.options.len() <= 3 {
        json!({
            "type": "buttons",
            "body": poll.title,
            "buttons": poll.options.iter().enumerate().map(|(i, option)| {
                json!({ "id": poll_option_id(i), "title": option.title })
            }).collect::<Vec<_>>()
        })
    } else {
        json!({
            "type": "list",
            "body": poll.title,
            "buttonText": "View options",
            "sections": [{
                "title": "Options",
                "rows": poll.options.iter().enumerate().map(|(i, option)| {
                    json!({ "id": poll_option_id(i), "title": option.title })
                }).collect::<Vec<_>>()
            }]
        })
    }
}

fn cache_poll(cache: &Arc<Mutex<PollCache>>, message_id: String, poll: Poll) {
    let mut cache = cache.lock().unwrap();
    cache.values.remove(&message_id);
    cache.order.retain(|id| id != &message_id);
    cache.values.insert(message_id.clone(), poll);
    cache.order.push_back(message_id);
    while cache.order.len() > MAX_POLL_CACHE_SIZE {
        if let Some(oldest) = cache.order.pop_front() {
            cache.values.remove(&oldest);
        }
    }
}

fn mime_to_media_type(mime_type: &str) -> &'static str {
    if mime_type.starts_with("image/") {
        "image"
    } else if mime_type.starts_with("video/") {
        "video"
    } else if mime_type.starts_with("audio/") {
        "audio"
    } else {
        "document"
    }
}

fn voice_filename(mime_type: &str) -> String {
    mime_type
        .split_once('/')
        .map(|(_, ext)| format!("voice.{ext}"))
        .unwrap_or_else(|| "voice".to_string())
}

fn wa_contact_to_spectrum(card: WhatsAppContactCard) -> Contact {
    Contact {
        user: None,
        name: Some(ContactName {
            formatted: Some(card.name.formatted_name),
            first: card.name.first_name,
            last: card.name.last_name,
            middle: card.name.middle_name,
            prefix: card.name.prefix,
            suffix: card.name.suffix,
        }),
        phones: card
            .phones
            .into_iter()
            .map(|phone| ContactPhone {
                value: phone.phone,
                phone_type: map_wa_phone_type(phone.phone_type.as_deref()),
            })
            .collect(),
        emails: card
            .emails
            .into_iter()
            .map(|email| ContactEmail {
                value: email.email,
                email_type: map_wa_simple_type(email.email_type.as_deref()),
            })
            .collect(),
        addresses: card
            .addresses
            .into_iter()
            .map(|address| ContactAddress {
                street: address.street,
                city: address.city,
                region: address.state,
                postal_code: address.zip,
                country: address.country,
                address_type: map_wa_simple_type(address.address_type.as_deref()),
            })
            .collect(),
        org: card.org.map(|org| ContactOrg {
            name: org.company,
            title: org.title,
            department: org.department,
        }),
        urls: card.urls.into_iter().map(|url| url.url).collect(),
        birthday: card.birthday,
        note: None,
        photo: None,
        raw: None,
    }
}

fn spectrum_contact_to_wa(contact: &Contact) -> Value {
    json!({
        "name": {
            "formattedName": contact.name.as_ref().and_then(|n| n.formatted.clone()).unwrap_or_else(|| {
                let Some(name) = &contact.name else { return "Unknown".to_string(); };
                let parts = [name.first.as_deref(), name.middle.as_deref(), name.last.as_deref()]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                if parts.is_empty() { "Unknown".to_string() } else { parts.join(" ") }
            }),
            "firstName": contact.name.as_ref().and_then(|n| n.first.clone()),
            "lastName": contact.name.as_ref().and_then(|n| n.last.clone()),
            "middleName": contact.name.as_ref().and_then(|n| n.middle.clone()),
            "prefix": contact.name.as_ref().and_then(|n| n.prefix.clone()),
            "suffix": contact.name.as_ref().and_then(|n| n.suffix.clone()),
        },
        "phones": contact.phones.iter().map(|phone| json!({
            "phone": phone.value,
            "type": spectrum_phone_type_to_wa(phone.phone_type.as_ref()),
        })).collect::<Vec<_>>(),
        "emails": contact.emails.iter().map(|email| json!({
            "email": email.value,
            "type": spectrum_simple_type_to_wa(email.email_type.as_ref()),
        })).collect::<Vec<_>>(),
        "addresses": contact.addresses.iter().map(|address| json!({
            "street": address.street,
            "city": address.city,
            "state": address.region,
            "zip": address.postal_code,
            "country": address.country,
            "type": spectrum_simple_type_to_wa(address.address_type.as_ref()),
        })).collect::<Vec<_>>(),
        "urls": contact.urls.iter().map(|url| json!({ "url": url })).collect::<Vec<_>>(),
        "org": contact.org.as_ref().map(|org| json!({
            "company": org.name,
            "department": org.department,
            "title": org.title,
        })),
        "birthday": contact.birthday,
    })
}

fn map_wa_phone_type(value: Option<&str>) -> Option<ContactPointType> {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("CELL" | "MOBILE" | "IPHONE") => Some(ContactPointType::Mobile),
        Some("HOME") => Some(ContactPointType::Home),
        Some("WORK" | "BUSINESS") => Some(ContactPointType::Work),
        Some(_) => Some(ContactPointType::Other),
        None => None,
    }
}

fn map_wa_simple_type(value: Option<&str>) -> Option<ContactPointType> {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("HOME") => Some(ContactPointType::Home),
        Some("WORK" | "BUSINESS") => Some(ContactPointType::Work),
        Some(_) => Some(ContactPointType::Other),
        None => None,
    }
}

fn spectrum_phone_type_to_wa(value: Option<&ContactPointType>) -> Option<&'static str> {
    match value {
        Some(ContactPointType::Mobile) => Some("CELL"),
        Some(ContactPointType::Home) => Some("HOME"),
        Some(ContactPointType::Work) => Some("WORK"),
        Some(ContactPointType::Other) => Some("OTHER"),
        None => None,
    }
}

fn spectrum_simple_type_to_wa(value: Option<&ContactPointType>) -> Option<&'static str> {
    match value {
        Some(ContactPointType::Home) => Some("HOME"),
        Some(ContactPointType::Work) => Some("WORK"),
        Some(ContactPointType::Other) => Some("OTHER"),
        Some(ContactPointType::Mobile) => Some("CELL"),
        None => None,
    }
}

pub fn resolve_whatsapp_space(users: &[User]) -> Result<WhatsAppSpaceRef> {
    match users {
        [] => Err(SpectrumError::msg(
            "WhatsApp space creation requires at least one user",
        )),
        [user] => Ok(WhatsAppSpaceRef {
            id: user.id.clone(),
        }),
        _ => Err(UnsupportedError::action(
            "createSpace",
            Some("WhatsApp Business".to_string()),
            Some("only 1:1 conversations are supported".to_string()),
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{ContentBuilder, attachment, option, poll, reaction, text, typing};

    #[derive(Default)]
    struct FakeWaApi {
        calls: Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl WhatsAppApi for FakeWaApi {
        async fn send_message(&self, to: &str, payload: Value) -> Result<WhatsAppSendResult> {
            self.calls
                .lock()
                .unwrap()
                .push(json!({ "to": to, "payload": payload }));
            Ok(WhatsAppSendResult {
                message_id: format!("msg-{}", self.calls.lock().unwrap().len()),
            })
        }

        async fn upload_media(
            &self,
            file: Bytes,
            mime_type: &str,
            filename: &str,
        ) -> Result<String> {
            self.calls.lock().unwrap().push(json!({
                "upload": { "len": file.len(), "mimeType": mime_type, "filename": filename }
            }));
            Ok("media-1".to_string())
        }
    }

    #[test]
    fn inbound_contacts_fan_out() {
        let cache = Arc::new(Mutex::new(PollCache::default()));
        let card = WhatsAppContactCard {
            name: WhatsAppContactName {
                formatted_name: "Ada Lovelace".to_string(),
                first_name: Some("Ada".to_string()),
                last_name: Some("Lovelace".to_string()),
                middle_name: None,
                prefix: None,
                suffix: None,
            },
            phones: vec![WhatsAppContactPhone {
                phone: "+15551234567".to_string(),
                phone_type: Some("CELL".to_string()),
            }],
            emails: Vec::new(),
            addresses: Vec::new(),
            urls: Vec::new(),
            org: None,
            birthday: None,
        };
        let records = whatsapp_inbound_to_messages(
            &cache,
            WhatsAppInboundMessage {
                id: "in-1".to_string(),
                from: "+1555".to_string(),
                timestamp_ms: 0,
                context_id: None,
                content: WhatsAppInboundContent::Contacts {
                    contacts: vec![card.clone(), card],
                },
            },
        );
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id, "in-1:0");
        assert!(matches!(records[0].content, Content::Contact(_)));
    }

    #[test]
    fn inbound_interactive_poll_reply_maps_to_poll_option() {
        let cache = Arc::new(Mutex::new(PollCache::default()));
        let poll = match poll("Pick", vec![option("A"), option("B")]).unwrap() {
            Content::Poll(poll) => poll,
            _ => unreachable!(),
        };
        cache_poll(&cache, "poll-1".to_string(), poll);
        let records = whatsapp_inbound_to_messages(
            &cache,
            WhatsAppInboundMessage {
                id: "in-1".to_string(),
                from: "user".to_string(),
                timestamp_ms: 0,
                context_id: Some("poll-1".to_string()),
                content: WhatsAppInboundContent::Interactive {
                    reply_id: "opt_1".to_string(),
                    raw: json!({}),
                },
            },
        );
        let Content::PollOption(option) = &records[0].content else {
            panic!("expected poll option");
        };
        assert_eq!(option.title, "B");
    }

    #[tokio::test]
    async fn sends_text_payload() {
        let api = FakeWaApi::default();
        let cache = Arc::new(Mutex::new(PollCache::default()));
        let record =
            send_whatsapp_content(&api, &cache, "user", text("hello").build().await.unwrap())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(record.id, "msg-1");
        assert_eq!(api.calls.lock().unwrap()[0]["payload"]["text"], "hello");
    }

    #[tokio::test]
    async fn sends_attachment_as_uploaded_media() {
        let api = FakeWaApi::default();
        let cache = Arc::new(Mutex::new(PollCache::default()));
        let content = attachment(Vec::from("abc"))
            .options(crate::content::AttachmentOptions {
                name: Some("a.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
            })
            .build()
            .await
            .unwrap();
        let record = send_whatsapp_content(&api, &cache, "user", content)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.id, "msg-2");
        assert_eq!(api.calls.lock().unwrap()[0]["upload"]["filename"], "a.txt");
        assert_eq!(
            api.calls.lock().unwrap()[1]["payload"]["document"]["id"],
            "media-1"
        );
    }

    #[tokio::test]
    async fn sends_poll_and_caches_for_reply_mapping() {
        let api = FakeWaApi::default();
        let cache = Arc::new(Mutex::new(PollCache::default()));
        let content = poll(
            "Pick",
            vec![option("A"), option("B"), option("C"), option("D")],
        )
        .unwrap();
        let record = send_whatsapp_content(&api, &cache, "user", content)
            .await
            .unwrap()
            .unwrap();
        assert!(cache.lock().unwrap().values.contains_key(&record.id));
        assert_eq!(
            api.calls.lock().unwrap()[0]["payload"]["interactive"]["type"],
            "list"
        );
    }

    #[tokio::test]
    async fn reaction_and_typing_are_fire_and_forget() {
        let api = FakeWaApi::default();
        let cache = Arc::new(Mutex::new(PollCache::default()));
        let target = Message {
            id: "target".to_string(),
            content: text("target").build().await.unwrap(),
            direction: MessageDirection::Inbound,
            platform: "WhatsApp Business".to_string(),
            sender: None,
            space: WhatsAppSpaceRef {
                id: "user".to_string(),
            }
            .to_space_ref(),
            extra: Map::new(),
        };
        assert!(
            send_whatsapp_content(
                &api,
                &cache,
                "user",
                reaction("👍", target).build().await.unwrap()
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            send_whatsapp_content(
                &api,
                &cache,
                "user",
                typing(crate::content::TypingState::Start)
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(api.calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn resolve_space_is_one_to_one_only() {
        assert!(resolve_whatsapp_space(&[]).is_err());
        let user = User {
            id: "1555".to_string(),
            platform: "WhatsApp Business".to_string(),
            kind: None,
            extra: Map::new(),
        };
        assert_eq!(resolve_whatsapp_space(&[user]).unwrap().id, "1555");
    }
}
