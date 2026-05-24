pub mod audio;
pub mod cloud;
pub mod content;
pub mod error;
pub mod platform;
pub mod providers;
pub mod spectrum;
pub mod store;
pub mod stream;
pub mod types;
pub mod utils;

pub use audio::{M4aAudio, ensure_m4a, is_m4a, is_m4a_mime_type, resolve_ffmpeg_path};
pub use cloud::{
    CloudClient, CloudPlatform, DedicatedTokenData, ImessageInfoData, PlatformStatus,
    PlatformsData, SharedTokenData, SlackTeamMeta, SlackTokenData, SpectrumCloudError,
    SubscriptionData, SubscriptionStatus, TokenData, WhatsappBusinessTokenData, cloud,
};
pub use content::{
    Attachment, Avatar, AvatarAction, Contact, ContactAddress, ContactEmail, ContactName,
    ContactOrg, ContactPhone, Content, ContentBuilder, ContentInput, Edit, Group, Poll, PollChoice,
    PollOption, Reaction, Rename, Reply, Richlink, Text, Typing, TypingState, Voice, attachment,
    avatar, contact, custom, edit, group, option, poll, reaction, rename, reply, resolve_contents,
    richlink, text, typing, voice,
};
pub use error::{Result, SpectrumError, UnsupportedError, UnsupportedKind};
pub use platform::{
    AgentSender, BuiltSpace, Message, MessageDirection, PlatformMessageRecord, PlatformRuntime,
    Space, SpaceRef, User, wrap_provider_message,
};
pub use spectrum::{Spectrum, SpectrumInstance, SpectrumOptions};
pub use store::{Store, create_store};
pub use stream::{Broadcaster, ManagedStream, broadcast, merge_streams, stream};
pub use utils::{IdentifierKind, classify_identifier, from_vcard, to_vcard};
