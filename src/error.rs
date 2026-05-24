use thiserror::Error;

pub type Result<T> = std::result::Result<T, SpectrumError>;

#[derive(Debug, Error)]
pub enum SpectrumError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Unsupported(Box<UnsupportedError>),
}

impl SpectrumError {
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

impl From<UnsupportedError> for SpectrumError {
    fn from(value: UnsupportedError) -> Self {
        Self::Unsupported(Box::new(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedKind {
    Content,
    Action,
}

#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct UnsupportedError {
    pub kind: UnsupportedKind,
    pub platform: Option<String>,
    pub content_type: Option<String>,
    pub action: Option<String>,
    pub detail: Option<String>,
    message: String,
}

impl UnsupportedError {
    pub fn content(
        content_type: impl Into<String>,
        platform: Option<String>,
        detail: Option<String>,
    ) -> Self {
        let content_type = content_type.into();
        Self::new(
            UnsupportedKind::Content,
            platform,
            Some(content_type),
            None,
            detail,
        )
    }

    pub fn action(
        action: impl Into<String>,
        platform: Option<String>,
        detail: Option<String>,
    ) -> Self {
        let action = action.into();
        Self::new(
            UnsupportedKind::Action,
            platform,
            None,
            Some(action),
            detail,
        )
    }

    pub fn with_platform(&self, platform: impl Into<String>) -> Self {
        if self.platform.is_some() {
            return self.clone();
        }
        Self::new(
            self.kind,
            Some(platform.into()),
            self.content_type.clone(),
            self.action.clone(),
            self.detail.clone(),
        )
    }

    fn new(
        kind: UnsupportedKind,
        platform: Option<String>,
        content_type: Option<String>,
        action: Option<String>,
        detail: Option<String>,
    ) -> Self {
        let platform_label = platform.as_deref().unwrap_or("platform").to_string();
        let subject = match kind {
            UnsupportedKind::Content => {
                format!(
                    "content type \"{}\"",
                    content_type.as_deref().unwrap_or("unknown")
                )
            }
            UnsupportedKind::Action => {
                format!("action \"{}\"", action.as_deref().unwrap_or("unknown"))
            }
        };
        let suffix = detail
            .as_ref()
            .map(|detail| format!(": {detail}"))
            .unwrap_or_default();
        Self {
            kind,
            platform,
            content_type,
            action,
            detail,
            message: format!("{platform_label} does not support {subject}{suffix}"),
        }
    }
}
