use std::collections::BTreeMap;

use base64::Engine;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

pub const DEFAULT_SPECTRUM_CLOUD_URL: &str = "https://spectrum.photon.codes";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    Active,
    Canceled,
    PastDue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionData {
    pub status: Option<SubscriptionStatus>,
    pub tier: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedTokenData {
    #[serde(rename = "expiresIn")]
    pub expires_in: u64,
    pub token: String,
    #[serde(rename = "type")]
    pub token_type: SharedTokenType,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedTokenType {
    Shared,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DedicatedTokenData {
    pub auth: BTreeMap<String, String>,
    #[serde(rename = "expiresIn")]
    pub expires_in: u64,
    pub numbers: BTreeMap<String, Option<String>>,
    #[serde(rename = "type")]
    pub token_type: DedicatedTokenType,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DedicatedTokenType {
    Dedicated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TokenData {
    Shared {
        #[serde(rename = "expiresIn")]
        expires_in: u64,
        token: String,
    },
    Dedicated {
        auth: BTreeMap<String, String>,
        #[serde(rename = "expiresIn")]
        expires_in: u64,
        numbers: BTreeMap<String, Option<String>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudPlatform {
    Imessage,
    WhatsappBusiness,
    Slack,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformStatus {
    pub enabled: bool,
}

pub type PlatformsData = BTreeMap<CloudPlatform, PlatformStatus>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImessageInfoData {
    #[serde(rename = "type")]
    pub info_type: ImessageInfoType,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImessageInfoType {
    Shared,
    Dedicated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhatsappBusinessTokenData {
    pub auth: BTreeMap<String, String>,
    #[serde(rename = "expiresIn")]
    pub expires_in: u64,
    pub numbers: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackTeamMeta {
    #[serde(rename = "appId")]
    pub app_id: String,
    #[serde(rename = "botUserId")]
    pub bot_user_id: String,
    #[serde(rename = "grantedScopes")]
    pub granted_scopes: Vec<String>,
    #[serde(rename = "teamName")]
    pub team_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlackTokenData {
    pub auth: BTreeMap<String, String>,
    #[serde(rename = "expiresIn")]
    pub expires_in: u64,
    pub teams: BTreeMap<String, SlackTeamMeta>,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct SpectrumCloudError {
    pub status: u16,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
struct SuccessResponse<T> {
    data: T,
    succeed: bool,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[derive(Clone)]
pub struct CloudClient {
    base_url: String,
    http: reqwest::Client,
}

pub fn cloud() -> CloudClient {
    CloudClient::from_env()
}

impl Default for CloudClient {
    fn default() -> Self {
        Self::from_env()
    }
}

impl CloudClient {
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("SPECTRUM_CLOUD_URL")
                .unwrap_or_else(|_| DEFAULT_SPECTRUM_CLOUD_URL.to_string()),
        )
    }

    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn get_subscription(
        &self,
        project_id: &str,
    ) -> Result<SubscriptionData, SpectrumCloudError> {
        self.get(&format!("/projects/{project_id}/billing/subscription"))
            .await
    }

    pub async fn issue_imessage_tokens(
        &self,
        project_id: &str,
        project_secret: &str,
    ) -> Result<TokenData, SpectrumCloudError> {
        self.post_auth(
            &format!("/projects/{project_id}/imessage/tokens"),
            project_id,
            project_secret,
        )
        .await
    }

    pub async fn get_imessage_info(
        &self,
        project_id: &str,
    ) -> Result<ImessageInfoData, SpectrumCloudError> {
        self.get(&format!("/projects/{project_id}/imessage/")).await
    }

    pub async fn issue_whatsapp_business_tokens(
        &self,
        project_id: &str,
        project_secret: &str,
    ) -> Result<WhatsappBusinessTokenData, SpectrumCloudError> {
        self.post_auth(
            &format!("/projects/{project_id}/whatsapp-business/tokens"),
            project_id,
            project_secret,
        )
        .await
    }

    pub async fn issue_slack_tokens(
        &self,
        project_id: &str,
        project_secret: &str,
    ) -> Result<SlackTokenData, SpectrumCloudError> {
        self.post_auth(
            &format!("/projects/{project_id}/slack/tokens"),
            project_id,
            project_secret,
        )
        .await
    }

    pub async fn get_platforms(
        &self,
        project_id: &str,
    ) -> Result<PlatformsData, SpectrumCloudError> {
        self.get(&format!("/projects/{project_id}/platforms/"))
            .await
    }

    pub async fn toggle_platform(
        &self,
        project_id: &str,
        project_secret: &str,
        platform: CloudPlatform,
        enabled: bool,
    ) -> Result<PlatformsData, SpectrumCloudError> {
        #[derive(Serialize)]
        struct Body {
            platform: CloudPlatform,
            enabled: bool,
        }

        self.request_json(
            self.http
                .patch(self.url(&format!("/projects/{project_id}/platforms/")))
                .header("Authorization", basic_auth(project_id, project_secret))
                .json(&Body { platform, enabled }),
        )
        .await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, SpectrumCloudError> {
        self.request_json(self.http.get(self.url(path))).await
    }

    async fn post_auth<T: DeserializeOwned>(
        &self,
        path: &str,
        project_id: &str,
        project_secret: &str,
    ) -> Result<T, SpectrumCloudError> {
        self.request_json(
            self.http
                .post(self.url(path))
                .header("Authorization", basic_auth(project_id, project_secret)),
        )
        .await
    }

    async fn request_json<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, SpectrumCloudError> {
        let response = request
            .send()
            .await
            .map_err(|err| SpectrumCloudError::new(0, "REQUEST", err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(parse_error_response(status, response).await);
        }
        let json = response
            .json::<SuccessResponse<T>>()
            .await
            .map_err(|err| SpectrumCloudError::new(status.as_u16(), "DECODE", err.to_string()))?;
        if !json.succeed {
            return Err(SpectrumCloudError::new(
                status.as_u16(),
                "UNKNOWN",
                "Server returned succeed=false",
            ));
        }
        Ok(json.data)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

async fn parse_error_response(
    status: StatusCode,
    response: reqwest::Response,
) -> SpectrumCloudError {
    let body = response.text().await.unwrap_or_default();
    match serde_json::from_str::<ErrorBody>(&body) {
        Ok(parsed) => SpectrumCloudError::new(status.as_u16(), parsed.code, parsed.message),
        Err(_) => SpectrumCloudError::new(
            status.as_u16(),
            "UNKNOWN",
            if body.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("HTTP error")
                    .to_string()
            } else {
                body
            },
        ),
    }
}

fn basic_auth(project_id: &str, project_secret: &str) -> String {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{project_id}:{project_secret}"));
    format!("Basic {encoded}")
}

impl SpectrumCloudError {
    fn new(status: u16, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_matches_cloud_contract() {
        assert_eq!(
            basic_auth("project", "secret"),
            "Basic cHJvamVjdDpzZWNyZXQ="
        );
    }

    #[test]
    fn token_data_uses_type_discriminator() {
        let token: TokenData =
            serde_json::from_str(r#"{"type":"shared","expiresIn":3600,"token":"abc"}"#).unwrap();
        assert_eq!(
            token,
            TokenData::Shared {
                expires_in: 3600,
                token: "abc".to_string()
            }
        );
    }

    #[test]
    fn cloud_platform_serializes_as_api_value() {
        assert_eq!(
            serde_json::to_string(&CloudPlatform::WhatsappBusiness).unwrap(),
            "\"whatsapp_business\""
        );
    }
}
