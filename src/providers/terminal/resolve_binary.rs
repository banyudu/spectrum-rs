use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::error::{Result, SpectrumError};

pub const DEFAULT_TUICHAT_VERSION: &str = "0.1.4";
const REPO: &str = "photon-hq/tuichat";
const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone, Debug, Default)]
pub struct ResolveTuichatOptions {
    pub force: bool,
    pub version: Option<String>,
}

pub async fn resolve_tuichat_binary(options: ResolveTuichatOptions) -> Result<PathBuf> {
    if let Ok(override_path) = env::var("TUICHAT_BINARY") {
        let path = PathBuf::from(override_path);
        if !path.exists() {
            return Err(SpectrumError::msg(format!(
                "tuichat: TUICHAT_BINARY={} does not exist",
                path.display()
            )));
        }
        return Ok(path);
    }

    let version = match options.version {
        Some(version) => version,
        None => env::var("TUICHAT_VERSION").unwrap_or_else(|_| DEFAULT_TUICHAT_VERSION.to_string()),
    };
    validate_version(&version)?;
    let target = target_suffix()?;
    let ext = if target.starts_with("windows") {
        ".exe"
    } else {
        ""
    };
    let filename = format!("tuichat-{target}{ext}");
    let dir = cache_dir_for(&version)?;
    let path = dir.join(&filename);

    if !options.force && path.exists() {
        return Ok(path);
    }

    let bytes = download_verified(&version, &filename).await?;
    fs::create_dir_all(&dir).await?;
    write_binary(&path, &bytes).await?;
    Ok(path)
}

pub fn target_suffix() -> Result<&'static str> {
    let os = env::consts::OS;
    let arch = env::consts::ARCH;
    match (os, arch) {
        ("macos", "aarch64") => Ok("darwin-arm64"),
        ("macos", "x86_64") => Ok("darwin-x64"),
        ("linux", "x86_64") => Ok("linux-x64"),
        ("linux", "aarch64") => Ok("linux-arm64"),
        ("windows", "x86_64") => Ok("windows-x64"),
        _ => Err(SpectrumError::msg(format!(
            "tuichat: unsupported platform/arch: {os}-{arch}"
        ))),
    }
}

pub fn cache_dir_for(version: &str) -> Result<PathBuf> {
    validate_version(version)?;
    let home = env::var("HOME").map(PathBuf::from).map_err(|_| {
        SpectrumError::msg("tuichat: HOME is not set and cache directory cannot be resolved")
    })?;
    let path = match env::consts::OS {
        "windows" => env::var("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join("AppData").join("Local"))
            .join("tuichat")
            .join(format!("v{version}")),
        "macos" => home
            .join("Library")
            .join("Caches")
            .join("tuichat")
            .join(format!("v{version}")),
        _ => env::var("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join(".cache"))
            .join("tuichat")
            .join(format!("v{version}")),
    };
    Ok(path)
}

pub fn parse_checksums(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(hash) = parts.next() else {
            continue;
        };
        let Some(name) = parts.next() else {
            continue;
        };
        if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
            out.insert(
                name.trim_start_matches('*').to_string(),
                hash.to_ascii_lowercase(),
            );
        }
    }
    out
}

fn validate_version(version: &str) -> Result<()> {
    let mut split = version.splitn(2, ['-', '+']);
    let core = split.next().unwrap_or_default();
    let core_parts: Vec<_> = core.split('.').collect();
    let valid_core = core_parts.len() == 3
        && core_parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()));
    let valid_suffix = split.next().is_none_or(|suffix| {
        !suffix.is_empty()
            && suffix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
    });
    if valid_core && valid_suffix {
        Ok(())
    } else {
        Err(SpectrumError::msg(format!(
            "tuichat: invalid version \"{version}\" - expected semver like 0.1.4"
        )))
    }
}

async fn download_verified(version: &str, filename: &str) -> Result<Vec<u8>> {
    let base = format!("https://github.com/{REPO}/releases/download/v{version}");
    let client = reqwest::Client::new();
    let sums = tokio::time::timeout(
        DOWNLOAD_TIMEOUT,
        client.get(format!("{base}/SHA256SUMS")).send(),
    )
    .await
    .map_err(|_| {
        SpectrumError::msg(format!(
            "tuichat: timed out fetching v{version} release assets after {}ms",
            DOWNLOAD_TIMEOUT.as_millis()
        ))
    })?
    .map_err(|err| SpectrumError::msg(err.to_string()))?;
    let bin = tokio::time::timeout(
        DOWNLOAD_TIMEOUT,
        client.get(format!("{base}/{filename}")).send(),
    )
    .await
    .map_err(|_| {
        SpectrumError::msg(format!(
            "tuichat: timed out fetching v{version} release assets after {}ms",
            DOWNLOAD_TIMEOUT.as_millis()
        ))
    })?
    .map_err(|err| SpectrumError::msg(err.to_string()))?;

    if !sums.status().is_success() {
        return Err(SpectrumError::msg(format!(
            "tuichat: failed to fetch SHA256SUMS (v{version}): HTTP {}",
            sums.status().as_u16()
        )));
    }
    if !bin.status().is_success() {
        return Err(SpectrumError::msg(format!(
            "tuichat: failed to fetch {filename} (v{version}): HTTP {}",
            bin.status().as_u16()
        )));
    }

    let checksums = parse_checksums(
        &sums
            .text()
            .await
            .map_err(|err| SpectrumError::msg(err.to_string()))?,
    );
    let expected = checksums.get(filename).ok_or_else(|| {
        SpectrumError::msg(format!(
            "tuichat: no checksum for {filename} in SHA256SUMS (v{version})"
        ))
    })?;
    let bytes = bin
        .bytes()
        .await
        .map_err(|err| SpectrumError::msg(err.to_string()))?
        .to_vec();
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if &actual != expected {
        return Err(SpectrumError::msg(format!(
            "tuichat: checksum mismatch for {filename} (expected {expected}, got {actual})"
        )));
    }
    Ok(bytes)
}

async fn write_binary(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = fs::File::create(&tmp).await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    drop(file);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).await?;
    }
    fs::rename(&tmp, path).await.inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sha256_sums() {
        let parsed = parse_checksums(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef  *tuichat-linux-x64\nbad line",
        );
        assert_eq!(
            parsed.get("tuichat-linux-x64").map(String::as_str),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
    }

    #[test]
    fn rejects_path_like_versions() {
        assert!(cache_dir_for("../../evil").is_err());
        assert!(cache_dir_for("0.1.4").is_ok());
        assert!(cache_dir_for("0.1.4-beta.1").is_ok());
    }
}
