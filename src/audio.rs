use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use once_cell::sync::Lazy;
use regex::Regex;
use tokio::process::Command;

use crate::error::{Result, SpectrumError};

const M4A_BRANDS: [&[u8; 4]; 7] = [
    b"M4A ", b"M4B ", b"M4P ", b"mp42", b"mp41", b"isom", b"iso2",
];

const M4A_MIME_TYPES: [&str; 5] = [
    "audio/mp4",
    "audio/mp4a-latm",
    "audio/x-m4a",
    "audio/aac",
    "audio/aacp",
];

const FFMPEG_MISSING_MESSAGE: &str = "voice content: input is not m4a/aac and ffmpeg is unavailable. Install `ffmpeg-static` or ensure `ffmpeg` is on PATH.";

static FFMPEG_PATH: OnceLock<String> = OnceLock::new();
static DURATION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"Duration:\s*(\d+):(\d{2}):(\d{2})(?:\.(\d{1,3}))?").unwrap());

#[derive(Clone, Debug, PartialEq)]
pub struct M4aAudio {
    pub buffer: Bytes,
    pub duration: Option<f64>,
}

pub fn is_m4a(buffer: &[u8]) -> bool {
    if buffer.len() < 12 {
        return false;
    }
    if &buffer[4..8] != b"ftyp" {
        return false;
    }
    M4A_BRANDS.iter().any(|brand| &buffer[8..12] == *brand)
}

pub fn is_m4a_mime_type(mime_type: &str) -> bool {
    let normalized = mime_type.to_ascii_lowercase();
    M4A_MIME_TYPES
        .iter()
        .any(|candidate| *candidate == normalized)
}

pub fn resolve_ffmpeg_path() -> String {
    FFMPEG_PATH
        .get_or_init(|| std::env::var("FFMPEG_PATH").unwrap_or_else(|_| "ffmpeg".to_string()))
        .clone()
}

pub async fn ensure_m4a(buffer: impl Into<Bytes>, mime_type: &str) -> Result<M4aAudio> {
    let buffer = buffer.into();
    if is_m4a_mime_type(mime_type) || is_m4a(&buffer) {
        return Ok(M4aAudio {
            buffer,
            duration: None,
        });
    }
    transcode_to_m4a(buffer).await
}

async fn transcode_to_m4a(buffer: Bytes) -> Result<M4aAudio> {
    let ffmpeg = resolve_ffmpeg_path();
    let dir = create_temp_dir().await?;
    let in_path = dir.join("in");
    let out_path = dir.join("out.m4a");

    let result = async {
        tokio::fs::write(&in_path, &buffer).await?;
        let output = Command::new(&ffmpeg)
            .arg("-y")
            .arg("-i")
            .arg(&in_path)
            .arg("-f")
            .arg("ipod")
            .arg("-c:a")
            .arg("aac")
            .arg(&out_path)
            .output()
            .await
            .map_err(map_ffmpeg_spawn_error)?;

        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            return Err(SpectrumError::msg(format!(
                "ffmpeg conversion failed (exit {code}): {stderr}"
            )));
        }

        Ok(M4aAudio {
            buffer: Bytes::from(tokio::fs::read(&out_path).await?),
            duration: parse_duration(&stderr),
        })
    }
    .await;

    let cleanup = tokio::fs::remove_dir_all(&dir).await;
    if result.is_ok()
        && let Err(err) = cleanup
    {
        return Err(err.into());
    }
    result
}

fn map_ffmpeg_spawn_error(err: std::io::Error) -> SpectrumError {
    if err.kind() == std::io::ErrorKind::NotFound {
        SpectrumError::msg(FFMPEG_MISSING_MESSAGE)
    } else {
        err.into()
    }
}

async fn create_temp_dir() -> Result<PathBuf> {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    for attempt in 0..100_u32 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let dir = base.join(format!("spectrum-voice-{pid}-{nanos}-{attempt}"));
        match tokio::fs::create_dir(&dir).await {
            Ok(()) => return Ok(dir),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        }
    }
    Err(SpectrumError::msg(
        "failed to create temporary directory for voice conversion",
    ))
}

fn parse_duration(stderr: &str) -> Option<f64> {
    let captures = DURATION_PATTERN.captures(stderr)?;
    let hours: f64 = captures.get(1)?.as_str().parse().ok()?;
    let minutes: f64 = captures.get(2)?.as_str().parse().ok()?;
    let seconds: f64 = captures.get(3)?.as_str().parse().ok()?;
    let fraction = captures
        .get(4)
        .map(|value| format!("0.{}", value.as_str()).parse::<f64>().ok())
        .unwrap_or(Some(0.0))?;
    let duration = hours * 3600.0 + minutes * 60.0 + seconds + fraction;
    duration.is_finite().then_some(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m4a_with_brand(brand: &[u8; 4]) -> Vec<u8> {
        let mut bytes = b"\0\0\0\0ftyp".to_vec();
        bytes.extend_from_slice(brand);
        bytes
    }

    #[test]
    fn detects_supported_m4a_brands() {
        for brand in M4A_BRANDS {
            assert!(is_m4a(&m4a_with_brand(brand)));
        }
    }

    #[test]
    fn rejects_short_or_unknown_m4a_buffers() {
        assert!(!is_m4a(b"ftypM4A "));
        assert!(!is_m4a(b"\0\0\0\0moovM4A "));
        assert!(!is_m4a(b"\0\0\0\0ftypWAV "));
    }

    #[test]
    fn detects_m4a_mime_types_case_insensitively() {
        assert!(is_m4a_mime_type("audio/mp4"));
        assert!(is_m4a_mime_type("Audio/X-M4A"));
        assert!(!is_m4a_mime_type("audio/wav"));
    }

    #[tokio::test]
    async fn ensure_m4a_returns_mime_matches_without_conversion() {
        let buffer = Bytes::from_static(b"not actually m4a");
        let audio = ensure_m4a(buffer.clone(), "audio/aac").await.unwrap();
        assert_eq!(audio.buffer, buffer);
        assert_eq!(audio.duration, None);
    }

    #[tokio::test]
    async fn ensure_m4a_returns_signature_matches_without_conversion() {
        let buffer = Bytes::from(m4a_with_brand(b"isom"));
        let audio = ensure_m4a(buffer.clone(), "application/octet-stream")
            .await
            .unwrap();
        assert_eq!(audio.buffer, buffer);
        assert_eq!(audio.duration, None);
    }

    #[test]
    fn parses_ffmpeg_duration() {
        let duration =
            parse_duration("Input #0\n  Duration: 01:02:03.456, start: 0.000000, bitrate: 64 kb/s");
        assert_eq!(duration, Some(3723.456));
    }

    #[test]
    fn ignores_missing_ffmpeg_duration() {
        assert_eq!(parse_duration("no duration here"), None);
    }
}
