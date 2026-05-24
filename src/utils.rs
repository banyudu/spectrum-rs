use base64::Engine;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::content::{
    Contact, ContactAddress, ContactEmail, ContactName, ContactOrg, ContactPhone, ContactPhoto,
    ContactPointType,
};
use crate::error::{Result, SpectrumError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentifierKind {
    Phone,
    Email,
    Unknown,
}

static PHONE_LIKE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^\+?[\d\s()\-.]{7,}$").unwrap());
static EMAIL_LIKE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[^\s@]+@[^\s@]+\.[^\s@]+$").unwrap());

pub fn classify_identifier(s: &str) -> (IdentifierKind, String) {
    if EMAIL_LIKE.is_match(s) {
        return (IdentifierKind::Email, s.trim().to_ascii_lowercase());
    }
    if PHONE_LIKE.is_match(s) && s.chars().filter(|c| c.is_ascii_digit()).count() >= 7 {
        let mut out = String::new();
        if s.trim_start().starts_with('+') {
            out.push('+');
        }
        out.extend(s.chars().filter(|c| c.is_ascii_digit()));
        return (IdentifierKind::Phone, out);
    }
    (IdentifierKind::Unknown, s.to_string())
}

#[derive(Debug, Clone)]
struct VCardProperty {
    name: String,
    value: String,
    types: Vec<String>,
}

pub fn from_vcard(input: &str) -> Result<Contact> {
    let props = parse_vcard(input)?;
    let mut contact = Contact {
        user: None,
        name: extract_name(&props),
        phones: extract_phones(&props),
        emails: extract_emails(&props),
        addresses: extract_addresses(&props),
        org: extract_org(&props),
        urls: props_named(&props, "URL")
            .into_iter()
            .map(|p| p.value.clone())
            .collect(),
        birthday: prop_value(&props, "BDAY"),
        note: prop_value(&props, "NOTE"),
        photo: extract_photo(&props),
        raw: Some(input.to_string()),
    };
    if contact.urls.is_empty() {
        contact.urls = Vec::new();
    }
    Ok(contact)
}

pub fn to_vcard(contact: &Contact) -> String {
    if let Some(raw) = &contact.raw
        && raw.starts_with("BEGIN:VCARD")
    {
        return raw.clone();
    }
    let mut lines = vec!["BEGIN:VCARD".to_string(), "VERSION:3.0".to_string()];
    write_name(&mut lines, contact.name.as_ref());
    for phone in &contact.phones {
        lines.push(format!(
            "TEL{}:{}",
            type_param(phone_type_param(phone.phone_type.as_ref())),
            escape_value(&phone.value)
        ));
    }
    for email in &contact.emails {
        lines.push(format!(
            "EMAIL{}:{}",
            type_param(simple_type_param(email.email_type.as_ref())),
            escape_value(&email.value)
        ));
    }
    for address in &contact.addresses {
        let value = format!(
            ";;{};{};{};{};{}",
            address.street.as_deref().unwrap_or_default(),
            address.city.as_deref().unwrap_or_default(),
            address.region.as_deref().unwrap_or_default(),
            address.postal_code.as_deref().unwrap_or_default(),
            address.country.as_deref().unwrap_or_default()
        );
        lines.push(format!(
            "ADR{}:{}",
            type_param(simple_type_param(address.address_type.as_ref())),
            escape_value(&value)
        ));
    }
    if let Some(org) = &contact.org {
        if org.name.is_some() || org.department.is_some() {
            lines.push(format!(
                "ORG:{};{}",
                escape_value(org.name.as_deref().unwrap_or_default()),
                escape_value(org.department.as_deref().unwrap_or_default())
            ));
        }
        if let Some(title) = &org.title {
            lines.push(format!("TITLE:{}", escape_value(title)));
        }
    }
    for url in &contact.urls {
        lines.push(format!("URL:{}", escape_value(url)));
    }
    if let Some(birthday) = &contact.birthday {
        lines.push(format!("BDAY:{}", escape_value(birthday)));
    }
    if let Some(note) = &contact.note {
        lines.push(format!("NOTE:{}", escape_value(note)));
    }
    if let Some(photo) = &contact.photo {
        let subtype = photo
            .mime_type
            .split('/')
            .nth(1)
            .unwrap_or("jpeg")
            .to_ascii_uppercase();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&photo.data);
        lines.push(format!("PHOTO;ENCODING=b;TYPE={subtype}:{encoded}"));
    }
    lines.push("END:VCARD".to_string());
    lines.join("\r\n")
}

fn parse_vcard(input: &str) -> Result<Vec<VCardProperty>> {
    let normalized = input
        .trim_start_matches('\u{feff}')
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    if !normalized
        .lines()
        .any(|line| line.eq_ignore_ascii_case("BEGIN:VCARD"))
    {
        return Err(SpectrumError::msg("Invalid vCard: no cards parsed"));
    }
    let mut unfolded: Vec<String> = Vec::new();
    for line in normalized.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(last) = unfolded.last_mut() {
                last.push_str(line.trim_start());
            }
        } else {
            unfolded.push(line.to_string());
        }
    }
    let mut props = Vec::new();
    for line in unfolded {
        let Some((head, value)) = line.split_once(':') else {
            continue;
        };
        let mut head_parts = head.split(';');
        let name = head_parts.next().unwrap_or_default().to_ascii_uppercase();
        if matches!(name.as_str(), "BEGIN" | "END" | "VERSION") {
            continue;
        }
        let mut types = Vec::new();
        for param in head_parts {
            let value = param
                .strip_prefix("TYPE=")
                .or_else(|| param.strip_prefix("type="))
                .unwrap_or(param);
            types.extend(value.split(',').map(|s| s.to_ascii_lowercase()));
        }
        props.push(VCardProperty {
            name,
            value: unescape_value(value.trim()),
            types,
        });
    }
    Ok(props)
}

fn props_named<'a>(props: &'a [VCardProperty], name: &str) -> Vec<&'a VCardProperty> {
    props.iter().filter(|prop| prop.name == name).collect()
}

fn prop_value(props: &[VCardProperty], name: &str) -> Option<String> {
    props_named(props, name)
        .first()
        .map(|p| p.value.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn extract_name(props: &[VCardProperty]) -> Option<ContactName> {
    let formatted = prop_value(props, "FN");
    let structured = prop_value(props, "N");
    if formatted.is_none() && structured.is_none() {
        return None;
    }
    let mut name = ContactName {
        formatted,
        ..ContactName::default()
    };
    if let Some(value) = structured {
        let mut parts = value.split(';').map(str::trim);
        name.last = non_empty(parts.next());
        name.first = non_empty(parts.next());
        name.middle = non_empty(parts.next());
        name.prefix = non_empty(parts.next());
        name.suffix = non_empty(parts.next());
    }
    Some(name)
}

fn extract_phones(props: &[VCardProperty]) -> Vec<ContactPhone> {
    props_named(props, "TEL")
        .into_iter()
        .map(|prop| ContactPhone {
            value: prop.value.trim().to_string(),
            phone_type: phone_type(prop),
        })
        .collect()
}

fn extract_emails(props: &[VCardProperty]) -> Vec<ContactEmail> {
    props_named(props, "EMAIL")
        .into_iter()
        .map(|prop| ContactEmail {
            value: prop.value.trim().to_string(),
            email_type: simple_type(prop),
        })
        .collect()
}

fn extract_addresses(props: &[VCardProperty]) -> Vec<ContactAddress> {
    props_named(props, "ADR")
        .into_iter()
        .map(|prop| {
            let parts: Vec<_> = prop.value.split(';').map(str::trim).collect();
            ContactAddress {
                street: non_empty(parts.get(2).copied()),
                city: non_empty(parts.get(3).copied()),
                region: non_empty(parts.get(4).copied()),
                postal_code: non_empty(parts.get(5).copied()),
                country: non_empty(parts.get(6).copied()),
                address_type: simple_type(prop),
            }
        })
        .collect()
}

fn extract_org(props: &[VCardProperty]) -> Option<ContactOrg> {
    let org = prop_value(props, "ORG");
    let title = prop_value(props, "TITLE");
    if org.is_none() && title.is_none() {
        return None;
    }
    let mut out = ContactOrg {
        title,
        ..ContactOrg::default()
    };
    if let Some(org) = org {
        let mut parts = org.split(';').map(str::trim);
        out.name = non_empty(parts.next());
        out.department = non_empty(parts.next());
    }
    Some(out)
}

fn extract_photo(props: &[VCardProperty]) -> Option<ContactPhoto> {
    let prop = props_named(props, "PHOTO").into_iter().next()?;
    if let Some((mime_type, base64)) = prop
        .value
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(";base64,"))
    {
        let data = base64::engine::general_purpose::STANDARD
            .decode(base64)
            .ok()?;
        return Some(ContactPhoto {
            mime_type: mime_type.to_string(),
            data: data.into(),
        });
    }
    let photo_type = prop.types.first().map(String::as_str).unwrap_or("jpeg");
    let mime_type = if photo_type.starts_with("image/") {
        photo_type.to_string()
    } else {
        format!("image/{photo_type}")
    };
    let data = base64::engine::general_purpose::STANDARD
        .decode(&prop.value)
        .ok()?;
    Some(ContactPhoto {
        mime_type,
        data: data.into(),
    })
}

fn phone_type(prop: &VCardProperty) -> Option<ContactPointType> {
    if prop
        .types
        .iter()
        .any(|t| matches!(t.as_str(), "cell" | "mobile" | "iphone"))
    {
        return Some(ContactPointType::Mobile);
    }
    simple_type(prop)
}

fn simple_type(prop: &VCardProperty) -> Option<ContactPointType> {
    if prop.types.iter().any(|t| t == "home") {
        Some(ContactPointType::Home)
    } else if prop.types.iter().any(|t| t == "work") {
        Some(ContactPointType::Work)
    } else if prop.types.is_empty() {
        None
    } else {
        Some(ContactPointType::Other)
    }
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
}

fn phone_type_param(value: Option<&ContactPointType>) -> Option<&'static str> {
    match value {
        Some(ContactPointType::Mobile) => Some("CELL"),
        Some(ContactPointType::Home) => Some("HOME"),
        Some(ContactPointType::Work) => Some("WORK"),
        Some(ContactPointType::Other) => Some("OTHER"),
        None => None,
    }
}

fn simple_type_param(value: Option<&ContactPointType>) -> Option<&'static str> {
    match value {
        Some(ContactPointType::Home) => Some("HOME"),
        Some(ContactPointType::Work) => Some("WORK"),
        Some(ContactPointType::Other) => Some("OTHER"),
        Some(ContactPointType::Mobile) => Some("CELL"),
        None => None,
    }
}

fn type_param(value: Option<&str>) -> String {
    value.map(|v| format!(";TYPE={v}")).unwrap_or_default()
}

fn write_name(lines: &mut Vec<String>, name: Option<&ContactName>) {
    let formatted = name
        .and_then(|n| n.formatted.clone())
        .or_else(|| {
            let name = name?;
            let parts = [
                name.first.as_deref(),
                name.middle.as_deref(),
                name.last.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join(" "))
        })
        .unwrap_or_else(|| "Unknown".to_string());
    lines.push(format!("FN:{}", escape_value(&formatted)));
    if let Some(name) = name
        && (name.first.is_some()
            || name.last.is_some()
            || name.middle.is_some()
            || name.prefix.is_some()
            || name.suffix.is_some())
    {
        lines.push(format!(
            "N:{};{};{};{};{}",
            escape_value(name.last.as_deref().unwrap_or_default()),
            escape_value(name.first.as_deref().unwrap_or_default()),
            escape_value(name.middle.as_deref().unwrap_or_default()),
            escape_value(name.prefix.as_deref().unwrap_or_default()),
            escape_value(name.suffix.as_deref().unwrap_or_default())
        ));
    }
}

fn escape_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace(',', "\\,")
}

fn unescape_value(value: &str) -> String {
    value
        .replace("\\n", "\n")
        .replace("\\,", ",")
        .replace("\\\\", "\\")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_identifiers() {
        assert_eq!(
            classify_identifier("User@Example.COM"),
            (IdentifierKind::Email, "user@example.com".to_string())
        );
        assert_eq!(
            classify_identifier("+1 (555) 123-4567"),
            (IdentifierKind::Phone, "+15551234567".to_string())
        );
        assert_eq!(
            classify_identifier("not a handle"),
            (IdentifierKind::Unknown, "not a handle".to_string())
        );
    }

    #[test]
    fn parses_vcard_contact_fields() {
        let vcf = "BEGIN:VCARD\nVERSION:3.0\nFN:Ada Lovelace\nN:Lovelace;Ada;;;\nTEL;TYPE=CELL:+15551234567\nEMAIL;TYPE=WORK:ada@example.com\nORG:Analytical Engines;Research\nTITLE:Countess\nURL:https://example.com\nEND:VCARD";
        let contact = from_vcard(vcf).unwrap();
        assert_eq!(contact.name.unwrap().first.as_deref(), Some("Ada"));
        assert_eq!(contact.phones[0].phone_type, Some(ContactPointType::Mobile));
        assert_eq!(contact.emails[0].email_type, Some(ContactPointType::Work));
        assert_eq!(contact.org.unwrap().department.as_deref(), Some("Research"));
        assert_eq!(contact.urls, vec!["https://example.com"]);
    }
}
