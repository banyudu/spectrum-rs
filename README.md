# spectrum-rs

Rust port of `spectrum-ts`, a unified messaging SDK for agents.

This crate currently contains the core Spectrum model:

- content builders for text, attachments, contacts, reactions, replies, edits, groups, polls, typing, rename, avatar, voice, and rich links
- message, space, user, and provider-facing records
- in-memory store and identifier helpers
- vCard import/export helpers
- stream fanout helpers
- trait-backed Slack, WhatsApp Business, terminal, and iMessage provider slices
- iMessage remote send/inbound/event/runtime helpers
- iMessage local-mode helpers: AppleScript text/attachment sender plus a typed polling interface for chat.db-backed rows

The local iMessage poller is intentionally split at the storage boundary: apps can provide a `LocalImessageApi` implementation backed by chat.db, while the crate handles row-to-Spectrum conversion, polling, local sends, and runtime integration.
