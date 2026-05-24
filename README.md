# spectrum-rs

Rust port of `spectrum-ts`, a unified messaging SDK for agents.

This crate currently contains the core Spectrum model:

- content builders for text, attachments, contacts, reactions, replies, edits, groups, polls, typing, rename, avatar, voice, and rich links
- message, space, user, and provider-facing records
- in-memory store and identifier helpers
- vCard import/export helpers
- stream fanout helpers

Provider integrations are being ported after the shared runtime surface is in place.
