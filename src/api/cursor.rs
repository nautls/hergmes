//! The cursors: where the next page starts.
//!
//! A box page's cursor ([`Cursor`]) is pinned to the first page's block. Opaque to clients:
//! base64url without padding of 81 bytes, so 108 characters. A version byte, the pinned block
//! (height and header id), the last box returned (inclusion height and box id), and an 8-byte
//! fingerprint of the key. Integers are big-endian.
//!
//! A history page's cursor ([`TxCursor`]) is anchored on a tx instead: 49 bytes, so 66
//! characters. A version byte, the fingerprint, the anchor's tx id, its position from the
//! oldest end and the list's length when it was read. Its length differs from a box cursor's,
//! so a cursor sent to the other route is `invalid_cursor`.
//!
//! The fingerprint turns a cursor sent to another key into `invalid_cursor`. Without it the
//! pin would be valid and paging would resume after the other key's last box, so the client
//! would silently miss boxes. It is not a signature: a forged cursor misleads only its sender.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use blake2::{Blake2b256, Digest as _};

use crate::{
    engine::{BlockRef, BoxKey, TrackingKey},
    types::{Digest, ergo::TxId},
};

/// Bumped when the layout changes, so an old cursor is `invalid_cursor` rather than misread.
pub const VERSION: u8 = 1;

const LEN: usize = 1 + 4 + 32 + 4 + 32 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    /// The block every page of the snapshot is as of.
    pub at: BlockRef,
    /// The last box of the page before.
    pub after: BoxKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    #[error("not a cursor")]
    Malformed,
    #[error("unknown cursor version {0}")]
    Version(u8),
    #[error("the cursor is for another key")]
    OtherKey,
}

impl Cursor {
    pub fn encode(&self, key: &TrackingKey) -> String {
        let mut bytes = Vec::with_capacity(LEN);
        bytes.push(VERSION);
        bytes.extend_from_slice(&self.at.height.to_be_bytes());
        bytes.extend_from_slice(&self.at.header_id.0);
        bytes.extend_from_slice(&self.after.inclusion_height.to_be_bytes());
        bytes.extend_from_slice(&self.after.box_id.0);
        bytes.extend_from_slice(&fingerprint(key));
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Decodes a cursor for `key`.
    pub fn decode(encoded: &str, key: &TrackingKey) -> Result<Self, CursorError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| CursorError::Malformed)?;
        let version = *bytes.first().ok_or(CursorError::Malformed)?;
        if version != VERSION {
            return Err(CursorError::Version(version));
        }
        let bytes: &[u8; LEN] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| CursorError::Malformed)?;
        let (_, rest) = bytes.split_at(1);
        let (height, rest) = rest.split_at(4);
        let (header_id, rest) = rest.split_at(32);
        let (inclusion_height, rest) = rest.split_at(4);
        let (box_id, print) = rest.split_at(32);
        if print != fingerprint(key) {
            return Err(CursorError::OtherKey);
        }
        Ok(Self {
            at: BlockRef { height: u32_at(height), header_id: Digest(array(header_id)) },
            after: BoxKey {
                inclusion_height: u32_at(inclusion_height),
                box_id: Digest(array(box_id)),
            },
        })
    }
}

/// Bumped when the history cursor's layout changes. Independent of [`VERSION`]: the lengths
/// already tell the two kinds apart.
pub const TX_VERSION: u8 = 1;

const TX_LEN: usize = 1 + 8 + 32 + 4 + 4;

/// Where a history's next page starts: below `anchor`, the oldest tx of the page before.
///
/// An address's tx list only grows at its end and loses its end to a rollback, so a tx's
/// position counted from the oldest end holds until a reorg reaches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxCursor {
    pub anchor: TxId,
    /// The anchor's position from the oldest end, from 0.
    pub pos: u32,
    /// The list's length when the page was read.
    pub total: u32,
}

impl TxCursor {
    pub fn encode(&self, key: &TrackingKey) -> String {
        let mut bytes = Vec::with_capacity(TX_LEN);
        bytes.push(TX_VERSION);
        bytes.extend_from_slice(&fingerprint(key));
        bytes.extend_from_slice(&self.anchor.0);
        bytes.extend_from_slice(&self.pos.to_be_bytes());
        bytes.extend_from_slice(&self.total.to_be_bytes());
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Decodes a cursor for `key`. One whose anchor is not inside its list was not made by
    /// the relay, and is `Malformed`.
    pub fn decode(encoded: &str, key: &TrackingKey) -> Result<Self, CursorError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| CursorError::Malformed)?;
        let version = *bytes.first().ok_or(CursorError::Malformed)?;
        if version != TX_VERSION {
            return Err(CursorError::Version(version));
        }
        let bytes: &[u8; TX_LEN] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| CursorError::Malformed)?;
        let (_, rest) = bytes.split_at(1);
        let (print, rest) = rest.split_at(8);
        let (anchor, rest) = rest.split_at(32);
        let (pos, total) = rest.split_at(4);
        if print != fingerprint(key) {
            return Err(CursorError::OtherKey);
        }
        let cursor = Self { anchor: Digest(array(anchor)), pos: u32_at(pos), total: u32_at(total) };
        if cursor.pos >= cursor.total {
            return Err(CursorError::Malformed);
        }
        Ok(cursor)
    }
}

/// The first 8 bytes of a hash of the key, its kind included: a token id and a 32-byte tree
/// with the same bytes are different keys, and so are a token id and a 32-byte template.
fn fingerprint(key: &TrackingKey) -> [u8; 8] {
    let mut hasher = Blake2b256::new();
    match key {
        TrackingKey::Tree(tree) => {
            hasher.update([0]);
            hasher.update(&tree.0);
        }
        TrackingKey::Token(id) => {
            hasher.update([1]);
            hasher.update(id.0);
        }
        TrackingKey::Template(template) => {
            hasher.update([2]);
            hasher.update(&template.0);
        }
    }
    array(&hasher.finalize()[..8])
}

fn u32_at(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(array(bytes))
}

fn array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    bytes.try_into().expect("split at the right length")
}

#[cfg(test)]
#[path = "cursor_tests.rs"]
mod tests;
