//! What an EIP-4 minting box says beyond what the node's indexer reads: the asset type (R7)
//! and, for an NFT, its artwork (R8 and R9).
//!
//! The indexer reads only R4 to R6 into a token's name, description and decimals (node 6.0.8,
//! `IndexedToken.fromBox`), so the rest comes from the minting box itself. Read as Nautilus
//! does (nautilus-wallet, `graphQlService.parseEIP4Asset`): R7 as `Coll[Byte]`, and for an
//! artwork NFT R8 as the artwork's hash and R9 as a link, or a link and a cover as a pair. A
//! register of any other shape is as if absent.
//!
//! Unlike Nautilus, R8 and R9 are left unread for the NFTs whose R8 and R9 are not artwork
//! (EIP-4, "Asset types"): an artwork collection leaves them empty, and file attachments hold
//! a list of hashes and an EIP-29 attachment, an EIP since revoked.

use crate::types::{
    Digest, HashDigest,
    ergo::NonMandatoryRegisters,
    ergo_tree::{byte_coll, byte_coll_pair},
};

/// The asset types EIP-4 registers ("Asset types"), each R7's category and subcategory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetType {
    /// `01`, with a subcategory left out or not registered.
    Nft,
    /// `0101`
    NftPicture,
    /// `0102`
    NftAudio,
    /// `0103`
    NftVideo,
    /// `0104`
    NftArtworkCollection,
    /// `010f`
    NftFileAttachments,
    /// `0201`
    MembershipThresholdSignature,
}

impl AssetType {
    /// The type R7's bytes name, by their first two; `None` for a category EIP-4 does not
    /// register, so a client never reads a code it can't name.
    fn from_r7(r7: &[u8]) -> Option<Self> {
        Some(match r7 {
            [0x01, 0x01, ..] => Self::NftPicture,
            [0x01, 0x02, ..] => Self::NftAudio,
            [0x01, 0x03, ..] => Self::NftVideo,
            [0x01, 0x04, ..] => Self::NftArtworkCollection,
            [0x01, 0x0f, ..] => Self::NftFileAttachments,
            [0x01, ..] => Self::Nft,
            [0x02, 0x01, ..] => Self::MembershipThresholdSignature,
            _ => return None,
        })
    }

    /// Whether R8 and R9 are the artwork's hash and links: for picture, audio and video NFTs,
    /// and for an NFT of no registered subcategory, read as a picture.
    pub fn has_artwork(self) -> bool {
        matches!(self, Self::Nft | Self::NftPicture | Self::NftAudio | Self::NftVideo)
    }

    /// The name clients read.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nft => "nft",
            Self::NftPicture => "nft-picture",
            Self::NftAudio => "nft-audio",
            Self::NftVideo => "nft-video",
            Self::NftArtworkCollection => "nft-artwork-collection",
            Self::NftFileAttachments => "nft-file-attachments",
            Self::MembershipThresholdSignature => "membership-threshold-signature",
        }
    }
}

/// A minting box's EIP-4 asset type and artwork. Present only if R7 is a `Coll[Byte]` naming
/// a registered type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asset {
    pub asset_type: AssetType,
    /// R8, the SHA-256 of an artwork NFT's artwork. Dropped unless 32 bytes.
    pub artwork_hash: Option<HashDigest>,
    /// R9, or its first element, as UTF-8. Absent if empty.
    pub artwork_url: Option<Box<str>>,
    /// R9's second element, as UTF-8. Absent if empty.
    pub artwork_cover: Option<Box<str>>,
}

impl Asset {
    /// The asset a minting box's registers describe. `None` without a `Coll[Byte]` R7 naming
    /// a registered type.
    pub fn from_registers(registers: &NonMandatoryRegisters) -> Option<Self> {
        let asset_type = AssetType::from_r7(byte_coll(&registers.r7.as_ref()?.0)?)?;
        let mut asset =
            Self { asset_type, artwork_hash: None, artwork_url: None, artwork_cover: None };
        if !asset_type.has_artwork() {
            return Some(asset);
        }
        asset.artwork_hash = registers
            .r8
            .as_ref()
            .and_then(|r8| byte_coll(&r8.0))
            .and_then(|hash| Some(Digest(hash.try_into().ok()?)));
        if let Some(r9) = &registers.r9 {
            if let Some(url) = byte_coll(&r9.0) {
                asset.artwork_url = text(url);
            } else if let Some((url, cover)) = byte_coll_pair(&r9.0) {
                asset.artwork_url = text(url);
                asset.artwork_cover = text(cover);
            }
        }
        Some(asset)
    }

    /// The bytes this holds beyond its own size, for weighing a cache entry.
    pub fn heap_size(&self) -> usize {
        self.artwork_url.as_ref().map_or(0, |s| s.len())
            + self.artwork_cover.as_ref().map_or(0, |s| s.len())
    }
}

/// Text as the node reads R4 and R5 (`IndexedToken.fromBox`, Java's `new String(bytes,
/// "UTF-8")`): invalid sequences replaced, not refused. Empty is absent.
fn text(bytes: &[u8]) -> Option<Box<str>> {
    (!bytes.is_empty()).then(|| String::from_utf8_lossy(bytes).into())
}

#[cfg(test)]
#[path = "eip4_tests.rs"]
mod tests;
