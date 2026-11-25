use super::*;
use crate::{
    fixtures,
    types::{HexBytes, ergo::BoxById},
};

fn hex(s: &str) -> HexBytes {
    hex::decode(s).unwrap().into()
}

fn registers(r7: Option<&str>, r8: Option<&str>, r9: Option<&str>) -> NonMandatoryRegisters {
    NonMandatoryRegisters {
        r7: r7.map(hex),
        r8: r8.map(hex),
        r9: r9.map(hex),
        ..Default::default()
    }
}

fn asset(r7: &str, r8: Option<&str>, r9: Option<&str>) -> Option<Asset> {
    Asset::from_registers(&registers(Some(r7), r8, r9))
}

/// `Coll[Byte]` holding `bytes`, as a register's constant.
fn coll(bytes: &[u8]) -> String {
    format!("0e{:02x}{}", bytes.len(), hex::encode(bytes))
}

const HASH: &str = "c20f0300da131d6ba9cc39f0b710ac007a1a1e2abb0d13e0f640f8f0646bba29";

fn minting_box(name: &str) -> NonMandatoryRegisters {
    fixtures::load::<BoxById>(name).indexed.utxo.registers
}

/// The recorded picture NFT: its type, hash, link and cover.
#[test]
fn a_recorded_picture_nft_is_read_whole() {
    let asset = Asset::from_registers(&minting_box("minting_box_picture")).unwrap();
    assert_eq!(asset.asset_type, AssetType::NftPicture);
    assert_eq!(asset.artwork_hash, Some(HASH.parse().unwrap()));
    assert_eq!(
        asset.artwork_url.as_deref(),
        Some("https://sigmafi.app/assets/nautilus-6ad63456.svg")
    );
    assert_eq!(
        asset.artwork_cover.as_deref(),
        Some("https://upload.wikimedia.org/wikipedia/commons/6/6e/USS_Nautilus_SSN571.JPG")
    );
}

/// A token with only a name, a description and decimals has no asset type.
#[test]
fn a_recorded_plain_token_has_no_asset() {
    assert_eq!(Asset::from_registers(&minting_box("minting_box_plain")), None);
    assert_eq!(Asset::from_registers(&NonMandatoryRegisters::default()), None);
}

#[test]
fn a_single_link_is_the_url_and_no_cover() {
    let asset = asset("0e020103", None, Some(&coll(b"ipfs://video"))).unwrap();
    assert_eq!(asset.artwork_url.as_deref(), Some("ipfs://video"));
    assert_eq!(asset.artwork_cover, None);
    assert_eq!(asset.artwork_hash, None);
}

/// `Coll[Byte]` with its element in a byte of its own, and a pair written as a 2-tuple, are
/// the same types to the node.
#[test]
fn types_in_their_long_encodings_are_read() {
    let url = &coll(b"u")[2..];
    let r9 = format!("60020c020c02{url}{url}");
    let asset = asset("0c02020101", Some(&format!("0c0220{HASH}")), Some(&r9)).unwrap();
    assert_eq!(asset.asset_type, AssetType::NftPicture);
    assert!(asset.artwork_hash.is_some());
    assert_eq!(asset.artwork_url.as_deref(), Some("u"));
    assert_eq!(asset.artwork_cover.as_deref(), Some("u"));
}

/// Only an artwork NFT has artwork: the R8 and R9 of a membership token, an artwork
/// collection or file attachments mean something else.
#[test]
fn only_artwork_nfts_have_artwork() {
    let (r8, r9) = (coll(&[7; 32]), coll(b"x"));
    for (r7, asset_type) in [
        ("0e020201", AssetType::MembershipThresholdSignature),
        ("0e020104", AssetType::NftArtworkCollection),
        ("0e02010f", AssetType::NftFileAttachments),
    ] {
        let asset = asset(r7, Some(&r8), Some(&r9)).unwrap();
        assert_eq!(asset.asset_type, asset_type);
        assert_eq!((asset.artwork_hash, asset.artwork_url), (None, None), "{r7}");
    }
    for r7 in ["0e020101", "0e020102", "0e020103", "0e0101", "0e020199"] {
        let asset = asset(r7, Some(&r8), Some(&r9)).unwrap();
        assert!(asset.artwork_hash.is_some() && asset.artwork_url.is_some(), "{r7}");
    }
}

/// Each registered type by its name, read by R7's first two bytes. An NFT whose subcategory
/// is left out or unregistered is still an NFT, with its artwork; any other unregistered type
/// is no asset at all.
#[test]
fn asset_types_are_named() {
    for (r7, asset_type, name) in [
        ("0e020101", AssetType::NftPicture, "nft-picture"),
        ("0e020102", AssetType::NftAudio, "nft-audio"),
        ("0e020103", AssetType::NftVideo, "nft-video"),
        ("0e020104", AssetType::NftArtworkCollection, "nft-artwork-collection"),
        ("0e02010f", AssetType::NftFileAttachments, "nft-file-attachments"),
        ("0e0101", AssetType::Nft, "nft"),
        ("0e020199", AssetType::Nft, "nft"),
        ("0e03010199", AssetType::NftPicture, "nft-picture"),
        ("0e020201", AssetType::MembershipThresholdSignature, "membership-threshold-signature"),
    ] {
        assert_eq!(asset(r7, None, None).unwrap().asset_type, asset_type, "{r7}");
        assert_eq!(asset_type.as_str(), name);
    }
    let nft = asset("0e0101", Some(&coll(&[7; 32])), None).unwrap();
    assert!(nft.artwork_hash.is_some(), "a category alone is still an NFT");
    for r7 in ["0e00", "0e0102", "0e0202", "0e020202", "0e03ff0102"] {
        assert_eq!(asset(r7, None, None), None, "{r7}");
    }
}

#[test]
fn a_hash_of_another_length_is_dropped() {
    for len in [0, 31, 33] {
        let asset = asset("0e020101", Some(&coll(&vec![7; len])), None).unwrap();
        assert_eq!(asset.artwork_hash, None, "{len}");
    }
}

/// Text is decoded as the node decodes a name, and an empty link is no link.
#[test]
fn links_are_lossy_utf8_and_empty_is_absent() {
    let nft = asset("0e020101", None, Some(&coll(b"a\xffb"))).unwrap();
    assert_eq!(nft.artwork_url.as_deref(), Some("a\u{fffd}b"));
    let r9 = format!("3c0e0e00{}", &coll(b"cover")[2..]);
    let nft = asset("0e020101", None, Some(&r9)).unwrap();
    assert_eq!((nft.artwork_url.as_deref(), nft.artwork_cover.as_deref()), (None, Some("cover")));
}

/// Any other shape is as if the register were absent: another type, a value cut short, bytes
/// past the value, a length past the end.
#[test]
fn other_shapes_are_absent() {
    assert_eq!(asset("0402", None, None), None, "an Int R7");
    assert_eq!(asset("0e020101ff", None, None), None, "a byte past R7's value");
    assert_eq!(asset("0e030101", None, None), None, "R7 cut short");
    assert_eq!(asset("0e80", None, None), None, "a length cut short");
    assert_eq!(asset("0effffffffffffffffffff01", None, None), None, "a length past the end");

    let link = coll(b"link");
    for r9 in [
        "0402".to_owned(),
        format!("{link}00"),
        format!("3c0e04{}02", &link[2..]),
        format!("3c0e0e{}", &link[2..]),
        format!("60030e0e0e{0}{0}{0}", &link[2..]),
        "1a0100".to_owned(),
    ] {
        let nft = asset("0e020101", None, Some(&r9)).unwrap();
        assert_eq!((nft.artwork_url, nft.artwork_cover), (None, None), "{r9}");
    }
}

#[test]
fn heap_size_counts_every_byte_held() {
    let r9 = format!("3c0e0e{}{}", &coll(b"abc")[2..], &coll(b"de")[2..]);
    let asset = asset("0e020101", Some(&coll(&[7; 32])), Some(&r9)).unwrap();
    assert_eq!(asset.heap_size(), 3 + 2);
}

fn of_mint(registers: &NonMandatoryRegisters) -> Standard {
    Standard::of_mint("n".into(), "d".into(), 2, registers)
}

/// Both recorded mints are EIP-4, with the indexer's fields and the picture's asset; a mint
/// with nothing to read follows no standard, so the indexer's defaults are dropped.
#[test]
fn the_recorded_mints_are_eip4() {
    let picture = minting_box("minting_box_picture");
    assert_eq!(
        of_mint(&picture),
        Standard::Eip4(Eip4 {
            name: "n".into(),
            description: "d".into(),
            decimals: 2,
            asset: Asset::from_registers(&picture).map(Box::new),
        })
    );
    let Standard::Eip4(plain) = of_mint(&minting_box("minting_box_plain")) else {
        panic!("the plain mint is EIP-4");
    };
    assert_eq!(plain.asset, None);
    let nothing = Standard::of_mint("".into(), "".into(), 0, &NonMandatoryRegisters::default());
    assert_eq!(nothing, Standard::None);
}

/// The indexer's fields with `r6` as the minting box's only register.
fn with_r6(name: &str, description: &str, decimals: i32, r6: Option<&str>) -> Standard {
    let registers = NonMandatoryRegisters { r6: r6.map(hex), ..Default::default() };
    Standard::of_mint(name.into(), description.into(), decimals, &registers)
}

/// Any one of a name, a description, decimals or an asset type is enough. Decimals of `0`
/// count, as a decimal string or an `Int`; non-zero decimals the indexer read count whatever
/// R6 holds, as a number in non-ASCII digits that this does not parse.
#[test]
fn any_one_field_is_eip4() {
    let arabic_eight = coll("٨".as_bytes());
    for (name, description, decimals, r6) in [
        ("n", "", 0, None),
        ("", "d", 0, None),
        ("", "", 8, Some(arabic_eight.as_str())),
        ("", "", 0, Some(coll(b"0").as_str())),
        ("", "", 0, Some("0e022d30")),
        ("", "", 0, Some("0400")),
    ] {
        let standard = with_r6(name, description, decimals, r6);
        assert_eq!(standard.as_str(), "eip-4", "{name:?} {description:?} {decimals} {r6:?}");
    }

    let empty = coll(b"");
    let picture = NonMandatoryRegisters {
        r4: Some(hex(&empty)),
        r5: Some(hex(&empty)),
        r6: Some(hex(&empty)),
        ..registers(Some("0e020101"), Some(&coll(&[7; 32])), Some(&coll(b"ipfs://a")))
    };
    let Standard::Eip4(nft) = Standard::of_mint("".into(), "".into(), 0, &picture) else {
        panic!("an asset type alone is EIP-4");
    };
    assert_eq!(nft.asset.unwrap().asset_type, AssetType::NftPicture);
}

/// The indexer's defaults, with nothing in R6 that it reads as decimals: text that is no
/// `Int`, or a constant of another type.
#[test]
fn nothing_the_indexer_reads_is_none() {
    for r6 in [None, Some(coll(b"")), Some(coll(b"1.5")), Some(coll(b"99999999999"))]
        .iter()
        .map(Option::as_deref)
        .chain([Some("0502")])
    {
        assert_eq!(with_r6("", "", 0, r6), Standard::None, "{r6:?}");
    }
}

/// An EIP-4 token weighs its strings and its asset's links; none weighs nothing.
#[test]
fn standards_are_named_and_weighed() {
    let r9 = coll(&[b'u'; 100]);
    let nft = NonMandatoryRegisters {
        r4: Some(hex(&coll(b"ab"))),
        ..registers(Some("0e020101"), None, Some(&r9))
    };
    let eip4 = Standard::of_mint("ab".into(), "cde".into(), 0, &nft);
    assert_eq!(eip4.as_str(), "eip-4");
    assert_eq!(eip4.heap_size(), 2 + 3 + size_of::<Asset>() + 100);
    assert_eq!(Standard::None.as_str(), "none");
    assert_eq!(Standard::None.heap_size(), 0);
}
