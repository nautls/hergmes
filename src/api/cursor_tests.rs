//! The cursor: a round trip for any position, its length, and each way a cursor is refused.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use proptest::prelude::*;

use super::*;
use crate::types::HexBytes;

fn tree(bytes: &[u8]) -> TrackingKey {
    TrackingKey::Tree(HexBytes(bytes.to_vec()))
}

fn cursor(height: u32, inclusion_height: u32) -> Cursor {
    Cursor {
        at: BlockRef { height, header_id: Digest([7; 32]) },
        after: BoxKey { inclusion_height, box_id: Digest([9; 32]) },
    }
}

proptest! {
    #[test]
    fn a_cursor_decodes_to_what_was_encoded(
        height in any::<u32>(),
        header_id in any::<[u8; 32]>(),
        inclusion_height in any::<u32>(),
        box_id in any::<[u8; 32]>(),
        key in prop_oneof![
            prop::collection::vec(any::<u8>(), 1..300).prop_map(|b| tree(&b)),
            any::<[u8; 32]>().prop_map(|id| TrackingKey::Token(Digest(id))),
        ],
    ) {
        let cursor = Cursor {
            at: BlockRef { height, header_id: Digest(header_id) },
            after: BoxKey { inclusion_height, box_id: Digest(box_id) },
        };
        let encoded = cursor.encode(&key);
        prop_assert_eq!(encoded.len(), 108);
        prop_assert!(encoded.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'));
        prop_assert_eq!(Cursor::decode(&encoded, &key), Ok(cursor));
    }
}

#[test]
fn a_cursor_for_another_key_is_refused() {
    let encoded = cursor(10, 3).encode(&tree(&[1, 2, 3]));
    assert_eq!(Cursor::decode(&encoded, &tree(&[1, 2, 4])), Err(CursorError::OtherKey));
    // A token id with the bytes of a 32-byte tree is another key.
    let bytes = [5; 32];
    let encoded = cursor(10, 3).encode(&tree(&bytes));
    let token = TrackingKey::Token(Digest(bytes));
    assert_eq!(Cursor::decode(&encoded, &token), Err(CursorError::OtherKey));
}

#[test]
fn a_cursor_of_another_version_is_refused() {
    let key = tree(&[1]);
    let mut bytes = URL_SAFE_NO_PAD.decode(cursor(10, 3).encode(&key)).unwrap();
    bytes[0] = 2;
    let encoded = URL_SAFE_NO_PAD.encode(&bytes);
    assert_eq!(Cursor::decode(&encoded, &key), Err(CursorError::Version(2)));
}

#[test]
fn malformed_cursors_are_refused() {
    let key = tree(&[1]);
    let good = cursor(10, 3).encode(&key);
    let bytes = URL_SAFE_NO_PAD.decode(&good).unwrap();
    for (case, encoded) in [
        ("empty", String::new()),
        ("not base64url", format!("+{}", &good[1..])),
        ("padded", format!("{good}=")),
        ("a byte short", URL_SAFE_NO_PAD.encode(&bytes[..bytes.len() - 1])),
        ("a byte long", URL_SAFE_NO_PAD.encode([&bytes[..], &[0]].concat())),
    ] {
        assert_eq!(Cursor::decode(&encoded, &key), Err(CursorError::Malformed), "{case}");
    }
}
