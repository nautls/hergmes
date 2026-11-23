use super::*;
use crate::{
    fixtures::{load, sample},
    types::{
        address::Address,
        ergo::{IndexedBlock, IndexedBox},
    },
};

fn token(key: &str) -> TrackingKey {
    TrackingKey::Token(sample(key).parse().unwrap())
}

#[test]
fn address_key_matches_its_unspent_boxes() {
    let key = TrackingKey::Tree(
        Address::decode(&sample("address"))
            .unwrap()
            .into_ergo_tree(),
    );
    let boxes: Vec<IndexedBox> = load("unspent_by_ergo_tree");
    assert!(!boxes.is_empty());
    assert!(boxes.iter().all(|b| key.matches(&b.utxo)));

    let others: Vec<IndexedBox> = load("unspent_by_token_small_supply");
    assert!(others.iter().all(|b| !key.matches(&b.utxo)));
}

#[test]
fn token_key_matches_boxes_holding_the_token() {
    let small = token("smallSupplyToken");
    let boxes: Vec<IndexedBox> = load("unspent_by_token_small_supply");
    assert!(boxes.iter().all(|b| small.matches(&b.utxo)));

    let nft = token("nft");
    assert!(boxes.iter().all(|b| !nft.matches(&b.utxo)));
    let nft_boxes: Vec<IndexedBox> = load("unspent_by_token_nft");
    assert!(nft_boxes.iter().all(|b| nft.matches(&b.utxo)));
}

#[test]
fn a_box_matches_its_tree_and_each_token_it_holds() {
    let boxes: Vec<IndexedBox> = load("unspent_by_token_nft");
    let utxo = &boxes[0].utxo;
    assert!(TrackingKey::Tree(utxo.ergo_tree.clone()).matches(utxo));
    assert!(token("nft").matches(utxo));
    for t in &utxo.tokens {
        assert!(TrackingKey::Token(t.id).matches(utxo));
    }
}

#[test]
fn nft_moves_once_per_block() {
    let block: IndexedBlock = load("indexed_block");
    let nft = token("nft");
    let count =
        |boxes: Vec<&IndexedBox>| boxes.into_iter().filter(|b| nft.matches(&b.utxo)).count();
    assert_eq!(
        count(
            block
                .transactions
                .iter()
                .flat_map(|tx| &tx.inputs)
                .collect()
        ),
        1
    );
    assert_eq!(
        count(
            block
                .transactions
                .iter()
                .flat_map(|tx| &tx.outputs)
                .collect()
        ),
        1
    );
}

/// A template key matches every box the node lists under its template's hash, over several
/// trees, and none of another template.
#[test]
fn template_key_matches_the_boxes_of_its_trees() {
    let boxes: Vec<IndexedBox> = load("unspent_by_template");
    let bytes = template(&boxes[0].utxo.ergo_tree.0).expect("segregated");
    let key = TrackingKey::Template(HexBytes::from(bytes.to_vec()));
    assert!(boxes.iter().all(|b| key.matches(&b.utxo)));

    let others: Vec<IndexedBox> = load("unspent_by_token_small_supply");
    assert!(!others.iter().any(|b| key.matches(&b.utxo)));
    // Trees without segregated constants have no template here, whatever the node files them
    // under.
    let plain: Vec<IndexedBox> = load("unspent_by_template_p2pk");
    let p2pk = TrackingKey::Template(HexBytes::from(plain[0].utxo.ergo_tree.0[1..].to_vec()));
    assert!(!plain.iter().any(|b| p2pk.matches(&b.utxo)));
}

#[test]
fn has_templates_only_with_a_template_key() {
    let mut keys: BTreeMap<TrackingKey, ()> =
        [(TrackingKey::Tree(HexBytes::from(vec![4; 5])), ()), (token("nft"), ())].into();
    assert!(!has_templates(&keys));
    keys.insert(TrackingKey::Template(HexBytes::from(vec![1])), ());
    assert!(has_templates(&keys));
}

/// A box's template key comes out only when asked for, and only for a segregated tree.
#[test]
fn keys_of_yields_a_template_only_when_asked() {
    let templates = |utxo: &UTxO, asked: bool| {
        crate::engine::overlay::keys_of(utxo, asked)
            .filter(|k| matches!(k, TrackingKey::Template(_)))
            .count()
    };
    let boxes: Vec<IndexedBox> = load("unspent_by_template");
    assert_eq!(templates(&boxes[0].utxo, false), 0);
    assert_eq!(templates(&boxes[0].utxo, true), 1);
    let plain: Vec<IndexedBox> = load("unspent_by_template_p2pk");
    assert_eq!(templates(&plain[0].utxo, true), 0);
}
