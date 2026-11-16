//! The box cache: the byte budget over random box sizes, the weight of each part of a box,
//! and least-recently-used eviction.

use std::sync::Arc;

use proptest::prelude::*;

use super::*;
use crate::{
    sim::testing::{digest, utxo},
    types::{HexBytes, ergo::NonMandatoryRegisters},
};

/// Box `n`, with a tree, registers and tokens of the given sizes.
fn sized(n: u8, tree: usize, registers: [usize; 6], tokens: usize) -> UTxO {
    let bytes = |len: usize| (len > 0).then(|| HexBytes(vec![0; len]));
    let [r4, r5, r6, r7, r8, r9] = registers.map(bytes);
    UTxO {
        ergo_tree: HexBytes(vec![1; tree]),
        registers: NonMandatoryRegisters { r4, r5, r6, r7, r8, r9 },
        tokens: (0..tokens)
            .map(|t| Token { id: digest(t as u8), amount: 1 })
            .collect(),
        ..utxo(n, 0, 0, &[])
    }
}

fn arb_box(n: u8) -> impl Strategy<Value = UTxO> {
    (0usize..1200, prop::array::uniform6(0usize..500), 0usize..6)
        .prop_map(move |(tree, registers, tokens)| sized(n, tree, registers, tokens))
}

#[derive(Debug, Clone)]
enum Op {
    Insert(usize),
    Get(usize),
}

fn op(boxes: usize) -> impl Strategy<Value = Op> {
    prop_oneof![(0..boxes).prop_map(Op::Insert), (0..boxes).prop_map(Op::Get)]
}

/// The bytes the cached boxes take, weighed again rather than read from moka, so a weigher
/// that counts something else is caught.
fn held(cache: &BoxCache) -> u64 {
    cache.boxes().iter().map(|b| u64::from(weight(b))).sum()
}

proptest! {
    /// After housekeeping, the cache never holds more than its budget, and it keeps the box
    /// inserted last when that box fits, so it is not merely empty.
    #[test]
    fn the_cache_stays_within_its_byte_budget(
        boxes in (1usize..40).prop_flat_map(|n| {
            (0..n).map(|i| arb_box(i as u8)).collect::<Vec<_>>()
        }),
        budget in 0u64..12_000,
        ops in prop::collection::vec(op(40), 1..120),
    ) {
        let boxes: Vec<Arc<UTxO>> = boxes.into_iter().map(Arc::new).collect();
        let cache = BoxCache::new(budget);
        for op in ops {
            match op {
                Op::Insert(i) => {
                    let Some(b) = boxes.get(i) else { continue };
                    cache.insert(b.clone());
                    cache.settle();
                    let fits = budget > 0 && u64::from(weight(b)) <= budget;
                    prop_assert_eq!(cache.get(&b.id).is_some(), fits, "the box inserted last");
                }
                Op::Get(i) => {
                    let Some(b) = boxes.get(i) else { continue };
                    if let Some(found) = cache.get(&b.id) {
                        prop_assert!(Arc::ptr_eq(&found, b));
                    }
                    cache.settle();
                }
            }
            prop_assert!(held(&cache) <= budget, "{} bytes held, budget {budget}", held(&cache));
        }
    }
}

#[test]
fn every_part_of_a_box_is_weighed() {
    let base = weight(&sized(1, 10, [0; 6], 0));
    assert!(base as usize > size_of::<UTxO>(), "the fixed part");
    assert_eq!(weight(&sized(1, 110, [0; 6], 0)) - base, 100, "the tree");
    assert_eq!(weight(&sized(1, 10, [1, 2, 3, 4, 5, 6], 0)) - base, 21, "each register");
    assert_eq!(weight(&sized(1, 10, [0; 6], 3)) - base, 3 * 40, "40 bytes per token");
}

#[test]
fn a_box_larger_than_the_budget_is_not_cached() {
    let b = Arc::new(sized(1, 1000, [0; 6], 0));
    let cache = BoxCache::new(u64::from(weight(&b)) - 1);
    cache.insert(b.clone());
    cache.settle();
    assert!(cache.get(&b.id).is_none());
}

#[test]
fn a_budget_of_zero_caches_nothing() {
    let b = Arc::new(sized(1, 10, [0; 6], 0));
    let cache = BoxCache::disabled();
    cache.insert(b.clone());
    assert!(cache.get(&b.id).is_none());
}

#[test]
fn stats_count_the_boxes_their_weight_and_each_read() {
    let [a, b] = [1, 2].map(|n| Arc::new(sized(n, 100, [0; 6], 0)));
    let cache = BoxCache::new(1 << 20);
    assert_eq!(cache.stats(), CacheStats::default());
    cache.insert(a.clone());
    cache.insert(b.clone());
    assert!(cache.get(&a.id).is_some() && cache.get(&a.id).is_some());
    assert!(cache.get(&digest(9)).is_none());
    // Read with no `settle` first: the stats settle the cache themselves.
    let weighted_bytes = u64::from(weight(&a)) + u64::from(weight(&b));
    assert_eq!(cache.stats(), CacheStats { entries: 2, weighted_bytes, hits: 2, misses: 1 });

    let disabled = BoxCache::disabled();
    disabled.insert(a.clone());
    assert!(disabled.get(&a.id).is_none());
    assert_eq!(disabled.stats(), CacheStats::default(), "no cache, so no hit rate");
}

#[test]
fn the_least_recently_used_box_goes_first() {
    let [a, b, c] = [1, 2, 3].map(|n| Arc::new(sized(n, 100, [0; 6], 0)));
    // Room for two of the three.
    let cache = BoxCache::new(2 * u64::from(weight(&a)));
    for x in [&a, &b] {
        cache.insert(x.clone());
        cache.settle();
    }
    assert!(cache.get(&a.id).is_some(), "a is now used more recently than b");
    cache.settle();
    cache.insert(c.clone());
    cache.settle();
    assert!(cache.get(&b.id).is_none(), "b went");
    assert!(cache.get(&a.id).is_some() && cache.get(&c.id).is_some());
}
