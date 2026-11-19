use super::target;

#[test]
fn raises_the_soft_limit_to_the_hard_one() {
    assert_eq!(target(1_024, Some(524_288)), Some(524_288));
    assert_eq!(target(1_024, Some(2_048)), Some(2_048));
}

#[test]
fn leaves_a_soft_limit_at_the_hard_one() {
    assert_eq!(target(524_288, Some(524_288)), None);
    assert_eq!(target(1_024, Some(1_024)), None);
}

#[test]
fn leaves_the_soft_limit_under_an_unlimited_hard_one() {
    assert_eq!(target(1_024, None), None);
    assert_eq!(target(1_000_000, None), None);
}
