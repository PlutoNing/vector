use proptest::collection::{btree_set, hash_map, hash_set};
use proptest::option;
use proptest::prelude::*;



use super::{
    samples_to_buckets, Bucket, MetricTags, MetricValue, Quantile, Sample,
    StatisticKind, TagValue, TagValueSet,
};

fn realistic_float() -> proptest::num::f64::Any {
    proptest::num::f64::POSITIVE | proptest::num::f64::NEGATIVE | proptest::num::f64::ZERO
}

impl Arbitrary for StatisticKind {
    type Parameters = ();
    type Strategy = BoxedStrategy<StatisticKind>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        let strategy = prop_oneof![Just(StatisticKind::Histogram), Just(StatisticKind::Summary)];
        strategy.boxed()
    }
}

impl Arbitrary for Sample {
    type Parameters = ();
    type Strategy = BoxedStrategy<Sample>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (realistic_float(), any::<u32>())
            .prop_map(|(value, rate)| Sample { value, rate })
            .boxed()
    }
}

impl Arbitrary for Bucket {
    type Parameters = ();
    type Strategy = BoxedStrategy<Bucket>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (realistic_float(), any::<u64>())
            .prop_map(|(upper_limit, count)| Bucket { upper_limit, count })
            .boxed()
    }
}

impl Arbitrary for Quantile {
    type Parameters = ();
    type Strategy = BoxedStrategy<Quantile>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (0.0..=1.0, realistic_float())
            .prop_map(|(quantile, value)| Quantile { quantile, value })
            .boxed()
    }
}

impl Arbitrary for TagValue {
    type Parameters = ();
    type Strategy = BoxedStrategy<TagValue>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        option::of("[[:^cntrl:]]{0,16}")
            .prop_map(TagValue::from)
            .boxed()
    }
}

impl Arbitrary for TagValueSet {
    type Parameters = ();
    type Strategy = BoxedStrategy<TagValueSet>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        hash_set("[[:^cntrl:]]{0,16}", 1..16)
            .prop_map(|values| values.into_iter().collect())
            .boxed()
    }
}

impl Arbitrary for MetricTags {
    type Parameters = ();
    type Strategy = BoxedStrategy<MetricTags>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        hash_map("[[:word:]]{1,32}", "[[:^cntrl:]]{1,32}", 0..16)
            .prop_map(|values| values.into_iter().collect())
            .boxed()
    }
}
