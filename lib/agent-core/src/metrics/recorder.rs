use std::sync::{atomic::Ordering, Arc, RwLock};
use std::{cell::OnceCell, time::Duration};

use chrono::Utc;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::{registry::Registry as MetricsRegistry, MetricKindMask};
use quanta::Clock;

use super::metric_matcher::MetricKeyMatcher;
use super::recency::{GenerationalStorage, Recency};
use super::storage::VectorStorage;
use crate::event::{Metric, MetricValue};

thread_local!(static LOCAL_REGISTRY: OnceCell<Registry> = const { OnceCell::new() });

#[allow(dead_code)]
pub(super) struct Registry {
    registry: MetricsRegistry<Key, GenerationalStorage<VectorStorage>>,
    recency: RwLock<Option<Recency<Key, MetricKeyMatcher>>>,
}

impl Registry {
    /* 产生一个新的registry */
    fn new() -> Self {
        Self {
            /* 看来是个metrics  registry, 主要参数是存储 */
            registry: MetricsRegistry::new(GenerationalStorage::new(VectorStorage)),
            recency: RwLock::new(None),
        }
    }

    pub(super) fn clear(&self) {
        self.registry.clear();
    }

    pub(super) fn set_expiry(
        &self,
        global_timeout: Option<Duration>,
        expire_metrics_per_metric_set: Vec<(MetricKeyMatcher, Duration)>,
    ) {
        let recency = if global_timeout.is_none() && expire_metrics_per_metric_set.is_empty() {
            None
        } else {
            Some(Recency::new(
                Clock::new(),
                MetricKindMask::ALL,
                global_timeout,
                expire_metrics_per_metric_set,
            ))
        };
        *(self.recency.write()).expect("Failed to acquire write lock on recency map") = recency;
    }

    pub(super) fn visit_metrics(&self) -> Vec<Metric> {
        let timestamp = Utc::now();

        let mut metrics = Vec::new();
        let recency = self
            .recency
            .read()
            .expect("Failed to acquire read lock on recency map");
        let recency = recency.as_ref();

        for (key, counter) in self.registry.get_counter_handles() {
            if recency
                .is_none_or(|recency| recency.should_store_counter(&key, &counter, &self.registry))
            {
                // NOTE this will truncate if the value is greater than 2**52.
                #[allow(clippy::cast_precision_loss)]
                let value = counter.get_inner().load(Ordering::Relaxed) as f64;
                let value = MetricValue::Counter { value };
                metrics.push(Metric::from_metric_kv(&key, value, timestamp));
            }
        }
        for (key, gauge) in self.registry.get_gauge_handles() {
            if recency
                .is_none_or(|recency| recency.should_store_gauge(&key, &gauge, &self.registry))
            {
                let value = gauge.get_inner().load(Ordering::Relaxed);
                let value = MetricValue::Gauge { value };
                metrics.push(Metric::from_metric_kv(&key, value, timestamp));
            }
        }
        for (key, histogram) in self.registry.get_histogram_handles() {
            if recency.is_none_or(|recency| {
                recency.should_store_histogram(&key, &histogram, &self.registry)
            }) {
                let value = histogram.get_inner().make_metric();
                metrics.push(Metric::from_metric_kv(&key, value, timestamp));
            }
        }
        metrics
    }

    fn get_counter(&self, key: &Key) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn get_gauge(&self, key: &Key) -> Gauge {
        self.registry.get_or_create_gauge(key, |c| c.clone().into())
    }

    fn get_histogram(&self, key: &Key) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

///自动为 VectorRecorder 实现 Clone trait，使得枚举的实例可以被克隆
#[derive(Clone)] /*  */
pub(super) enum VectorRecorder {
    /* 包含一组registry （用于存储metric） */
    Global(Arc<Registry>),
    ThreadLocal,
}

impl VectorRecorder {
    /* 函数创建registry （一个metric registry , 包含了特定的存储） */
    pub(super) fn new_global() -> Self {
        /* Global 变体包含一个 Arc<Registry>，用于全局的 Registry。Arc 是一个线程安全的引用计数智能指针，允许多个所有者共享同一个 Registry 实例。 */
        Self::Global(Arc::new(Registry::new()))
    }

    pub(super) fn new_test() -> Self {
        Self::with_thread_local(Registry::clear);
        Self::ThreadLocal
    }

    pub(super) fn with_registry<T>(&self, doit: impl FnOnce(&Registry) -> T) -> T {
        match &self {
            Self::Global(registry) => doit(registry),
            // This is only called after the registry is created, so we can just use a dummy
            // idle_timeout parameter.
            Self::ThreadLocal => Self::with_thread_local(doit),
        }
    }

    fn with_thread_local<T>(doit: impl FnOnce(&Registry) -> T) -> T {
        LOCAL_REGISTRY.with(|oc| doit(oc.get_or_init(Registry::new)))
    }
}

impl Recorder for VectorRecorder {
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.with_registry(|r| r.get_counter(key))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.with_registry(|r| r.get_gauge(key))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.with_registry(|r| r.get_histogram(key))
    }

    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
}
