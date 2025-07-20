use std::{hash::Hash, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};

use futures_util::stream::{self, BoxStream};

use tower::{
    balance::p2c::Balance,
    buffer::{Buffer, BufferLayer},
    discover::Change,
    layer::{util::Stack, Layer},
    limit::RateLimit,
    retry::Retry,
    timeout::Timeout,
    Service, ServiceBuilder,
};


pub use crate::sinks::util::service::{
    concurrency::Concurrency,
    health::{HealthConfig, HealthLogic, HealthService},
    map::Map,
};
use crate::{
    internal_events::OpenGauge,
    sinks::util::{
        adaptive_concurrency::{
            AdaptiveConcurrencyLimit, AdaptiveConcurrencyLimitLayer, AdaptiveConcurrencySettings,
        },
        retries::{FibonacciRetryPolicy, JitterMode, RetryLogic},
        service::map::MapLayer,
        sink::Response,
        Batch, BatchSink, Partition, PartitionBatchSink,
    },
};

mod concurrency;
mod health;
mod map;

pub type Svc<S, L> =
    RateLimit<AdaptiveConcurrencyLimit<Retry<FibonacciRetryPolicy<L>, Timeout<S>>, L>>;
pub type TowerBatchedSink<S, B, RL> = BatchSink<Svc<S, RL>, B>;
pub type TowerPartitionSink<S, B, RL, K> = PartitionBatchSink<Svc<S, RL>, B, K>;

// Distributed service types
pub type DistributedService<S, RL, HL, K, Req> = RateLimit<
    Retry<
        FibonacciRetryPolicy<RL>,
        Buffer<Req, <Balance<DiscoveryService<S, RL, HL, K>, Req> as Service<Req>>::Future>,
    >,
>;
pub type DiscoveryService<S, RL, HL, K> =
    BoxStream<'static, Result<Change<K, SingleDistributedService<S, RL, HL>>, crate::Error>>;
pub type SingleDistributedService<S, RL, HL> =
    AdaptiveConcurrencyLimit<HealthService<Timeout<S>, HL>, RL>;

pub trait ServiceBuilderExt<L> {
    fn map<R1, R2, F>(self, f: F) -> ServiceBuilder<Stack<MapLayer<R1, R2>, L>>
    where
        F: Fn(R1) -> R2 + Send + Sync + 'static;

    fn settings<RL, Request>(
        self,
        settings: TowerRequestSettings,
        retry_logic: RL,
    ) -> ServiceBuilder<Stack<TowerRequestLayer<RL, Request>, L>>;
}

impl<L> ServiceBuilderExt<L> for ServiceBuilder<L> {
    fn map<R1, R2, F>(self, f: F) -> ServiceBuilder<Stack<MapLayer<R1, R2>, L>>
    where
        F: Fn(R1) -> R2 + Send + Sync + 'static,
    {
        self.layer(MapLayer::new(Arc::new(f)))
    }

    fn settings<RL, Request>(
        self,
        settings: TowerRequestSettings,
        retry_logic: RL,
    ) -> ServiceBuilder<Stack<TowerRequestLayer<RL, Request>, L>> {
        self.layer(TowerRequestLayer {
            settings,
            retry_logic,
            _pd: std::marker::PhantomData,
        })
    }
}


#[derive(Debug, Clone)]
pub struct TowerRequestSettings {
    pub concurrency: Option<usize>,
    pub timeout: Duration,
    pub rate_limit_duration: Duration,
    pub rate_limit_num: u64,
    pub retry_attempts: usize,
    pub retry_max_duration: Duration,
    pub retry_initial_backoff: Duration,
    pub adaptive_concurrency: AdaptiveConcurrencySettings,
    pub retry_jitter_mode: JitterMode,
}

impl TowerRequestSettings {
    pub fn retry_policy<L: RetryLogic>(&self, logic: L) -> FibonacciRetryPolicy<L> {
        FibonacciRetryPolicy::new(
            self.retry_attempts,
            self.retry_initial_backoff,
            self.retry_max_duration,
            logic,
            self.retry_jitter_mode,
        )
    }

    /// Note: This has been deprecated, please do not use when creating new Sinks.
    pub fn partition_sink<B, RL, S, K>(
        &self,
        retry_logic: RL,
        service: S,
        batch: B,
        batch_timeout: Duration,
    ) -> TowerPartitionSink<S, B, RL, K>
    where
        RL: RetryLogic<Response = S::Response>,
        S: Service<B::Output> + Clone + Send + 'static,
        S::Error: Into<crate::Error> + Send + Sync + 'static,
        S::Response: Send + Response,
        S::Future: Send + 'static,
        B: Batch,
        B::Input: Partition<K>,
        B::Output: Send + Clone + 'static,
        K: Hash + Eq + Clone + Send + 'static,
    {
        let service = ServiceBuilder::new()
            .settings(self.clone(), retry_logic)
            .service(service);
        PartitionBatchSink::new(service, batch, batch_timeout)
    }

    /// Note: This has been deprecated, please do not use when creating new Sinks.
    pub fn batch_sink<B, RL, S>(
        &self,
        retry_logic: RL,
        service: S,
        batch: B,
        batch_timeout: Duration,
    ) -> TowerBatchedSink<S, B, RL>
    where
        RL: RetryLogic<Response = S::Response>,
        S: Service<B::Output> + Clone + Send + 'static,
        S::Error: Into<crate::Error> + Send + Sync + 'static,
        S::Response: Send + Response,
        S::Future: Send + 'static,
        B: Batch,
        B::Output: Send + Clone + 'static,
    {
        let service = ServiceBuilder::new()
            .settings(self.clone(), retry_logic)
            .service(service);
        BatchSink::new(service, batch, batch_timeout)
    }

    /// Distributes requests to services [(Endpoint, service, healthcheck)]
    ///
    /// [BufferLayer] suggests that the `buffer_bound` should be at least equal to
    /// the number of the callers of the service. For sinks, this should typically be 1.
    pub fn distributed_service<Req, RL, HL, S>(
        self,
        retry_logic: RL,
        services: Vec<(String, S)>,
        health_config: HealthConfig,
        health_logic: HL,
        buffer_bound: usize,
    ) -> DistributedService<S, RL, HL, usize, Req>
    where
        Req: Clone + Send + 'static,
        RL: RetryLogic<Response = S::Response>,
        HL: HealthLogic<Response = S::Response, Error = crate::Error>,
        S: Service<Req> + Clone + Send + 'static,
        S::Error: Into<crate::Error> + Send + Sync + 'static,
        S::Response: Send,
        S::Future: Send + 'static,
    {
        let policy = self.retry_policy(retry_logic.clone());

        // Build services
        let open = OpenGauge::new();
        let services = services
            .into_iter()
            .map(|(endpoint, inner)| {
                // Build individual service
                ServiceBuilder::new()
                    .layer(AdaptiveConcurrencyLimitLayer::new(
                        self.concurrency,
                        self.adaptive_concurrency,
                        retry_logic.clone(),
                    ))
                    .service(
                        health_config.build(
                            health_logic.clone(),
                            ServiceBuilder::new().timeout(self.timeout).service(inner),
                            open.clone(),
                            endpoint,
                        ), // NOTE: there is a version conflict for crate `tracing` between `tracing_tower` crate
                           // and Vector. Once that is resolved, this can be used instead of passing endpoint everywhere.
                           // .trace_service(|_| info_span!("endpoint", %endpoint)),
                    )
            })
            .enumerate()
            .map(|(i, service)| Ok::<_, S::Error>(Change::Insert(i, service)))
            .collect::<Vec<_>>();

        // Build sink service
        ServiceBuilder::new()
            .rate_limit(self.rate_limit_num, self.rate_limit_duration)
            .retry(policy)
            // [Balance] must be wrapped with a [BufferLayer] so that the overall service implements Clone.
            .layer(BufferLayer::new(buffer_bound))
            .service(Balance::new(Box::pin(stream::iter(services)) as Pin<Box<_>>))
    }
}

#[derive(Debug, Clone)]
pub struct TowerRequestLayer<L, Request> {
    settings: TowerRequestSettings,
    retry_logic: L,
    _pd: PhantomData<Request>,
}

impl<S, RL, Request> Layer<S> for TowerRequestLayer<RL, Request>
where
    S: Service<Request> + Send + 'static,
    S::Response: Send + 'static,
    S::Error: Into<crate::Error> + Send + Sync + 'static,
    S::Future: Send + 'static,
    RL: RetryLogic<Response = S::Response> + Send + 'static,
    Request: Clone + Send + 'static,
{
    type Service = Svc<S, RL>;

    fn layer(&self, inner: S) -> Self::Service {
        let policy = self.settings.retry_policy(self.retry_logic.clone());
        ServiceBuilder::new()
            .rate_limit(
                self.settings.rate_limit_num,
                self.settings.rate_limit_duration,
            )
            .layer(AdaptiveConcurrencyLimitLayer::new(
                self.settings.concurrency,
                self.settings.adaptive_concurrency,
                self.retry_logic.clone(),
            ))
            .retry(policy)
            .timeout(self.settings.timeout)
            .service(inner)
    }
}
