use vector_config::configurable_component;

/// TCP keepalive settings for socket-based components.
#[configurable_component]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[configurable(metadata(docs::human_name = "Wait Time"))]
pub struct TcpKeepaliveConfig {
    /// The time to wait before starting to send TCP keepalive probes on an idle connection.
    #[configurable(metadata(docs::type_unit = "seconds"))]
    pub time_secs: Option<u64>,
}
