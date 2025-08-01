use agent_lib::event::MetricTags;
use byteorder::{ByteOrder, NativeEndian};
use std::{collections::HashMap, io, path::Path};

use netlink_packet_core::{
    NetlinkHeader, NetlinkMessage, NetlinkPayload, NLM_F_ACK, NLM_F_DUMP, NLM_F_REQUEST,
};
use netlink_packet_sock_diag::{
    constants::*,
    inet::{ExtensionFlags, InetRequest, InetResponseHeader, SocketId, StateFlags},
    SockDiagMessage,
};
use netlink_sys::{
    protocols::NETLINK_SOCK_DIAG, AsyncSocket, AsyncSocketExt, SocketAddr, TokioSocket,
};
use snafu::{ResultExt, Snafu};

use super::HostMetrics;

const PROC_IPV6_FILE: &str = "/proc/net/if_inet6";
const TCP_CONNS_TOTAL: &str = "tcp_connections_total";
const TCP_TX_QUEUED_BYTES_TOTAL: &str = "tcp_tx_queued_bytes_total";
const TCP_RX_QUEUED_BYTES_TOTAL: &str = "tcp_rx_queued_bytes_total";
const STATE: &str = "state";

impl HostMetrics {
    /* 获取tcp的指标 */
    pub async fn tcp_metrics(&self, output: &mut super::MetricsBuffer) {
        match build_tcp_stats().await {
            Ok(stats) => {
                output.name = "tcp";
                for (state, count) in stats.conn_states {
                    let tags = metric_tags! {
                        STATE => state
                    };
                    output.gauge(TCP_CONNS_TOTAL, count, tags);
                }

                output.gauge(
                    TCP_TX_QUEUED_BYTES_TOTAL,
                    stats.tx_queued_bytes,
                    MetricTags::default(),
                );
                output.gauge(
                    TCP_RX_QUEUED_BYTES_TOTAL,
                    stats.rx_queued_bytes,
                    MetricTags::default(),
                );
            }
            Err(error) => {
                error!("Failed to load tcp connection info: {}", error);
            }
        }
    }
}

#[derive(Debug, Snafu)]
enum TcpError {
    #[snafu(display("Could not open new netlink socket: {}", source))]
    NetlinkSocket { source: io::Error },
    #[snafu(display("Could not send netlink message: {}", source))]
    NetlinkSend { source: io::Error },
    #[snafu(display("Could not parse netlink response: {}", source))]
    NetlinkParse {
        source: netlink_packet_utils::DecodeError,
    },
    #[snafu(display("Could not recognize TCP state {state}"))]
    InvalidTcpState { state: u8 },
    #[snafu(display("Received an error message from netlink; code: {code}"))]
    NetlinkMsgError { code: i32 },
    #[snafu(display("Invalid message length: {length}"))]
    InvalidLength { length: usize },
}

#[repr(u8)]
enum TcpState {
    Established = 1,
    SynSent = 2,
    SynRecv = 3,
    FinWait1 = 4,
    FinWait2 = 5,
    TimeWait = 6,
    Close = 7,
    CloseWait = 8,
    LastAck = 9,
    Listen = 10,
    Closing = 11,
}

impl From<TcpState> for String {
    fn from(val: TcpState) -> Self {
        match val {
            TcpState::Established => "established".into(),
            TcpState::SynSent => "syn_sent".into(),
            TcpState::SynRecv => "syn_recv".into(),
            TcpState::FinWait1 => "fin_wait1".into(),
            TcpState::FinWait2 => "fin_wait2".into(),
            TcpState::TimeWait => "time_wait".into(),
            TcpState::Close => "close".into(),
            TcpState::CloseWait => "close_wait".into(),
            TcpState::LastAck => "last_ack".into(),
            TcpState::Listen => "listen".into(),
            TcpState::Closing => "closing".into(),
        }
    }
}

impl TryFrom<u8> for TcpState {
    type Error = TcpError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(TcpState::Established),
            2 => Ok(TcpState::SynSent),
            3 => Ok(TcpState::SynRecv),
            4 => Ok(TcpState::FinWait1),
            5 => Ok(TcpState::FinWait2),
            6 => Ok(TcpState::TimeWait),
            7 => Ok(TcpState::Close),
            8 => Ok(TcpState::CloseWait),
            9 => Ok(TcpState::LastAck),
            10 => Ok(TcpState::Listen),
            11 => Ok(TcpState::Closing),
            _ => Err(TcpError::InvalidTcpState { state: value }),
        }
    }
}

#[derive(Debug, Default)]
struct TcpStats {
    conn_states: HashMap<String, f64>,
    rx_queued_bytes: f64,
    tx_queued_bytes: f64,
}
/* 从缓冲区解析Netlink消息，提取InetResponseHeader。

参数：
- buffer - 包含Netlink消息的原始字节切片
- headers - 用于存储解析后的InetResponseHeader的可变代理

返回：
- Ok(true) 如果解析完成（收到Done消息）
- Ok(false) 如果期望更多数据，此时可再次调用此函数
- Err(TcpError) 在长度无效、反序列化失败或Netlink错误时

错误：
在消息长度无效或Netlink错误时返回TcpError变体。 */
/// Parses Netlink messages from a buffer, extracting [`InetResponseHeader`]s.
fn parse_netlink_messages(
    buffer: &[u8],
    headers: &mut Vec<InetResponseHeader>,
) -> Result<bool, TcpError> {
    let mut offset = 0;
    let mut done = false;

    while offset < buffer.len() {
        let remaining_bytes = &buffer[offset..];
        if remaining_bytes.len() < 4 {
            // Still treat this as an error since we can't even read the length
            return Err(TcpError::InvalidLength {
                length: remaining_bytes.len(),
            });
        }
        // This function panics if the buffer length is less than 4.
        let length = NativeEndian::read_u32(&remaining_bytes[0..4]) as usize;
        if length == 0 {
            break;
        }
        if length > remaining_bytes.len() {
            return Err(TcpError::InvalidLength { length });
        }

        let msg_bytes = &remaining_bytes[..length];
        let rx_packet =
            <NetlinkMessage<SockDiagMessage>>::deserialize(msg_bytes).context(NetlinkParseSnafu)?;

        match rx_packet.payload {
            NetlinkPayload::InnerMessage(SockDiagMessage::InetResponse(response)) => {
                headers.push(response.header);
            }
            NetlinkPayload::Done(_) => {
                done = true;
                break;
            }
            NetlinkPayload::Error(error) => {
                if let Some(code) = error.code {
                    return Err(TcpError::NetlinkMsgError { code: code.get() });
                }
            }
            _ => {}
        }

        offset += length;
    }

    Ok(done)
}

/// Fetches [`InetResponseHeader`]s for TCP sockets using Netlink.
///
/// # Arguments
/// * `addr_family` - Address family (`AF_INET` for IPv4, `AF_INET6` for IPv6).
///
/// # Returns
/// * `Ok(Vec<InetResponseHeader>)` containing headers for active TCP sockets.
/// * `Err(TcpError)` on socket creation, send, receive, or parsing errors.
///
/// # Errors
/// Returns [`TcpError`] for socket-related or message parsing failures.
///
/// # Notes
/// Asynchronously queries the kernel via a Netlink socket for TCP socket info.
async fn fetch_netlink_inet_headers(addr_family: u8) -> Result<Vec<InetResponseHeader>, TcpError> {
    let unicast_socket: SocketAddr = SocketAddr::new(0, 0);
    let mut socket = TokioSocket::new(NETLINK_SOCK_DIAG).context(NetlinkSocketSnafu)?;

    let mut inet_req = InetRequest {
        family: addr_family,
        protocol: IPPROTO_TCP,
        extensions: ExtensionFlags::INFO,
        states: StateFlags::all(),
        socket_id: SocketId::new_v4(),
    };
    if addr_family == AF_INET6 {
        inet_req.socket_id = SocketId::new_v6();
    }

    let mut hdr = NetlinkHeader::default();
    hdr.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_DUMP;
    let mut msg = NetlinkMessage::new(hdr, SockDiagMessage::InetRequest(inet_req).into());
    msg.finalize();

    let mut buf = vec![0; msg.header.length as usize];
    msg.serialize(&mut buf[..]);

    socket
        .send_to(&buf[..msg.buffer_len()], &unicast_socket)
        .await
        .context(NetlinkSendSnafu)?;

    let mut receive_buffer = vec![0; 4096];
    let mut inet_resp_hdrs = Vec::with_capacity(32); // Pre-allocate with an estimate

    while let Ok(()) = socket.recv(&mut &mut receive_buffer[..]).await {
        let done = parse_netlink_messages(&receive_buffer, &mut inet_resp_hdrs)?;
        if done {
            break;
        }
    }

    Ok(inet_resp_hdrs)
}

fn parse_nl_inet_hdrs(
    hdrs: Vec<InetResponseHeader>,
    tcp_stats: &mut TcpStats,
) -> Result<(), TcpError> {
    for hdr in hdrs {
        let state: TcpState = hdr.state.try_into()?;
        let state_str: String = state.into();
        *tcp_stats.conn_states.entry(state_str).or_insert(0.0) += 1.0;
        tcp_stats.tx_queued_bytes += f64::from(hdr.send_queue);
        tcp_stats.rx_queued_bytes += f64::from(hdr.recv_queue)
    }

    Ok(())
}
/* 构建TCP当前的状态? */
async fn build_tcp_stats() -> Result<TcpStats, TcpError> {
    let mut tcp_stats = TcpStats::default();
    let resp = fetch_netlink_inet_headers(AF_INET).await?;
    parse_nl_inet_hdrs(resp, &mut tcp_stats)?;

    if is_ipv6_enabled() {
        let resp = fetch_netlink_inet_headers(AF_INET6).await?;
        parse_nl_inet_hdrs(resp, &mut tcp_stats)?;
    }

    Ok(tcp_stats)
}

fn is_ipv6_enabled() -> bool {
    Path::new(PROC_IPV6_FILE).exists()
}
