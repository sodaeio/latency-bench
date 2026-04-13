use {
    anyhow::Result,
    clap::Parser,
    futures::StreamExt,
    std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::sync::broadcast,
};

pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            include!(concat!(env!("OUT_DIR"), "/solana.storage.confirmed_block.rs"));
        }
    }
}

pub mod geyser {
    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
}

pub mod shredstream {
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub mod richat {
    include!(concat!(env!("OUT_DIR"), "/richat.rs"));
}

pub mod soda {
    pub mod stream {
        include!(concat!(env!("OUT_DIR"), "/soda.stream.rs"));
    }
}

use geyser::{SubscribeRequest, SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(about = "Compare latency across Solana gRPC / shredstream / QUIC endpoints")]
struct Args {
    /// Endpoints as name=url pairs with optional protocol suffix.
    ///
    /// Supported suffixes: :shredstream, :quic, :soda, :sodaws, :sodatcp.
    /// No suffix implies Yellowstone-compatible gRPC (geyser proto).
    ///
    /// Examples:
    ///   richat=http://localhost:10200
    ///   shredpath=http://host:9090:shredstream
    ///   richat-quic=host:10101:quic
    #[arg(short, long, required = true, num_args = 1..)]
    endpoint: Vec<String>,

    /// Number of matched transactions to collect before stopping
    #[arg(short, long, default_value = "1000")]
    transactions: usize,

    /// Account pubkey to filter transactions on
    #[arg(short, long, default_value = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")]
    account: String,
}

// ---------------------------------------------------------------------------
// Endpoint config
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq)]
enum EndpointKind {
    Yellowstone,
    Shredstream,
    Quic,
    Soda,
    SodaWs,
}

struct EndpointConfig {
    name: String,
    url: String,
    kind: EndpointKind,
}

type SigMap = Arc<Mutex<HashMap<String, Vec<(usize, Instant)>>>>;

const SUFFIX_MAP: &[(&str, EndpointKind)] = &[
    (":sodaws", EndpointKind::SodaWs),
    (":soda", EndpointKind::Soda),
    (":shredstream", EndpointKind::Shredstream),
    (":quic", EndpointKind::Quic),
];

fn parse_endpoint(s: &str) -> EndpointConfig {
    let (name, rest) = s
        .split_once('=')
        .unwrap_or_else(|| panic!("invalid endpoint format: {s} — expected name=url[:suffix]"));

    for (suffix, kind) in SUFFIX_MAP {
        if let Some(url) = rest.strip_suffix(suffix) {
            return EndpointConfig {
                name: name.to_string(),
                url: url.to_string(),
                kind: kind.clone(),
            };
        }
    }

    EndpointConfig {
        name: name.to_string(),
        url: rest.to_string(),
        kind: EndpointKind::Yellowstone,
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let endpoints: Vec<EndpointConfig> = args.endpoint.iter().map(|e| parse_endpoint(e)).collect();
    let num_endpoints = endpoints.len();

    let sig_map: SigMap = Arc::new(Mutex::new(HashMap::new()));
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let target = args.transactions;
    let seen = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();

    for (idx, ep) in endpoints.iter().enumerate() {
        let sig_map = sig_map.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let seen = seen.clone();
        let done = done.clone();
        let shutdown_tx = shutdown_tx.clone();
        let url = ep.url.clone();
        let name = ep.name.clone();
        let account = args.account.clone();
        let kind = ep.kind.clone();

        let handle = std::thread::Builder::new()
            .name(format!("bench-{name}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");
                rt.block_on(async move {
                    match kind {
                        EndpointKind::Yellowstone => {
                            run_yellowstone(idx, name, url, account, target, num_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Shredstream => {
                            run_shredstream(idx, name, url, account, target, num_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Quic => {
                            run_quic(idx, name, url, account, target, num_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Soda => {
                            run_soda(idx, name, url, target, num_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::SodaWs => {
                            run_soda_ws(idx, name, url, account, target, num_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                    }
                });
            })
            .expect("failed to spawn bench thread");
        handles.push(handle);
    }

    loop {
        if handles.iter().any(|h| h.is_finished()) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = shutdown_tx.send(());
    std::thread::sleep(Duration::from_millis(500));

    // Analyze
    let map = sig_map.lock().unwrap();
    let mut deltas: Vec<Vec<f64>> = vec![Vec::new(); num_endpoints];
    let mut wins = vec![0usize; num_endpoints];

    for arrivals in map.values() {
        if arrivals.len() < 2 {
            continue;
        }
        let earliest = arrivals.iter().map(|(_, t)| *t).min().unwrap();
        for (idx, time) in arrivals {
            deltas[*idx].push(time.duration_since(earliest).as_secs_f64() * 1_000.0);
        }
        for (idx, time) in arrivals {
            if *time == earliest {
                wins[*idx] += 1;
                break;
            }
        }
    }

    let matched = map.values().filter(|v| v.len() >= 2).count();
    print_report(&endpoints, &map, &deltas, &wins, matched, num_endpoints);

    Ok(())
}

// ---------------------------------------------------------------------------
// Signature recording
// ---------------------------------------------------------------------------

fn record_signature(
    sig_map: &SigMap,
    seen: &AtomicUsize,
    done: &AtomicBool,
    shutdown_tx: &broadcast::Sender<()>,
    name: &str,
    idx: usize,
    target: usize,
    num_endpoints: usize,
    sig: &str,
) {
    let now = Instant::now();
    let all_seen = {
        let mut map = sig_map.lock().unwrap();
        let entry = map.entry(sig.to_string()).or_default();
        if entry.iter().any(|(i, _)| *i == idx) {
            return;
        }
        entry.push((idx, now));
        entry.len() == num_endpoints
    };

    if all_seen {
        let count = seen.fetch_add(1, Ordering::Relaxed) + 1;
        if count % (target / 10).max(1) == 0 {
            eprintln!("[progress] {count}/{target} matched");
        }
        if count >= target && !done.swap(true, Ordering::AcqRel) {
            eprintln!("[{name}] reached target, waiting 3s for stragglers...");
            let shutdown = shutdown_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                let _ = shutdown.send(());
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Report / table rendering
// ---------------------------------------------------------------------------

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const WHITE: &str = "\x1b[97m";
const GRAY: &str = "\x1b[90m";

fn fmt_dur(ms: f64) -> String {
    if ms.abs() < 1.0 {
        format!("{:.2}µs", ms * 1000.0)
    } else if ms.abs() < 10.0 {
        format!("{:.2}ms", ms)
    } else {
        format!("{:.1}ms", ms)
    }
}

fn percentile(sorted: &[f64], p: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

fn hot_color(ms: f64) -> &'static str {
    if ms >= 10.0 {
        RED
    } else if ms >= 1.0 {
        YELLOW
    } else {
        GREEN
    }
}

fn colored(color: &str, text: &str) -> String {
    format!("{color}{text}{RESET}")
}

fn visible_len(s: &str) -> usize {
    let mut len = 0;
    let mut in_esc = false;
    for c in s.chars() {
        if in_esc {
            if c.is_ascii_alphabetic() {
                in_esc = false;
            }
        } else if c == '\x1b' {
            in_esc = true;
        } else {
            len += 1;
        }
    }
    len
}

fn pad(s: &str, width: usize, align: char) -> String {
    let vis = visible_len(s);
    if vis >= width {
        return s.to_string();
    }
    let gap = width - vis;
    match align {
        '<' => format!("{s}{}", " ".repeat(gap)),
        '>' => format!("{}{s}", " ".repeat(gap)),
        _ => {
            let left = gap / 2;
            let right = gap - left;
            format!("{}{s}{}", " ".repeat(left), " ".repeat(right))
        }
    }
}

struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    aligns: Vec<char>,
}

impl Table {
    fn new(headers: &[&str], aligns: &[char]) -> Self {
        Self {
            headers: headers.iter().map(|s| s.to_string()).collect(),
            rows: Vec::new(),
            aligns: aligns.to_vec(),
        }
    }

    fn add_row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    fn print(&self, indent: &str) {
        let cols = self.headers.len();
        let mut widths = vec![0usize; cols];
        for (i, h) in self.headers.iter().enumerate() {
            widths[i] = visible_len(h);
        }
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(visible_len(cell));
            }
        }

        let hline = |left: &str, mid: &str, right: &str| {
            let segs: Vec<String> = widths.iter().map(|w| "─".repeat(w + 2)).collect();
            print!("{indent}{WHITE}{left}{}{right}{RESET}\n", segs.join(mid));
        };

        let fmt_row = |cells: &[String], is_header: bool| {
            let mut out = format!("{indent}{WHITE}│{RESET}");
            for (i, cell) in cells.iter().enumerate() {
                let a = if is_header { '^' } else { self.aligns[i] };
                out.push_str(&format!(" {} {WHITE}│{RESET}", pad(cell, widths[i], a)));
            }
            println!("{out}");
        };

        hline("┌", "┬", "┐");
        let styled: Vec<String> = self.headers.iter().map(|h| format!("{BOLD}{WHITE}{h}{RESET}")).collect();
        fmt_row(&styled, true);
        hline("├", "┼", "┤");
        for (i, row) in self.rows.iter().enumerate() {
            fmt_row(row, false);
            if i < self.rows.len() - 1 {
                hline("├", "┼", "┤");
            }
        }
        hline("└", "┴", "┘");
    }
}

fn print_report(
    endpoints: &[EndpointConfig],
    map: &HashMap<String, Vec<(usize, Instant)>>,
    deltas: &[Vec<f64>],
    wins: &[usize],
    matched: usize,
    num_endpoints: usize,
) {
    // Rank by wins → assign colors
    let mut ranked: Vec<usize> = (0..num_endpoints).collect();
    ranked.sort_by(|a, b| wins[*b].cmp(&wins[*a]));
    let mut ep_color = vec![YELLOW; num_endpoints];
    if num_endpoints >= 2 {
        ep_color[ranked[0]] = GREEN;
        ep_color[ranked[num_endpoints - 1]] = RED;
    }

    // Compute percentiles per endpoint
    let pcts: Vec<[f64; 5]> = (0..num_endpoints)
        .map(|idx| {
            let mut d = deltas[idx].clone();
            d.sort_by(|a, b| a.partial_cmp(b).unwrap());
            [
                percentile(&d, 5),
                percentile(&d, 25),
                percentile(&d, 50),
                percentile(&d, 95),
                percentile(&d, 99),
            ]
        })
        .collect();

    println!();
    println!("  {BOLD}{WHITE}Latency Comparison — {matched} transactions matched by 2+ endpoints{RESET}");
    println!();
    println!("  {BOLD}{WHITE}Latency Distribution{RESET}");
    println!();

    let mut table = Table::new(&["Endpoint", "p5", "p25", "p50", "p95", "p99"], &['<', '>', '>', '>', '>', '>']);
    for (idx, ep) in endpoints.iter().enumerate() {
        let c = ep_color[idx];
        let p = &pcts[idx];
        table.add_row(vec![
            colored(c, &ep.name),
            colored(hot_color(p[0]), &fmt_dur(p[0])),
            colored(hot_color(p[1]), &fmt_dur(p[1])),
            colored(hot_color(p[2]), &fmt_dur(p[2])),
            colored(hot_color(p[3]), &fmt_dur(p[3])),
            colored(hot_color(p[4]), &fmt_dur(p[4])),
        ]);
    }
    table.print("  ");
    println!();

    // Head-to-head pairwise comparisons
    for i in 0..num_endpoints {
        for j in (i + 1)..num_endpoints {
            let mut pair_deltas: Vec<f64> = Vec::new();
            let mut i_wins = 0usize;
            let mut j_wins = 0usize;

            for arrivals in map.values() {
                let ti = arrivals.iter().find(|(k, _)| *k == i).map(|(_, t)| *t);
                let tj = arrivals.iter().find(|(k, _)| *k == j).map(|(_, t)| *t);
                if let (Some(ti), Some(tj)) = (ti, tj) {
                    if ti < tj {
                        i_wins += 1;
                        pair_deltas.push(-(tj.duration_since(ti).as_secs_f64() * 1_000.0));
                    } else if tj < ti {
                        j_wins += 1;
                        pair_deltas.push(ti.duration_since(tj).as_secs_f64() * 1_000.0);
                    }
                }
            }

            let common = pair_deltas.len();
            if common == 0 {
                continue;
            }

            let ci = ep_color[i];
            let cj = ep_color[j];

            println!("  {BOLD}{WHITE}Head-to-Head (common transactions only){RESET}");
            println!();
            println!("  {GRAY}•{RESET} Common transactions: {BOLD}{common}{RESET}");

            let mut h2h = vec![(&endpoints[i].name, ci, i_wins), (&endpoints[j].name, cj, j_wins)];
            h2h.sort_by(|a, b| b.2.cmp(&a.2));
            for (name, color, w) in &h2h {
                let pv = (*w as f64 / common as f64) * 100.0;
                println!("  {GRAY}•{RESET} {color}{name}{RESET} faster: {BOLD}{w}{RESET} {GRAY}({pv:.1}%){RESET}");
            }
            println!();

            // Advantage table
            let (winner, loser, positive) = if j_wins >= i_wins { (j, i, true) } else { (i, j, false) };
            let wn = &endpoints[winner].name;
            let ln = &endpoints[loser].name;
            let wc = ep_color[winner];
            let lc = ep_color[loser];

            let mut adv: Vec<f64> = pair_deltas
                .iter()
                .map(|&d| if positive { d } else { -d })
                .filter(|&d| d > 0.0)
                .collect();
            adv.sort_by(|a, b| a.partial_cmp(b).unwrap());

            if !adv.is_empty() {
                println!("  {BOLD}{wc}{wn}{RESET} Advantage over {lc}{ln}{RESET}");
                println!();

                let a = [
                    percentile(&adv, 5),
                    percentile(&adv, 25),
                    percentile(&adv, 50),
                    percentile(&adv, 95),
                    percentile(&adv, 99),
                ];

                let mut t = Table::new(&["Metric", "p5", "p25", "p50", "p95", "p99"], &['^', '>', '>', '>', '>', '>']);
                t.add_row(vec![
                    format!("{BOLD}Advantage{RESET}"),
                    colored(hot_color(a[0]), &fmt_dur(a[0])),
                    colored(hot_color(a[1]), &fmt_dur(a[1])),
                    colored(hot_color(a[2]), &fmt_dur(a[2])),
                    colored(hot_color(a[3]), &fmt_dur(a[3])),
                    colored(hot_color(a[4]), &fmt_dur(a[4])),
                ]);
                t.print("  ");
            }
            println!();
        }
    }
}

// ---------------------------------------------------------------------------
// Endpoint runners
// ---------------------------------------------------------------------------

async fn run_yellowstone(
    idx: usize,
    name: String,
    url: String,
    account: String,
    target: usize,
    num_endpoints: usize,
    sig_map: SigMap,
    seen: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let channel = match tonic::transport::Channel::from_shared(url.clone())
        .unwrap()
        .initial_stream_window_size(2 * 1024 * 1024)
        .initial_connection_window_size(4 * 1024 * 1024)
        .tcp_nodelay(true)
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{name}] connect failed: {e}");
            return;
        }
    };

    let mut client = geyser::geyser_client::GeyserClient::new(channel)
        .max_decoding_message_size(64 * 1024 * 1024);

    let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);
    let _ = req_tx
        .send(SubscribeRequest {
            transactions: HashMap::from([(
                "bench".to_string(),
                SubscribeRequestFilterTransactions {
                    account_include: vec![account],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        })
        .await;
    drop(req_tx);

    let response = match client
        .subscribe(tokio_stream::wrappers::ReceiverStream::new(req_rx))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[{name}] subscribe failed: {e}");
            return;
        }
    };

    eprintln!("[{name}] subscribed");
    let mut stream = response.into_inner();

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(update)) => {
                        if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof {
                            if let Some(tx_inner) = &tx.transaction {
                                let sig = bs58::encode(&tx_inner.signature).into_string();
                                record_signature(&sig_map, &seen, &done, &shutdown_tx, &name, idx, target, num_endpoints, &sig);
                            }
                        }
                    }
                    Some(Err(e)) => { eprintln!("[{name}] error: {e}"); return; }
                    None => { eprintln!("[{name}] stream ended"); return; }
                }
            }
            _ = shutdown_rx.recv() => return,
        }
    }
}

async fn run_shredstream(
    idx: usize,
    name: String,
    url: String,
    account: String,
    target: usize,
    num_endpoints: usize,
    sig_map: SigMap,
    seen: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let account_pubkey: solana_pubkey::Pubkey = match account.parse() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[{name}] invalid account pubkey: {e}");
            return;
        }
    };

    let mut client =
        match shredstream::shredstream_proxy_client::ShredstreamProxyClient::connect(url.clone()).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[{name}] connect failed: {e}");
                return;
            }
        };

    let mut stream = match client.subscribe_entries(shredstream::SubscribeEntriesRequest {}).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            eprintln!("[{name}] subscribe failed: {e}");
            return;
        }
    };

    eprintln!("[{name}] subscribed");

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => return,
            msg = stream.next() => {
                match msg {
                    Some(Ok(slot_entry)) => {
                        let entries: Vec<solana_entry::entry::Entry> = match bincode::deserialize(&slot_entry.entries) {
                            Ok(e) => e,
                            Err(e) => {
                                eprintln!("[{name}] deserialize error: {e}");
                                continue;
                            }
                        };
                        for entry in entries {
                            for tx in entry.transactions {
                                if tx.message.static_account_keys().iter().any(|k| k == &account_pubkey) {
                                    let sig = tx.signatures[0].to_string();
                                    record_signature(&sig_map, &seen, &done, &shutdown_tx, &name, idx, target, num_endpoints, &sig);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => { eprintln!("[{name}] error: {e}"); return; }
                    None => { eprintln!("[{name}] stream ended"); return; }
                }
            }
        }
    }
}

async fn run_quic(
    idx: usize,
    name: String,
    url: String,
    account: String,
    target: usize,
    num_endpoints: usize,
    sig_map: SigMap,
    seen: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    use prost::Message;
    use quinn::crypto::rustls::QuicClientConfig;
    use std::net::{SocketAddr, ToSocketAddrs};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr: SocketAddr = match url.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(a) => a,
            None => {
                eprintln!("[{name}] failed to resolve: {url}");
                return;
            }
        },
        Err(e) => {
            eprintln!("[{name}] resolve error: {e}");
            return;
        }
    };

    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))))
        .with_no_client_auth();

    let quic_client_config = match QuicClientConfig::try_from(crypto) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{name}] quic config error: {e}");
            return;
        }
    };

    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(0u8.into());
    transport_config.max_concurrent_uni_streams(1u32.into());
    let stream_rwnd: u32 = 12_500_000 / 1_000 * 100;
    transport_config.stream_receive_window(stream_rwnd.into());
    transport_config.send_window(8 * stream_rwnd as u64);
    transport_config.max_idle_timeout(Some(quinn::VarInt::from_u32(30_000).into()));

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_client_config));
    client_config.transport_config(Arc::new(transport_config));

    let bind_addr: SocketAddr = if addr.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };

    let mut endpoint = match quinn::Endpoint::client(bind_addr) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[{name}] endpoint error: {e}");
            return;
        }
    };
    endpoint.set_default_client_config(client_config);

    let conn = match endpoint.connect(addr, "localhost") {
        Ok(connecting) => match connecting.await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[{name}] connection failed: {e}");
                return;
            }
        },
        Err(e) => {
            eprintln!("[{name}] connect error: {e}");
            return;
        }
    };

    let subscribe_req = richat::QuicSubscribeRequest {
        x_token: None,
        recv_streams: 1,
        max_backlog: None,
        replay_from_slot: None,
        filter: Some(richat::RichatFilter {
            disable_accounts: true,
            disable_transactions: false,
            disable_entries: true,
        }),
    };
    let encoded = subscribe_req.encode_to_vec();

    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{name}] open_bi failed: {e}");
            return;
        }
    };

    if let Err(e) = send.write_u64(encoded.len() as u64).await {
        eprintln!("[{name}] write request len failed: {e}");
        return;
    }
    if let Err(e) = send.write_all(&encoded).await {
        eprintln!("[{name}] write request failed: {e}");
        return;
    }
    if let Err(e) = send.flush().await {
        eprintln!("[{name}] flush failed: {e}");
        return;
    }

    let resp_len = match recv.read_u64().await {
        Ok(n) => n as usize,
        Err(e) => {
            eprintln!("[{name}] read response len failed: {e}");
            return;
        }
    };
    let mut resp_buf = vec![0u8; resp_len];
    if let Err(e) = recv.read_exact(&mut resp_buf).await {
        eprintln!("[{name}] read response failed: {e}");
        return;
    }
    let response = match richat::QuicSubscribeResponse::decode(resp_buf.as_slice()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[{name}] decode response failed: {e}");
            return;
        }
    };
    if response.error.is_some() {
        eprintln!("[{name}] subscribe error: {:?}", response.error);
        return;
    }

    let uni_stream = match conn.accept_uni().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{name}] accept_uni failed: {e}");
            return;
        }
    };

    eprintln!("[{name}] subscribed (version: {})", response.version);

    let mut reader = tokio::io::BufReader::new(uni_stream);
    let account_bytes = bs58::decode(&account).into_vec().unwrap_or_default();

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => return,
            msg_id_result = reader.read_u64() => {
                let msg_id = match msg_id_result {
                    Ok(id) => id,
                    Err(e) => {
                        eprintln!("[{name}] read msg_id failed: {e}");
                        return;
                    }
                };

                if msg_id == u64::MAX {
                    let size = reader.read_u64().await.unwrap_or(0) as usize;
                    let mut buf = vec![0u8; size];
                    let _ = reader.read_exact(&mut buf).await;
                    eprintln!("[{name}] received close message");
                    return;
                }

                let size = match reader.read_u64().await {
                    Ok(n) => n as usize,
                    Err(e) => {
                        eprintln!("[{name}] read size failed: {e}");
                        return;
                    }
                };

                let mut buf = vec![0u8; size];
                if let Err(e) = reader.read_exact(&mut buf).await {
                    eprintln!("[{name}] read message failed: {e}");
                    return;
                }

                let update = match geyser::SubscribeUpdate::decode(buf.as_slice()) {
                    Ok(u) => u,
                    Err(_) => continue,
                };

                if let Some(UpdateOneof::Transaction(tx)) = update.update_oneof {
                    if let Some(tx_inner) = &tx.transaction {
                        if let Some(tx_msg) = &tx_inner.transaction {
                            if let Some(msg) = &tx_msg.message {
                                if !msg.account_keys.iter().any(|k| k == &account_bytes) {
                                    continue;
                                }
                            }
                        }
                        let sig = bs58::encode(&tx_inner.signature).into_string();
                        record_signature(&sig_map, &seen, &done, &shutdown_tx, &name, idx, target, num_endpoints, &sig);
                    }
                }
            }
        }
    }
}

async fn run_soda(
    idx: usize,
    name: String,
    url: String,
    target: usize,
    num_endpoints: usize,
    sig_map: SigMap,
    seen: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let channel = match tonic::transport::Channel::from_shared(url.clone())
        .unwrap()
        .initial_stream_window_size(2 * 1024 * 1024)
        .initial_connection_window_size(4 * 1024 * 1024)
        .tcp_nodelay(true)
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{name}] connect failed: {e}");
            return;
        }
    };

    let mut client = soda::stream::soda_stream_service_client::SodaStreamServiceClient::new(channel)
        .max_decoding_message_size(64 * 1024 * 1024);

    let response = match client
        .subscribe(soda::stream::SubscribeRequest {
            dex_protocols: vec![soda::stream::DexProtocol::PumpAmm as i32],
            program_ids: vec![],
            token_addresses: vec![],
            pool_addresses: vec![],
            event_types: vec![soda::stream::EventType::Trade as i32],
        })
        .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[{name}] subscribe failed: {e}");
            return;
        }
    };

    eprintln!("[{name}] subscribed");
    let mut stream = response.into_inner();

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    Some(Ok(event)) => {
                        if let Some(trade) = &event.trade {
                            if !trade.tx_hash.is_empty() {
                                record_signature(&sig_map, &seen, &done, &shutdown_tx, &name, idx, target, num_endpoints, &trade.tx_hash);
                            }
                        }
                    }
                    Some(Err(e)) => { eprintln!("[{name}] error: {e}"); return; }
                    None => { eprintln!("[{name}] stream ended"); return; }
                }
            }
            _ = shutdown_rx.recv() => return,
        }
    }
}

async fn run_soda_ws(
    idx: usize,
    name: String,
    url: String,
    account: String,
    target: usize,
    num_endpoints: usize,
    sig_map: SigMap,
    seen: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    use futures::SinkExt;
    use tokio_tungstenite::connect_async;

    let ws_url = format!("{url}/v1/ws");
    let (ws_stream, _) = match connect_async(&ws_url).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{name}] WS connect failed: {e}");
            return;
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // Read welcome message
    let _ = read.next().await;

    let sub = serde_json::json!({
        "method": "subscribe",
        "channel": "trades",
        "mints": [account, "So11111111111111111111111111111111111111112"]
    });
    let _ = write
        .send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string().into()))
        .await;

    // Read ack
    let _ = read.next().await;

    eprintln!("[{name}] subscribed");

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                            if parsed.get("type").and_then(|t| t.as_str()) == Some("trade") {
                                if let Some(sig) = parsed.pointer("/data/txHash").and_then(|s| s.as_str()) {
                                    record_signature(&sig_map, &seen, &done, &shutdown_tx, &name, idx, target, num_endpoints, sig);
                                }
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => { eprintln!("[{name}] WS error: {e}"); return; }
                    None => { eprintln!("[{name}] WS closed"); return; }
                }
            }
            _ = shutdown_rx.recv() => return,
        }
    }
}

// ---------------------------------------------------------------------------
// TLS: skip server certificate verification for QUIC benchmarks
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
