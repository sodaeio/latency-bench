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

mod shred;

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
    /// Supported suffixes: :shredstream, :quic, :soda, :sodaws, :udp.
    /// No suffix implies Yellowstone-compatible gRPC (geyser proto).
    ///
    /// :udp binds a UDP socket and parses raw turbine shreds (shredwatch-style).
    /// The URL is BIND_IP:PORT. Latency is attributed per signature via the
    /// slot the gRPC sources report — UDP "wins" for a tx if it saw any shred
    /// of that tx's slot before the gRPC stream delivered the tx itself.
    ///
    /// :pcap captures shreds via AF_PACKET on Linux without binding the port
    /// (works alongside the validator). URL is [SRC_IP@]IFACE:PORT. Requires
    /// CAP_NET_RAW (e.g. `sudo setcap cap_net_raw=eip latency-bench`).
    ///
    /// Examples:
    ///   richat=http://localhost:10200
    ///   shredpath=http://host:9090:shredstream
    ///   richat-quic=host:10101:quic
    ///   turbine=0.0.0.0:8001:udp
    ///   shredpath-port=45.154.33.82@bond0:10002:pcap
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
    Udp,
    Pcap,
}

struct EndpointConfig {
    name: String,
    url: String,
    kind: EndpointKind,
}

type SigMap = Arc<Mutex<HashMap<String, Vec<(usize, Instant)>>>>;
type SigSlotMap = Arc<Mutex<HashMap<String, u64>>>;
type SlotFirstSeen = Arc<Mutex<HashMap<u64, Instant>>>;

const SUFFIX_MAP: &[(&str, EndpointKind)] = &[
    (":sodaws", EndpointKind::SodaWs),
    (":soda", EndpointKind::Soda),
    (":shredstream", EndpointKind::Shredstream),
    (":quic", EndpointKind::Quic),
    (":udp", EndpointKind::Udp),
    (":pcap", EndpointKind::Pcap),
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
    let sig_slot: SigSlotMap = Arc::new(Mutex::new(HashMap::new()));
    // Per-endpoint slot_first_seen — populated only for UDP endpoints.
    let slot_maps: Vec<Option<SlotFirstSeen>> = endpoints
        .iter()
        .map(|ep| match ep.kind {
            EndpointKind::Udp | EndpointKind::Pcap => Some(Arc::new(Mutex::new(HashMap::new()))),
            _ => None,
        })
        .collect();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let target = args.transactions;
    let seen = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    // UDP/pcap sources can't push signatures themselves — they only know slots.
    // Target completion is driven by the live (non-shred) endpoints.
    let live_endpoints = endpoints
        .iter()
        .filter(|e| e.kind != EndpointKind::Udp && e.kind != EndpointKind::Pcap)
        .count()
        .max(1);

    let mut handles = Vec::new();

    for (idx, ep) in endpoints.iter().enumerate() {
        let sig_map = sig_map.clone();
        let sig_slot = sig_slot.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        let seen = seen.clone();
        let done = done.clone();
        let shutdown_tx = shutdown_tx.clone();
        let url = ep.url.clone();
        let name = ep.name.clone();
        let account = args.account.clone();
        let kind = ep.kind.clone();
        let slot_map = slot_maps[idx].clone();

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
                            run_yellowstone(idx, name, url, account, target, live_endpoints, sig_map, sig_slot, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Shredstream => {
                            run_shredstream(idx, name, url, account, target, live_endpoints, sig_map, sig_slot, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Quic => {
                            run_quic(idx, name, url, account, target, live_endpoints, sig_map, sig_slot, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Soda => {
                            run_soda(idx, name, url, target, live_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::SodaWs => {
                            run_soda_ws(idx, name, url, account, target, live_endpoints, sig_map, seen, done, shutdown_tx, shutdown_rx).await;
                        }
                        EndpointKind::Udp => {
                            run_udp(name, url, slot_map.expect("udp slot_map"), shutdown_rx).await;
                        }
                        EndpointKind::Pcap => {
                            run_pcap(name, url, slot_map.expect("pcap slot_map"), shutdown_rx).await;
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

    // Inject UDP arrivals: for each tx whose slot we know, attribute the UDP
    // endpoint's timestamp as the first time we saw any shred of that slot.
    {
        let slot_lookup = sig_slot.lock().unwrap();
        let mut map = sig_map.lock().unwrap();
        for (udp_idx, slot_map) in slot_maps.iter().enumerate() {
            let Some(slot_map) = slot_map else { continue };
            let slot_seen = slot_map.lock().unwrap();
            let mut hits = 0usize;
            for (sig, slot) in slot_lookup.iter() {
                if let Some(t) = slot_seen.get(slot) {
                    let entry = map.entry(sig.clone()).or_default();
                    if !entry.iter().any(|(i, _)| *i == udp_idx) {
                        entry.push((udp_idx, *t));
                        hits += 1;
                    }
                }
            }
            eprintln!(
                "[{}] attributed {hits} txs from {} slots seen on UDP",
                endpoints[udp_idx].name,
                slot_seen.len(),
            );
        }
    }

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
    sig_slot: SigSlotMap,
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
                            let slot = tx.slot;
                            if let Some(tx_inner) = &tx.transaction {
                                let sig = bs58::encode(&tx_inner.signature).into_string();
                                sig_slot.lock().unwrap().entry(sig.clone()).or_insert(slot);
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
    sig_slot: SigSlotMap,
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
                        let slot = slot_entry.slot;
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
                                    sig_slot.lock().unwrap().entry(sig.clone()).or_insert(slot);
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
    sig_slot: SigSlotMap,
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
                    let slot = tx.slot;
                    if let Some(tx_inner) = &tx.transaction {
                        if let Some(tx_msg) = &tx_inner.transaction {
                            if let Some(msg) = &tx_msg.message {
                                if !msg.account_keys.iter().any(|k| k == &account_bytes) {
                                    continue;
                                }
                            }
                        }
                        let sig = bs58::encode(&tx_inner.signature).into_string();
                        sig_slot.lock().unwrap().entry(sig.clone()).or_insert(slot);
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
// UDP shred capture (shredwatch-style)
// ---------------------------------------------------------------------------

async fn run_udp(
    name: String,
    url: String,
    slot_first_seen: SlotFirstSeen,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::{SocketAddr, ToSocketAddrs};

    let bind_addr: SocketAddr = match url.to_socket_addrs() {
        Ok(mut a) => match a.next() {
            Some(a) => a,
            None => {
                eprintln!("[{name}] failed to resolve bind addr: {url}");
                return;
            }
        },
        Err(e) => {
            eprintln!("[{name}] resolve error: {e}");
            return;
        }
    };

    let domain = if bind_addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
    let sock = match Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{name}] socket() failed: {e}");
            return;
        }
    };
    let _ = sock.set_recv_buffer_size(8 * 1024 * 1024);
    let _ = sock.set_reuse_address(true);
    let _ = sock.set_nonblocking(true);
    if let Err(e) = sock.bind(&bind_addr.into()) {
        eprintln!("[{name}] bind {bind_addr} failed: {e}");
        return;
    }

    let std_sock: std::net::UdpSocket = sock.into();
    let udp = match tokio::net::UdpSocket::from_std(std_sock) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{name}] tokio adopt failed: {e}");
            return;
        }
    };

    eprintln!("[{name}] listening for shreds on {bind_addr}");

    let mut buf = vec![0u8; 2048];
    let mut shreds_seen: u64 = 0;
    let mut slots_seen: u64 = 0;

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                eprintln!("[{name}] stopped after {shreds_seen} shreds across {slots_seen} slots");
                return;
            }
            r = udp.recv_from(&mut buf) => {
                let n = match r {
                    Ok((n, _)) => n,
                    Err(e) => {
                        eprintln!("[{name}] recv error: {e}");
                        continue;
                    }
                };
                let now = Instant::now();
                let Some(key) = shred::parse(&buf[..n]) else { continue };
                shreds_seen += 1;
                let mut map = slot_first_seen.lock().unwrap();
                let std::collections::hash_map::Entry::Vacant(slot) = map.entry(key.slot) else {
                    continue;
                };
                slot.insert(now);
                slots_seen += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AF_PACKET shred capture (Linux)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
async fn run_pcap(
    name: String,
    url: String,
    slot_first_seen: SlotFirstSeen,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    use std::net::Ipv4Addr;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::sync::atomic::AtomicBool;

    // Parse URL: [SRC_IP@]IFACE:PORT
    let (src_ip, iface_port) = match url.split_once('@') {
        Some((ip, rest)) => match ip.parse::<Ipv4Addr>() {
            Ok(a) => (Some(a), rest.to_string()),
            Err(e) => {
                eprintln!("[{name}] bad src ip {ip}: {e}");
                return;
            }
        },
        None => (None, url.clone()),
    };
    let (iface, port_str) = match iface_port.rsplit_once(':') {
        Some(p) => p,
        None => {
            eprintln!("[{name}] bad pcap url: {url} (expected [SRC@]IFACE:PORT)");
            return;
        }
    };
    let port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[{name}] bad port {port_str}: {e}");
            return;
        }
    };
    let iface = iface.to_string();

    // Open AF_PACKET socket
    let fd = unsafe {
        libc::socket(
            libc::AF_PACKET,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            (libc::ETH_P_IP as u16).to_be() as i32,
        )
    };
    if fd < 0 {
        eprintln!(
            "[{name}] AF_PACKET socket failed: {} (need CAP_NET_RAW; try `sudo setcap cap_net_raw=eip <bin>`)",
            std::io::Error::last_os_error()
        );
        return;
    }
    let owned: OwnedFd = unsafe { OwnedFd::from_raw_fd(fd) };

    // Increase RX buffer (best effort)
    let bufsz: i32 = 32 * 1024 * 1024;
    unsafe {
        libc::setsockopt(
            owned.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUFFORCE,
            &bufsz as *const _ as *const _,
            std::mem::size_of_val(&bufsz) as u32,
        );
    }

    // 100ms recv timeout so the blocking thread can check shutdown periodically.
    let tv = libc::timeval { tv_sec: 0, tv_usec: 100_000 };
    unsafe {
        libc::setsockopt(
            owned.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const _,
            std::mem::size_of_val(&tv) as u32,
        );
    }

    // Resolve interface index for SO_BINDTODEVICE-style scoping via sockaddr_ll
    let if_index = match if_nametoindex(&iface) {
        Some(i) => i,
        None => {
            eprintln!("[{name}] interface {iface} not found");
            return;
        }
    };
    let sll = libc::sockaddr_ll {
        sll_family: libc::AF_PACKET as u16,
        sll_protocol: (libc::ETH_P_IP as u16).to_be(),
        sll_ifindex: if_index as i32,
        sll_hatype: 0,
        sll_pkttype: 0,
        sll_halen: 0,
        sll_addr: [0; 8],
    };
    let bind_rc = unsafe {
        libc::bind(
            owned.as_raw_fd(),
            &sll as *const _ as *const _,
            std::mem::size_of::<libc::sockaddr_ll>() as u32,
        )
    };
    if bind_rc < 0 {
        eprintln!(
            "[{name}] bind to {iface} failed: {}",
            std::io::Error::last_os_error()
        );
        return;
    }

    // Build & attach BPF filter (UDP dst port [+ optional src IP])
    let prog = build_bpf(port, src_ip);
    let fprog = SockFprog {
        len: prog.len() as u16,
        filter: prog.as_ptr(),
    };
    let attach_rc = unsafe {
        libc::setsockopt(
            owned.as_raw_fd(),
            libc::SOL_SOCKET,
            SO_ATTACH_FILTER,
            &fprog as *const _ as *const _,
            std::mem::size_of::<SockFprog>() as u32,
        )
    };
    if attach_rc < 0 {
        eprintln!(
            "[{name}] SO_ATTACH_FILTER failed: {}",
            std::io::Error::last_os_error()
        );
        return;
    }

    eprintln!(
        "[{name}] AF_PACKET on {iface}, filter: udp dst port {port}{}",
        src_ip.map(|i| format!(" src {i}")).unwrap_or_default()
    );

    // Shutdown bridge: tokio task flips a bool the blocking thread checks.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_signal = stop.clone();
    tokio::spawn(async move {
        let _ = shutdown_rx.recv().await;
        stop_signal.store(true, Ordering::Release);
    });

    let raw_fd = owned.as_raw_fd();
    let name_for_thread = name.clone();
    let join = tokio::task::spawn_blocking(move || {
        let _keep_alive = owned; // OwnedFd lives until this thread returns
        let mut buf = vec![0u8; 2048];
        let mut shreds: u64 = 0;
        let mut slots: u64 = 0;
        loop {
            if stop.load(Ordering::Acquire) {
                break;
            }
            let n = unsafe {
                libc::recv(raw_fd, buf.as_mut_ptr() as *mut _, buf.len(), 0)
            };
            if n < 0 {
                let err = std::io::Error::last_os_error();
                let kind = err.kind();
                if matches!(kind, std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted) {
                    continue;
                }
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    continue;
                }
                eprintln!("[{name_for_thread}] recv error: {err}");
                break;
            }
            let now = Instant::now();
            let frame = &buf[..n as usize];
            let Some(payload) = strip_eth_ip_udp(frame) else { continue };
            let Some(key) = shred::parse(payload) else { continue };
            shreds += 1;
            let mut map = slot_first_seen.lock().unwrap();
            if let std::collections::hash_map::Entry::Vacant(e) = map.entry(key.slot) {
                e.insert(now);
                slots += 1;
            }
        }
        (shreds, slots)
    });

    let (shreds, slots) = match join.await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[{name}] join error: {e}");
            (0, 0)
        }
    };
    eprintln!("[{name}] stopped after {shreds} shreds across {slots} slots");
}

#[cfg(not(target_os = "linux"))]
async fn run_pcap(
    name: String,
    _url: String,
    _slot_first_seen: SlotFirstSeen,
    _shutdown_rx: broadcast::Receiver<()>,
) {
    eprintln!("[{name}] :pcap is Linux-only");
}

// Strip Ethernet (14) + variable IPv4 + UDP (8) and return the UDP payload.
#[cfg(target_os = "linux")]
fn strip_eth_ip_udp(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() < 14 + 20 + 8 { return None; }
    let ihl = (frame[14] & 0x0f) as usize * 4;
    if ihl < 20 { return None; }
    let udp_off = 14 + ihl;
    if frame.len() < udp_off + 8 { return None; }
    Some(&frame[udp_off + 8..])
}

#[cfg(target_os = "linux")]
fn if_nametoindex(name: &str) -> Option<u32> {
    let cname = std::ffi::CString::new(name).ok()?;
    let idx = unsafe { libc::if_nametoindex(cname.as_ptr()) };
    if idx == 0 { None } else { Some(idx) }
}

// Minimal classic-BPF builder. Layout matches the cBPF emitted for
//   `udp and dst port PORT [and src host SRC]`.
#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct SockFilter { code: u16, jt: u8, jf: u8, k: u32 }

#[cfg(target_os = "linux")]
#[repr(C)]
struct SockFprog { len: u16, filter: *const SockFilter }

#[cfg(target_os = "linux")]
const SO_ATTACH_FILTER: i32 = 26;

#[cfg(target_os = "linux")]
fn build_bpf(port: u16, src_ip: Option<std::net::Ipv4Addr>) -> Vec<SockFilter> {
    // BPF opcodes
    const LDH_ABS: u16 = 0x28;  // ld halfword absolute
    const LDB_ABS: u16 = 0x30;  // ld byte absolute
    const LDW_ABS: u16 = 0x20;  // ld word absolute
    const LDX_MSH: u16 = 0xb1;  // X = 4 * (P[k] & 0xf)
    const LDH_IND: u16 = 0x48;  // ld halfword from X+k
    const JEQ_K:   u16 = 0x15;  // if A == k
    const JSET_K:  u16 = 0x45;  // if A & k
    const RET_K:   u16 = 0x06;  // ret k

    let port = port as u32;
    let mut p: Vec<SockFilter> = Vec::new();
    let push = |p: &mut Vec<SockFilter>, code, jt, jf, k| p.push(SockFilter { code, jt, jf, k });

    // i=0  ldh [12]              ; ethertype
    push(&mut p, LDH_ABS, 0, 0, 12);
    // i=1  jeq #0x0800 -> next, else DROP
    let i1 = p.len();
    push(&mut p, JEQ_K, 0, 0, 0x0800);
    // i=2  ldb [23]               ; ip proto
    push(&mut p, LDB_ABS, 0, 0, 23);
    // i=3  jeq #17 -> next, else DROP
    let i3 = p.len();
    push(&mut p, JEQ_K, 0, 0, 17);
    // optional src ip filter
    let i_src = if let Some(ip) = src_ip {
        push(&mut p, LDW_ABS, 0, 0, 26);
        let i = p.len();
        push(&mut p, JEQ_K, 0, 0, u32::from(ip));
        Some(i)
    } else { None };
    // ldh [20]; jset #0x1fff (fragment) -> DROP
    push(&mut p, LDH_ABS, 0, 0, 20);
    let i_frag = p.len();
    push(&mut p, JSET_K, 0, 0, 0x1fff);
    // ldxb 4*([14]&0xf); ldh [x+16]
    push(&mut p, LDX_MSH, 0, 0, 14);
    push(&mut p, LDH_IND, 0, 0, 16);
    // jeq #PORT -> ACCEPT, else DROP
    let i_port = p.len();
    push(&mut p, JEQ_K, 0, 0, port);
    // ACCEPT
    let accept_idx = p.len();
    push(&mut p, RET_K, 0, 0, 0xffff);
    // DROP
    let drop_idx = p.len();
    push(&mut p, RET_K, 0, 0, 0);

    let set_jf = |p: &mut Vec<SockFilter>, idx: usize, target: usize| {
        p[idx].jf = (target - idx - 1) as u8;
    };
    let set_jt = |p: &mut Vec<SockFilter>, idx: usize, target: usize| {
        p[idx].jt = (target - idx - 1) as u8;
    };
    set_jf(&mut p, i1, drop_idx);
    set_jf(&mut p, i3, drop_idx);
    if let Some(i) = i_src { set_jf(&mut p, i, drop_idx); }
    set_jt(&mut p, i_frag, drop_idx);          // jset: branch if matched (fragment) -> drop
    set_jt(&mut p, i_port, accept_idx);
    set_jf(&mut p, i_port, drop_idx);
    p
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
