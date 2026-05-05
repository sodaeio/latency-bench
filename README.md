# latency-bench

Compare latency across Solana streaming endpoints side-by-side. Subscribes to multiple endpoints in parallel, matches transactions by signature, and reports which endpoint delivered each transaction first.

## Supported protocols

| Suffix | Protocol | Description |
|---|---|---|
| *(none)* | Yellowstone gRPC | Standard geyser-compatible gRPC (Yellowstone, Richat, etc.) |
| `:shredstream` | Shredstream | Entry stream from a shredstream proxy (ShredPath, Jito shredstream-proxy) |
| `:quic` | QUIC | Richat QUIC transport |
| `:soda` | Soda gRPC | Soda stream service (protobuf/gRPC) |
| `:sodaws` | Soda WebSocket | Soda stream service (WebSocket) |
| `:udp` | Raw UDP shreds | Bind a UDP socket and timestamp incoming turbine shreds (shredwatch-style) |
| `:pcap` | AF_PACKET shred capture | Linux-only sniff via AF_PACKET + cBPF, no port bind. URL `[SRC@]IFACE:PORT`. Needs `CAP_NET_RAW` |

## Build

```bash
cargo build --release
```

## Usage

```
latency-bench \
  -e "richat=http://localhost:10200" \
  -e "shredpath=http://host:9090:shredstream" \
  -t 1000
```

### Options

| Flag | Default | Description |
|---|---|---|
| `-e, --endpoint` | *(required)* | `name=url[:suffix]` pairs (at least 2 recommended) |
| `-t, --transactions` | `1000` | Number of matched transactions to collect |
| `-a, --account` | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | Account pubkey to filter on |

### Examples

#### Yellowstone gRPC

```bash
# Two Yellowstone-compatible gRPC endpoints
latency-bench \
  -e "validator-direct=http://localhost:10000" \
  -e "fanout-relay=http://10.0.0.2:10200" \
  -t 1500

# Custom account filter
latency-bench \
  -e "yellowstone-a=http://hostA:10000" \
  -e "yellowstone-b=http://hostB:10000" \
  -a "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
```

#### Mixing protocols

```bash
# gRPC vs Jito shredstream-proxy (entries decoded server-side)
latency-bench \
  -e "yellowstone=http://localhost:10000" \
  -e "shredpath=http://10.0.0.2:9090:shredstream"

# Three-way: gRPC vs Richat QUIC vs shredstream
latency-bench \
  -e "richat-grpc=http://localhost:10200" \
  -e "richat-quic=localhost:10101:quic" \
  -e "shredpath=http://10.0.0.2:9090:shredstream" \
  -t 2000

# Soda DEX trade stream over gRPC vs WebSocket
latency-bench \
  -e "soda-grpc=http://localhost:10300:soda" \
  -e "soda-ws=http://localhost:10301:sodaws" \
  -t 1000
```

#### Raw shred capture mixed with gRPC

```bash
# gRPC vs raw UDP shreds (plain bind — port must be free)
latency-bench \
  -e "yellowstone=http://localhost:10000" \
  -e "turbine=0.0.0.0:8001:udp" \
  -t 2000

# gRPC vs AF_PACKET shred sniff alongside the validator on the TVU port,
# filtered by the upstream forwarder's source IP
latency-bench \
  -e "yellowstone=http://localhost:10000" \
  -e "shredpath=10.0.0.2@bond0:10002:pcap" \
  -t 1500
```

#### Shreds-only (no gRPC, per-shred head-to-head)

```bash
# Compare two shred fan-out destinations directly. With no gRPC source,
# matching is by (slot, shred_index) and -t becomes the capture window
# in seconds (default 1000, capped at 300).
latency-bench -t 60 \
  -e "shred-A=10.0.0.2@bond0:10002:pcap" \
  -e "shred-B=10.0.0.2@bond0:9092:pcap"

# Same idea with two plain UDP listeners
latency-bench -t 30 \
  -e "shred-A=0.0.0.0:8001:udp" \
  -e "shred-B=0.0.0.0:8002:udp"
```

## Shred capture (`:udp`, `:pcap`)

Both kinds parse Solana turbine shreds with a minimal header parser
(`src/shred.rs`):

- offset 64: shred variant byte (data vs code classification)
- offset 65: slot (u64 LE)
- offset 73: index (u32 LE)

`:udp` opens a normal UDP socket — easy, but conflicts with anything else
already bound to that port. `:pcap` (Linux-only) opens an `AF_PACKET` socket
with an inline cBPF filter — sniffs the wire alongside the validator without
contending for the port. Needs `CAP_NET_RAW`:

```bash
sudo setcap cap_net_raw=eip ./latency-bench
```

`:pcap` URL is `[SRC@]IFACE:PORT`. **`IFACE` is a network interface name** —
`lo` for loopback, `eth0` / `ens3` / `bond0` etc. for NICs (find yours with
`ip link`). It is **not** an IP address. The optional `SRC@` filters to a
single source IP — useful when the port also receives unrelated turbine
traffic from many peers.

```bash
# Sniff loopback port 43011
-e "name=lo:43011:pcap"

# Sniff a NIC, only packets from a specific upstream
-e "name=10.0.0.2@bond0:10002:pcap"
```

If you pass an IP where `IFACE` belongs you'll get
`interface <ip> not found` — use `:udp` instead if you want to bind by IP.

### Two latency modes

**Mixed mode** (≥1 gRPC source + ≥1 shred source). Per-tx matching is bridged
via slot:

1. gRPC sources record `signature → slot` while they stream.
2. Shred sources record `slot → first_shred_arrival_instant`.
3. After collection, every signature whose slot was observed by a shred source
   gets a synthetic shred-source arrival at the slot's first-shred time.

This answers *"how soon did turbine tell us the slot existed vs how soon did
gRPC deliver the transaction?"* Granularity is one data point per slot.

**Shreds-only mode** (no gRPC sources, ≥2 shred sources). Matching is by full
`(slot, index, shred_type)` — every individual shred is a data point. Granularity
is one data point per shred (~1k–4k shreds/slot). `-t` is reinterpreted as the
capture window in seconds (capped at 300).

This answers *"do these two ports/forwarders deliver the same shreds at the
same time?"* — useful for measuring fan-out jitter or per-shred ordering bias
between two destinations.

Shred sources never drive `-t` completion in mixed mode — the gRPC sources do.

### Where to bind

- **`:udp` on a validator host**: plain `bind()` conflicts with the validator's
  TVU socket. Use `:pcap` instead, or capture out-of-process via tcpdump.
- **`:udp` off-box**: only meaningful if a forwarder (jito relay, doublezero,
  shredstream proxy, etc.) is sending turbine traffic to your IP:port.
- **`:pcap` on a validator host**: works in parallel with the validator. The
  validator and `latency-bench` both see every packet — no contention.

`:udp` sets `SO_RCVBUF = 8 MiB` and `SO_REUSEADDR`. `:pcap` sets
`SO_RCVBUFFORCE = 32 MiB` (requires the same `CAP_NET_RAW`).

## Output

The tool prints color-coded tables showing latency percentiles, win rates, and head-to-head comparisons:

```
  Latency Comparison — 1452 transactions matched by 2+ endpoints

  Latency Distribution

  ┌───────────┬────────┬────────┬────────┬────────┬─────────┐
  │ Endpoint  │   p5   │  p25   │  p50   │  p95   │   p99   │
  ├───────────┼────────┼────────┼────────┼────────┼─────────┤
  │ richat    │ 5.24ms │ 8.72ms │ 12.2ms │ 82.4ms │ 105.3ms │
  ├───────────┼────────┼────────┼────────┼────────┼─────────┤
  │ shredpath │ 0.00µs │ 0.00µs │ 0.00µs │ 0.00µs │  0.00µs │
  └───────────┴────────┴────────┴────────┴────────┴─────────┘

  Head-to-Head (common transactions only)

  • Common transactions: 1452
  • shredpath faster: 1452 (100.0%)
  • richat faster: 0 (0.0%)

  shredpath Advantage over richat

  ┌───────────┬────────┬────────┬────────┬────────┬─────────┐
  │  Metric   │   p5   │  p25   │  p50   │  p95   │   p99   │
  ├───────────┼────────┼────────┼────────┼────────┼─────────┤
  │ Advantage │ 5.24ms │ 8.72ms │ 12.2ms │ 82.4ms │ 105.3ms │
  └───────────┴────────┴────────┴────────┴────────┴─────────┘
```

## How it works

1. Each endpoint runs on a dedicated OS thread with its own tokio runtime to prevent CPU contention between streams.
2. Matching is by transaction signature when ≥1 gRPC source is present (shred sources joined via slot bridge); by `(slot, shred_index)` when only shred sources are present.
3. In sig-matching mode, once the target count of matched transactions is reached, a 3-second grace period collects stragglers. In shreds-only mode, the run ends after a fixed window (`-t` seconds).
4. Latency deltas are computed relative to the earliest arrival for each match.

## License

Apache-2.0
