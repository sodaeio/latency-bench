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

```bash
# Yellowstone vs shredstream
latency-bench \
  -e "yellowstone=http://localhost:10000" \
  -e "shredpath=http://10.0.0.2:9090:shredstream"

# Three-way: gRPC vs QUIC vs shredstream
latency-bench \
  -e "richat-grpc=http://localhost:10200" \
  -e "richat-quic=localhost:10101:quic" \
  -e "shredpath=http://10.0.0.2:9090:shredstream" \
  -t 2000

# gRPC vs raw turbine shreds (shredwatch-style UDP capture)
latency-bench \
  -e "yellowstone=http://localhost:10000" \
  -e "turbine=0.0.0.0:8001:udp" \
  -t 2000
```

## Raw UDP shred capture (`:udp`)

The `:udp` suffix turns one endpoint into a shredwatch-style UDP listener. The
URL is `BIND_IP:PORT` — the tool binds the socket and parses each datagram with
a minimal Solana shred header parser (`src/shred.rs`):

- offset 64: shred variant byte (data vs code classification)
- offset 65: slot (u64 LE)
- offset 73: index (u32 LE)

For every slot, only the **first** shred's arrival time is kept.

### Latency model

Raw shreds carry a fragment of a transaction, not the signature, so per-tx
matching is reconstructed via the gRPC sources' slot field:

1. Yellowstone / QUIC / shredstream sources record `signature → slot` while they
   stream.
2. UDP sources record `slot → first_shred_arrival_instant`.
3. After collection ends, every signature whose slot was observed by a UDP
   source is given a synthetic UDP arrival at the slot's first-shred time.

This answers: *"how soon did turbine tell us the slot existed, vs how soon did
the gRPC stream deliver the transaction?"* It does not reassemble FEC sets or
recover transactions from shreds.

UDP endpoints do not count toward the `-t` target — only the live gRPC-style
endpoints drive completion.

### Where to bind

- **On a validator host**: a plain `bind()` will conflict with the validator's
  TVU socket. Either run on a port that mirrors traffic to you (port mirror,
  jito/doublezero relay forwarding) or capture out-of-process via tcpdump.
  Native AF_PACKET capture inside latency-bench is not yet implemented.
- **Off-box**: only meaningful if a forwarder (jito relay, doublezero, custom
  shred forwarder) is sending turbine traffic to your IP:port.

The socket is set to `SO_RCVBUF = 8 MiB` and `SO_REUSEADDR`. No root required.

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
2. Transactions are matched across endpoints by signature. UDP sources are matched indirectly via slot (see *Raw UDP shred capture*).
3. Once the target count of matched transactions is reached, a 3-second grace period collects stragglers.
4. Latency deltas are computed relative to the earliest arrival for each transaction.

## License

Apache-2.0
