# latency-bench

Compare latency across Solana streaming endpoints side-by-side. Subscribes to multiple endpoints in parallel, matches transactions by signature, and reports which endpoint delivered each transaction first.

## Supported protocols

| Suffix | Protocol | Description |
|---|---|---|
| *(none)* | Yellowstone gRPC | Standard geyser-compatible gRPC (Yellowstone, Richat, etc.) |
| `:shredstream` | Shredstream | Raw shred/entry stream (e.g. ShredPath, Jito shredstream-proxy) |
| `:quic` | QUIC | Richat QUIC transport |
| `:soda` | Soda gRPC | Soda stream service (protobuf/gRPC) |
| `:sodaws` | Soda WebSocket | Soda stream service (WebSocket) |

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
```

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
2. Transactions are matched across endpoints by signature.
3. Once the target count of matched transactions is reached, a 3-second grace period collects stragglers.
4. Latency deltas are computed relative to the earliest arrival for each transaction.

## License

Apache-2.0
