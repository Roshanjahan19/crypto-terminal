# Crypto Terminal

Hellooo this is a real-time crypto market data pipeline I'm building to get hands-on with systems programming with WebSocket ingestion, Redis, and eventually a C++ engine doing the actual number crunching.

## What it does right now


Right now there's one piece working end-to-end: a Go service that connects to Coinbase's live trade feed and pushes clean, normalized data into Redis.

## The Go Ingestor (`go-ingestor/`)

This connects to Coinbase's public WebSocket feed and subscribes to live BTC-USD trades. A few things it handles:

- Stays connected — if the connection drops (which it will eventually), it automatically retries with exponential backoff instead of just dying
- Parses Coinbase's raw JSON into unique internal `tick` struct, so the rest of the system doesn't need to know or care what exchange the data came from
- Sends every tick to Redis two different ways:
  - Into a **Redis Stream** (`ticks:BTC-USD`) — this keeps a full, replayable history
  - Onto a **Redis Pub/Sub channel** (`ticks:live:BTC-USD`) — this is just for anything that wants the live feed right now, no history needed

### Running it

You'll need Go 1.21+ and Redis running somewhere.

```bash
# spin up redis
docker run -d -p 6379:6379 redis

# run the ingestor
cd go-ingestor
go run main.go
```

To check it's actually working:

```bash
docker exec -it <container_id> redis-cli
> subscribe ticks:live:BTC-USD   # watch live ticks come in
> xlen ticks:BTC-USD             # see how many ticks have been stored
```

## What's next

- **A C++ service that computes VWAP** (volume-weighted average price) over a rolling time window. Want to do this with a ring buffer so it's not constantly allocating memory for every tick.
- **A Go WebSocket server for the frontend** that handles backpressure — if a client's connection is slow, coalesce updates instead of letting the queue pile up or dropping the connection.
- **Some kind of terminal-style UI** to actually look at this data, probably with ImGui.

## Some of the reasoning behind the setup

I split Streams and Pub/Sub on purpose — Streams give me replay/history so nothing gets lost if a consumer is offline for a bit, while Pub/Sub is just for "tell me the latest price right now" and doesn't need to remember anything.

Everything's also split into separate services on purpose, rather than one big program — so if the frontend or the calculation engine is slow or crashes, it can't take down the actual connection to Coinbase.