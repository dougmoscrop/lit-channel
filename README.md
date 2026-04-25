# lit-channel

This component allows you declaratively define subscriptions across a shared WebSocket backend. Only one connection is opened across tabs and subscriptions.

<lit-channel name="foo"></lit-channel>
<lit-channel name="bar"></lit-channel>

This will establish a single connection to a WebSocket endpoint, defaulting to /api/ws, but configurable via a HEAD element:

```html
<head>
  <meta name="lit-channel-endpoint" content="wss://example.com/ws" />
</head>
```

If the bundled shared worker is served from a CDN, you can override just the worker script URL so the page loads it from the app origin instead:

```html
<head>
  <meta name="lit-channel-worker-url" content="/assets/lit-channel/shared-worker.js" />
</head>
```

To pass an optional Bearer token during the WebSocket handshake, provide a token meta tag:

```html
<head>
  <meta name="lit-channel-auth-token" content="Bearer YOUR_TOKEN" />
</head>
```

The token is normalized (the `Bearer ` prefix is optional) and sent as a WebSocket subprotocol pair: `['bearer', token]`.

## Resume and Replay

lit-channel has optional resume/replay protocol support for servers that expose ordered stream metadata. It is off by default, so legacy servers and consumers keep the existing subscribe, publish, unsubscribe, ping, and pong behavior.

`sessionId` is optional. If you enable resume without passing one, lit-channel generates a session ID for the bridge instance.

Enable it before any `<lit-channel>` element connects:

```js
import { configureLitChannel } from 'lit-channel'

configureLitChannel({
  resumeEnabled: true,
  sessionId: sessionStorage.getItem('lit-channel-session-id') ?? undefined,
  getResumeCursor(topic) {
    const cursor = localStorage.getItem(`lit-channel-cursor:${topic}`)
    return cursor ? { streamSeq: Number(cursor), cursor } : undefined
  },
})
```

You can also configure the bridge directly:

```js
import { PubSubBridge, SharedSocket } from 'lit-channel'

const socket = new SharedSocket()
await socket.connect()

const bridge = new PubSubBridge(socket, {
  resumeEnabled: true,
  sessionId: 'browser-session-123',
})
```

When `resumeEnabled` is true and a cursor is known for a topic, subscribe frames include a resume payload:

```json
{
  "type": "subscribe",
  "topic": "orders",
  "resume": {
    "streamSeq": 42,
    "cursor": "42",
    "sessionId": "browser-session-123"
  }
}
```

When no cursor is known, subscribe frames keep the legacy shape:

```json
{ "type": "subscribe", "topic": "orders" }
```

Servers can opt into ack and dedupe by sending runtime metadata on inbound payloads:

```json
{
  "type": "message",
  "topic": "orders",
  "payload": {
    "id": "order-1",
    "__rt": {
      "streamSeq": 43,
      "eventId": "orders-43"
    }
  }
}
```

If `payload.__rt.streamSeq` is a valid non-negative integer and advances the topic cursor, lit-channel sends:

```json
{
  "type": "ack",
  "topic": "orders",
  "streamSeq": 43,
  "cursor": "43",
  "sessionId": "browser-session-123"
}
```

If `payload.__rt.eventId` is present, duplicate event IDs are suppressed per topic before listener delivery. The dedupe cache is a fixed-size FIFO with a default limit of 1024 event IDs per topic. Override it with `eventIdDedupeLimit` when constructing `PubSubBridge` or calling `configureLitChannel`.

Both transports support the same resume and ack frames:

- SharedWorker transport forwards subscribe resume payloads and ack frames, and replays the best known cursor for active topics after reconnect.
- BroadcastChannel leader-election fallback forwards subscribe resume payloads and ack frames, and replays the best known cursor for active topics after reconnect.

Consumers currently patching `node_modules/lit-channel` can remove those overrides and configure first-class resume support through `configureLitChannel` or `new PubSubBridge(socket, options)`. If you persisted cursors in the prototype, return them from `getResumeCursor(topic)` using `{ streamSeq, cursor }`.

Using this element requires the `@web/rollup-plugin-import-meta-assets` in your build

## Development

- `npm i`
- `npx playwright install`
- `npm t`

## Debugging

Open: chrome://inspect/#workers to see what `shared-worker.js` is doing
