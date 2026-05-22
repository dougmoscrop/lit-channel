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

## Subscription Readiness and Control Frames

Servers can confirm that a subscription is established by sending a topic-scoped `subscribed` frame after accepting the client subscribe request:

```json
{ "type": "subscribed", "topic": "orders" }
```

Resume-aware servers can include ACK metadata, which lit-channel preserves on bridge and element events:

```json
{
  "type": "subscribed",
  "topic": "orders",
  "resume": {
    "accepted": true,
    "startSeq": 43,
    "serverCursor": "42",
    "replayEligible": true
  }
}
```

`PubSubBridge` exposes control frames separately from data callbacks. `subscribe(topic, callback)` still delivers only `type: "message"` payloads.

```js
const unsubscribe = bridge.subscribe('orders', (payload, topic) => {
  console.log('message', topic, payload)
})

bridge.addEventListener('subscribed', (event) => {
  console.log('ready', event.detail.topic, event.detail.resume)
})

await bridge.waitForSubscribed('orders', { timeout: 5000 })
```

`waitForSubscribed(topic, options)` resolves immediately when the latest ACK for that active topic is already known, or waits for the next matching `subscribed` frame. Pass `options.signal` to cancel or `options.timeout` to reject after a caller-defined deadline. A matching topic-scoped `error` frame rejects pending waiters. Topicless `error` frames are emitted as global bridge `error` and `control` events, but do not reject every subscription waiter.

The bridge emits these events with the original frame fields in `event.detail`:

- `control`
- `subscribed`
- `error`
- `replay-gap`
- `replay-complete`

Known subscribed ACK state is cleared when the last local listener for a topic unsubscribes. Active topic ACK state is also invalidated after a reconnect, then a fresh `subscribed` event is emitted when the server ACKs the resubscription.

`<lit-channel>` dispatches DOM events for the same control surface:

- `lit-channel-subscribed` with `detail: { topic, resume }`
- `lit-channel-error` with `detail: { frame }`
- `lit-channel-replay-gap` with `detail: { frame }`
- `lit-channel-replay-complete` with `detail: { frame }`
- `lit-channel-control` with `detail: { frame }`

All element events bubble and are composed. `lit-channel-message` remains data-only and is not fired for `subscribed`, `error`, `replay-gap`, or `replay-complete` frames.

## SharedWorker Upgrades

When the SharedWorker script URL changes, call `upgradeWorker()` with the new fingerprinted URL instead of reloading the page:

```js
import { SharedSocket } from 'lit-channel'

const socket = new SharedSocket({ workerUrl: '/assets/lit-channel/shared-worker-a1b2c3.js' })
await socket.connect()

await socket.upgradeWorker('/assets/lit-channel/shared-worker-d4e5f6.js')
```

For singleton `<lit-channel>` usage, `reloadSharedWorkers(workerUrl)` upgrades every active `SharedSocket` instance in the current page:

```js
import { reloadSharedWorkers } from 'lit-channel'

await reloadSharedWorkers('/assets/lit-channel/shared-worker-d4e5f6.js')
```

During an upgrade, `SharedSocket` asks the old worker for its active topics and resume cursors, starts a new worker from the next URL, sends the snapshot in the new worker config, queues page-originated messages while the swap is in progress, then emits the normal `reconnected` event so `PubSubBridge` can resubscribe with its latest cursors. The old worker keeps already accepted work alive until the handoff completes.

Lossless inbound upgrades require the resume/replay protocol above and a server that honors resume cursors. Without server replay, lit-channel preserves client-side queued messages during the swap, but a server message sent between two WebSocket subscriptions cannot be reconstructed by the browser alone.

Using this element requires the `@web/rollup-plugin-import-meta-assets` in your build

## Backlog

### Split the package into lit-channel, shared-socket, and shared-worker-service

Split the current package into three libraries so `<lit-channel>` becomes a thin LitElement integration over reusable shared-worker and socket infrastructure.

- `lit-channel`: keep the public web component, singleton configuration, and browser-facing events. It should consume `shared-socket` instead of owning worker and socket lifecycle code.
- `shared-socket`: move the shared WebSocket client, pub/sub bridge, resume/replay support, BroadcastChannel fallback, worker upgrade/reconnect behavior, and related tests here.
- `shared-worker-service`: introduce a SharedWorker host wrapper with lifecycle hooks for registering services to run inside a SharedWorker. `shared-socket` should be hosted inside this platform as the first service.

Acceptance notes:

- Preserve current `lit-channel` imports or provide a migration path for `SharedSocket`, `PubSubBridge`, and `reloadSharedWorkers`.
- Define package boundaries, build outputs, and test ownership before moving code.
- Keep SharedWorker upgrade and resume behavior covered across package boundaries.

## Development

- `npm i`
- `npx playwright install`
- `npm t`

## Debugging

Open: chrome://inspect/#workers to see what `shared-worker.js` is doing
