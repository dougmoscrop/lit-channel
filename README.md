# lit-channel

This component allows you declaratively define subscriptions across a shared WebSocket backend. Only one connection is opened across tabs and subscriptions.

<lit-channel topic="foo"></lit-channel>
<lit-channel topic="bar"></lit-channel>

This will establish a single connection to a WebSocket endpoint, defaulting to /api/ws, but configurable via a HEAD element:

```html
<head>
  <meta name="lit-channel-endpoint" content="wss://example.com/ws" />
</head>
```

Using this element requires the `@web/rollup-plugin-import-meta-assets` in your build

## Development

- `npm i`
- `npx playwright install`
- `npm t`

## Debugging

Open: chrome://inspect/#workers to see what `shared-worker.js` is doing
