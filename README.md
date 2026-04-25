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

Using this element requires the `@web/rollup-plugin-import-meta-assets` in your build

## Development

- `npm i`
- `npx playwright install`
- `npm t`

## Debugging

Open: chrome://inspect/#workers to see what `shared-worker.js` is doing
