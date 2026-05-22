# Changelog

## 1.3.0 - 2026-05-22

- Forward topic-scoped `subscribed`, `replay-gap`, `replay-complete`, and `error` control frames through SharedWorker and BroadcastChannel transports.
- Add `PubSubBridge` control events and `waitForSubscribed(topic, options)` for deterministic subscription readiness.
- Add `<lit-channel>` control DOM events, including `lit-channel-subscribed`, while keeping `lit-channel-message` data-only.
- Document subscription ACK, replay, and error event contracts.
