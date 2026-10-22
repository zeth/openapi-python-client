---
default: minor
---

# Stream Server-Sent Events and binary responses

An endpoint whose successful response is `text/event-stream` described by OpenAPI 3.2's `itemSchema`, or
`application/octet-stream`, now also gets `stream` and `astream` functions that read the response as it arrives instead
of buffering it. The existing functions are unchanged.

For Server-Sent Events, `stream` returns an `EventStream`, used as a context manager and iterated for `ServerSentEvent`
objects. Each alternative of a `oneOf` (or `anyOf`) in the `itemSchema` is a kind of event: `const` on `event` names it,
and `contentMediaType: application/json` with a `contentSchema` on `data` gives the type its data decodes to. Events the
document doesn't name, or whose data isn't described as JSON, keep their data as a string. The stream follows the HTML
standard's event-stream rules: for GET requests it reconnects after a closed or broken connection, waiting the server's
`retry` delay and sending `Last-Event-ID`, and it ends when a reconnection is answered with 204 No Content. Other
methods are never repeated, so their streams end when the connection closes. Lines end only at CR, LF or CRLF, so
Unicode line separators inside JSON data are kept, and a leading byte order mark is dropped. A successful response
that isn't `text/event-stream` raises `StreamError`. Event streams have no read timeout by default, since events can be
far apart; set `timeout` on the stream to replace that, and `reconnect_delay` for the wait before the server sends its
own `retry`. Closing an async stream finishes even if its task is cancelled again meanwhile.

For `application/octet-stream`, `stream` returns a `ByteStream`, used as a context manager, with `iter_bytes` and
`write_to`.

An error status raises `StreamError`, carrying the body parsed as the document describes it for that status. The runtime
for both is written to a `streams.py` module in clients that need it.
