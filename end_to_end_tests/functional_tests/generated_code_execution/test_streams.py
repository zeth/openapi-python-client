import asyncio
import io
import json
import time

import httpx
import pytest

from end_to_end_tests.functional_tests.helpers import (
    with_generated_client_fixture,
    with_generated_code_import,
    with_generated_code_imports,
)

STREAMS_SPEC = """
openapi: '3.2.0'
components:
  schemas:
    Ready:
      type: object
      properties:
        cursor: {"type": "string"}
      required: [cursor]
    Problem:
      type: object
      properties:
        message: {"type": "string"}
      required: [message]
  responses:
    Problem:
      description: A problem
      content:
        application/json:
          schema: {"$ref": "#/components/schemas/Problem"}
paths:
  /events:
    get:
      operationId: watch
      parameters:
        - name: Last-Event-ID
          in: header
          schema: {"type": "string"}
      responses:
        "200":
          description: Events
          content:
            text/event-stream:
              itemSchema:
                oneOf:
                  - type: object
                    properties:
                      event: {"const": "ready"}
                      data:
                        type: string
                        contentMediaType: application/json
                        contentSchema: {"$ref": "#/components/schemas/Ready"}
                    required: [event, data]
                  - type: object
                    properties:
                      event: {"const": "note"}
                      data: {"type": "string"}
                    required: [event, data]
        "204":
          description: The stream is finished
        "404": {"$ref": "#/components/responses/Problem"}
  /runs:
    post:
      operationId: run
      responses:
        "200":
          description: Events
          content:
            text/event-stream:
              itemSchema:
                type: object
                properties:
                  data: {"type": "string"}
                required: [data]
  /files/{name}:
    get:
      operationId: download
      parameters:
        - name: name
          in: path
          required: true
          schema: {"type": "string"}
      responses:
        "200":
          description: The file
          content:
            application/octet-stream: {}
        "404": {"$ref": "#/components/responses/Problem"}
"""


def _client(Client, handler):
    return Client(base_url="http://test", httpx_args={"transport": httpx.MockTransport(handler)})


def _events(body: str, status: int = 200) -> httpx.Response:
    return httpx.Response(status, headers={"content-type": "text/event-stream"}, content=body.encode())


@with_generated_client_fixture(STREAMS_SPEC)
@with_generated_code_imports(".client.Client", ".models.Ready", ".models.Problem", ".streams.StreamError")
@with_generated_code_import(".api.default.watch.stream", alias="watch")
@with_generated_code_import(".api.default.watch.astream", alias="awatch")
@with_generated_code_import(".api.default.run.stream", alias="run")
class TestEventStreams:
    def test_decodes_declared_events_and_passes_others_through(self, Client, Ready, watch):
        body = (
            ": a comment\n"
            'event: ready\nid: 1\ndata: {"cursor": "c1"}\n\n'
            "event: note\ndata:  two leading spaces keep one\ndata: second line\n\n"
            "event: unknown\ndata: kept as text\n\n"
            "data: no event name\n\n"
        )
        client = _client(Client, lambda request: _events(body))
        with watch(client=client, reconnect=False) as events:
            received = list(events)
        assert [(event.event, event.data, event.id) for event in received] == [
            ("ready", Ready(cursor="c1"), "1"),
            ("note", " two leading spaces keep one\nsecond line", "1"),
            ("unknown", "kept as text", "1"),
            ("message", "no event name", "1"),
        ]

    def test_reconnects_with_the_last_event_id_until_told_the_stream_is_finished(self, Client, Ready, watch):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:
                return _events('retry: 0\nid: 7\nevent: ready\ndata: {"cursor": "c7"}\n\n')
            return httpx.Response(204)

        with watch(client=_client(Client, handler)) as events:
            received = list(events)
        assert [event.data for event in received] == [Ready(cursor="c7")]
        assert received[0].retry == 0
        assert events.last_event_id == "7"
        assert "last-event-id" not in requests[0].headers
        assert requests[1].headers["last-event-id"] == "7"

    def test_reconnects_after_a_dropped_connection(self, Client, Ready, watch):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(request)
            if len(attempts) == 1:
                return _events('retry: 0\nevent: ready\ndata: {"cursor": "a"}\n\n')
            if len(attempts) == 2:
                raise httpx.ConnectError("daemon restarting")
            if len(attempts) == 3:
                return _events('event: ready\ndata: {"cursor": "b"}\n\n')
            return httpx.Response(204)

        with watch(client=_client(Client, handler)) as events:
            assert [event.data for event in events] == [Ready(cursor="a"), Ready(cursor="b")]
        assert len(attempts) == 4

    def test_an_unsafe_request_is_never_repeated(self, Client, run):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return _events("retry: 0\ndata: started\n\n")

        with run(client=_client(Client, handler)) as events:
            assert [event.data for event in events] == ["started"]
        assert len(requests) == 1

    def test_error_status_raises_with_the_declared_body(self, Client, Problem, StreamError, watch):
        client = _client(Client, lambda request: httpx.Response(404, json={"message": "gone"}))
        with pytest.raises(StreamError) as raised:
            with watch(client=client):
                pass
        assert raised.value.status_code == 404
        assert raised.value.parsed == Problem(message="gone")

    def test_finished_before_it_starts(self, Client, watch):
        with watch(client=_client(Client, lambda request: httpx.Response(204))) as events:
            assert list(events) == []

    def test_iterating_outside_a_with_statement_is_refused(self, Client, watch):
        with pytest.raises(RuntimeError):
            next(iter(watch(client=_client(Client, lambda request: _events("")))))

    def test_lines_end_only_at_cr_and_lf(self, Client, Ready, watch):
        # A leading byte order mark is dropped, and U+2028 inside JSON does not end a line.
        body = '\ufeffevent: ready\r\ndata: {"cursor": "a\u2028b"}\r\rdata: next\r\n\n'
        with watch(client=_client(Client, lambda request: _events(body)), reconnect=False) as events:
            received = list(events)
        assert [event.data for event in received] == [Ready(cursor="a\u2028b"), "next"]

    def test_a_response_that_is_not_an_event_stream_is_refused(self, Client, StreamError, watch):
        client = _client(Client, lambda request: httpx.Response(200, json={"not": "events"}))
        with pytest.raises(StreamError, match="Expected a text/event-stream response"):
            with watch(client=client):
                pass

    def test_reading_has_no_timeout_unless_one_is_set(self, Client, watch):
        timeouts = []

        def handler(request: httpx.Request) -> httpx.Response:
            timeouts.append(request.extensions["timeout"])
            return _events("data: x\n\n")

        client = Client(
            base_url="http://test", timeout=httpx.Timeout(5.0), httpx_args={"transport": httpx.MockTransport(handler)}
        )
        with watch(client=client, reconnect=False) as events:
            list(events)
        stream = watch(client=client, reconnect=False)
        stream.timeout = httpx.Timeout(1.0)
        with stream as events:
            list(events)
        assert (timeouts[0]["connect"], timeouts[0]["read"]) == (5.0, None)
        assert timeouts[1]["read"] == 1.0

    def test_reconnect_delay_applies_until_the_server_sets_one(self, Client, watch, monkeypatch):
        slept = []
        monkeypatch.setattr(time, "sleep", slept.append)
        responses = iter([_events("data: a\n\n"), _events("retry: 250\ndata: b\n\n"), httpx.Response(204)])
        stream = watch(client=_client(Client, lambda request: next(responses)))
        stream.reconnect_delay = 0.5
        with stream as events:
            assert [event.data for event in events] == ["a", "b"]
        assert slept == [0.5, 0.25]

    def test_asyncio(self, Client, Ready, awatch):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:
                return _events('retry: 0\nid: 3\nevent: ready\ndata: {"cursor": "c3"}\n\n')
            return httpx.Response(204)

        async def read():
            async with awatch(client=_client(Client, handler)) as events:
                return [event async for event in events]

        received = asyncio.run(read())
        assert [event.data for event in received] == [Ready(cursor="c3")]
        assert requests[1].headers["last-event-id"] == "3"


@with_generated_client_fixture(STREAMS_SPEC)
@with_generated_code_imports(".client.Client", ".models.Problem", ".streams.StreamError")
@with_generated_code_import(".api.default.download.stream", alias="download")
@with_generated_code_import(".api.default.download.astream", alias="adownload")
class TestByteStreams:
    def test_reads_the_body_in_chunks(self, Client, download):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.raw_path == b"/files/a%20b"
            return httpx.Response(200, headers={"content-type": "image/png"}, content=iter([b"ab", b"cd"]))

        with download("a b", client=_client(Client, handler)) as body:
            assert body.response.headers["content-type"] == "image/png"
            assert b"".join(body.iter_bytes()) == b"abcd"

    def test_writes_to_a_file(self, Client, download):
        client = _client(Client, lambda request: httpx.Response(200, content=b"x" * 10))
        target = io.BytesIO()
        with download("f", client=client) as body:
            assert body.write_to(target) == 10
        assert target.getvalue() == b"x" * 10

    def test_error_status_raises_with_the_declared_body(self, Client, Problem, StreamError, download):
        client = _client(Client, lambda request: httpx.Response(404, json={"message": "missing"}))
        with pytest.raises(StreamError) as raised:
            with download("f", client=client):
                pass
        assert raised.value.parsed == Problem(message="missing")
        assert json.loads(raised.value.content) == {"message": "missing"}

    def test_asyncio(self, Client, adownload):
        async def read():
            target = io.BytesIO()
            async with adownload(
                "f", client=_client(Client, lambda request: httpx.Response(200, content=b"xyz"))
            ) as body:
                await body.write_to(target)
            return target.getvalue()

        assert asyncio.run(read()) == b"xyz"
