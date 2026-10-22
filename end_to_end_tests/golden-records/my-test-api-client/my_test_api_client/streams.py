"""Streaming responses: Server-Sent Events and raw bytes, read as they arrive"""

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterator
from types import TracebackType
from typing import Any, BinaryIO, Generic, Self, TypeVar

import httpx
from attrs import define

T = TypeVar("T")

DEFAULT_RETRY_SECONDS = 3.0
"""How long to wait before reconnecting, until the server sets its own delay with a `retry` field"""


@define
class ServerSentEvent(Generic[T]):
    """One event from a `text/event-stream` response"""

    event: str
    """ The event's name, "message" when the server gave none """
    data: T
    """ The event's data, decoded when the API describes it """
    id: str | None = None
    """ The stream's last event ID when this event arrived """
    retry: int | None = None
    """ The reconnection delay in milliseconds, if this event set one """


class StreamError(Exception):
    """Raised when a stream's request is answered with an error status instead of the stream"""

    def __init__(self, status_code: int, content: bytes, parsed: Any) -> None:
        self.status_code = status_code
        self.content = content
        self.parsed = parsed
        """ The response body, parsed as the API describes it for this status, or None """
        super().__init__(f"Unexpected status code: {status_code}")


class _EventParser:
    """Turns the lines of an event stream into events, as the HTML standard defines"""

    def __init__(self) -> None:
        self.last_event_id: str | None = None
        self.retry: int | None = None
        self._event = ""
        self._data: list[str] = []
        self._event_retry: int | None = None

    def feed(self, line: str) -> ServerSentEvent[str] | None:
        """Read one line; return the event it completes, if any"""
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return None
        field, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]
        if field == "event":
            self._event = value
        elif field == "data":
            self._data.append(value)
        elif field == "id" and "\0" not in value:
            self.last_event_id = value
        elif field == "retry" and value and all("0" <= character <= "9" for character in value):
            self.retry = self._event_retry = int(value)
        return None

    def _dispatch(self) -> ServerSentEvent[str] | None:
        event = None
        if self._data:
            event = ServerSentEvent(
                event=self._event or "message",
                data="\n".join(self._data),
                id=self.last_event_id,
                retry=self._event_retry,
            )
        self._event = ""
        self._data = []
        self._event_retry = None
        return event

    def reconnect_headers(self, request: dict[str, Any]) -> dict[str, Any]:
        """The request again, resuming after the last event this stream received"""
        if self.last_event_id is None:
            return request
        headers = dict(request.get("headers") or {})
        headers["Last-Event-ID"] = self.last_event_id
        return {**request, "headers": headers}

    def reconnect_delay(self) -> float:
        return DEFAULT_RETRY_SECONDS if self.retry is None else self.retry / 1000


def _check_status(response: httpx.Response, parse_error: Callable[[httpx.Response], Any]) -> None:
    if not 200 <= response.status_code <= 299:
        raise StreamError(response.status_code, response.content, parse_error(response))


class EventStream(Generic[T]):
    """The events of a `text/event-stream` response, read as they arrive

    Use it as a context manager, which closes the connection, and iterate over it for the events. With reconnection
    on, which is the default for GET requests, a closed or broken connection is reopened after the server's `retry`
    delay, resuming with `Last-Event-ID`, and the stream ends when the server answers a reconnection with 204 No
    Content. Without it, the stream ends when the connection closes.
    """

    def __init__(
        self,
        client: httpx.Client,
        request: dict[str, Any],
        decode: Callable[[ServerSentEvent[str]], ServerSentEvent[T]],
        parse_error: Callable[[httpx.Response], Any],
        *,
        reconnect: bool,
    ) -> None:
        self._client = client
        self._request = request
        self._decode = decode
        self._parse_error = parse_error
        self._reconnect = reconnect
        self._parser = _EventParser()
        self._response: httpx.Response | None = None
        self._lines: Iterator[str] | None = None
        self._entered = False
        self._finished = False

    @property
    def last_event_id(self) -> str | None:
        """The ID of the last event received, to resume from later"""
        return self._parser.last_event_id

    def _open(self, request: dict[str, Any]) -> None:
        response = self._client.send(self._client.build_request(**request), stream=True)
        if response.status_code == 204:
            response.close()
            self._finished = True
            return
        if not 200 <= response.status_code <= 299:
            try:
                response.read()
                _check_status(response, self._parse_error)
            finally:
                response.close()
        self._response = response
        self._lines = response.iter_lines()

    def _close_response(self) -> None:
        if self._response is not None:
            self._response.close()
        self._response = None
        self._lines = None

    def __enter__(self) -> Self:
        self._entered = True
        self._open(self._request)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection; the stream then ends"""
        self._finished = True
        self._close_response()

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> ServerSentEvent[T]:
        if not self._entered:
            raise RuntimeError("Use the event stream in a with statement")
        while not self._finished:
            if self._lines is None:
                # Reconnecting: an unreachable server is retried after the delay, as EventSource does.
                time.sleep(self._parser.reconnect_delay())
                try:
                    self._open(self._parser.reconnect_headers(self._request))
                except httpx.TransportError:
                    pass
                continue
            try:
                line = next(self._lines)
            except (StopIteration, httpx.TransportError):
                self._close_response()
                if not self._reconnect:
                    self._finished = True
                continue
            event = self._parser.feed(line)
            if event is not None:
                return self._decode(event)
        raise StopIteration


class AsyncEventStream(Generic[T]):
    """The asyncio form of `EventStream`: use it with `async with` and `async for`"""

    def __init__(
        self,
        client: httpx.AsyncClient,
        request: dict[str, Any],
        decode: Callable[[ServerSentEvent[str]], ServerSentEvent[T]],
        parse_error: Callable[[httpx.Response], Any],
        *,
        reconnect: bool,
    ) -> None:
        self._client = client
        self._request = request
        self._decode = decode
        self._parse_error = parse_error
        self._reconnect = reconnect
        self._parser = _EventParser()
        self._response: httpx.Response | None = None
        self._lines: AsyncIterator[str] | None = None
        self._entered = False
        self._finished = False

    @property
    def last_event_id(self) -> str | None:
        """The ID of the last event received, to resume from later"""
        return self._parser.last_event_id

    async def _open(self, request: dict[str, Any]) -> None:
        response = await self._client.send(self._client.build_request(**request), stream=True)
        if response.status_code == 204:
            await response.aclose()
            self._finished = True
            return
        if not 200 <= response.status_code <= 299:
            try:
                await response.aread()
                _check_status(response, self._parse_error)
            finally:
                await response.aclose()
        self._response = response
        self._lines = response.aiter_lines()

    async def _close_response(self) -> None:
        if self._response is not None:
            await self._response.aclose()
        self._response = None
        self._lines = None

    async def __aenter__(self) -> Self:
        self._entered = True
        await self._open(self._request)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the connection; the stream then ends"""
        self._finished = True
        await self._close_response()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> ServerSentEvent[T]:
        if not self._entered:
            raise RuntimeError("Use the event stream in an async with statement")
        while not self._finished:
            if self._lines is None:
                # Reconnecting: an unreachable server is retried after the delay, as EventSource does.
                await asyncio.sleep(self._parser.reconnect_delay())
                try:
                    await self._open(self._parser.reconnect_headers(self._request))
                except httpx.TransportError:
                    pass
                continue
            try:
                line = await self._lines.__anext__()
            except (StopAsyncIteration, httpx.TransportError):
                await self._close_response()
                if not self._reconnect:
                    self._finished = True
                continue
            event = self._parser.feed(line)
            if event is not None:
                return self._decode(event)
        raise StopAsyncIteration


class ByteStream:
    """A response's body, read in chunks as it arrives rather than held in memory

    Use it as a context manager, which closes the connection.
    """

    def __init__(
        self, client: httpx.Client, request: dict[str, Any], parse_error: Callable[[httpx.Response], Any]
    ) -> None:
        self._client = client
        self._request = request
        self._parse_error = parse_error
        self._response: httpx.Response | None = None

    @property
    def response(self) -> httpx.Response:
        """The response, for its status code and headers"""
        if self._response is None:
            raise RuntimeError("Use the byte stream in a with statement")
        return self._response

    def __enter__(self) -> Self:
        response = self._client.send(self._client.build_request(**self._request), stream=True)
        if not 200 <= response.status_code <= 299:
            try:
                response.read()
                _check_status(response, self._parse_error)
            finally:
                response.close()
        self._response = response
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._response is not None:
            self._response.close()

    def iter_bytes(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """The body in chunks, as they arrive"""
        return self.response.iter_bytes(chunk_size)

    def write_to(self, file: BinaryIO, chunk_size: int | None = None) -> int:
        """Write the body to a binary file as it arrives, and return the number of bytes written"""
        written = 0
        for chunk in self.iter_bytes(chunk_size):
            file.write(chunk)
            written += len(chunk)
        return written


class AsyncByteStream:
    """The asyncio form of `ByteStream`: use it with `async with`"""

    def __init__(
        self, client: httpx.AsyncClient, request: dict[str, Any], parse_error: Callable[[httpx.Response], Any]
    ) -> None:
        self._client = client
        self._request = request
        self._parse_error = parse_error
        self._response: httpx.Response | None = None

    @property
    def response(self) -> httpx.Response:
        """The response, for its status code and headers"""
        if self._response is None:
            raise RuntimeError("Use the byte stream in an async with statement")
        return self._response

    async def __aenter__(self) -> Self:
        response = await self._client.send(self._client.build_request(**self._request), stream=True)
        if not 200 <= response.status_code <= 299:
            try:
                await response.aread()
                _check_status(response, self._parse_error)
            finally:
                await response.aclose()
        self._response = response
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._response is not None:
            await self._response.aclose()

    def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        """The body in chunks, as they arrive"""
        return self.response.aiter_bytes(chunk_size)

    async def write_to(self, file: BinaryIO, chunk_size: int | None = None) -> int:
        """Write the body to a binary file as it arrives, and return the number of bytes written"""
        written = 0
        async for chunk in self.aiter_bytes(chunk_size):
            file.write(chunk)
            written += len(chunk)
        return written


__all__ = [
    "AsyncByteStream",
    "AsyncEventStream",
    "ByteStream",
    "EventStream",
    "ServerSentEvent",
    "StreamError",
]
