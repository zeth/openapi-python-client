__all__ = ["HTTPStatusPattern", "Response", "Responses", "StreamEvent", "response_from_data"]

from collections.abc import Iterator
from typing import TypedDict

from attrs import define, field

from openapi_python_client import strings
from openapi_python_client.parser.properties.schemas import get_reference_simple_name, parse_reference_path

from .. import Config
from .. import schema as oai
from ..schema.untrusted_string import UntrustedString
from ..strings import PythonIdentifier
from .errors import ParseError, PropertyError
from .properties import AnyProperty, Property, Schemas, property_from_data


@define
class Responses:
    patterns: list["Response"]
    default: "Response | None"

    def __iter__(self) -> Iterator["Response"]:
        yield from self.patterns
        if self.default:
            yield self.default

    def __len__(self) -> int:
        return len(self.patterns) + (1 if self.default else 0)


class _ResponseSource(TypedDict):
    """What data should be pulled from the httpx Response object"""

    attribute: str
    return_type: str


JSON_SOURCE = _ResponseSource(attribute="response.json()", return_type="Any")
BYTES_SOURCE = _ResponseSource(attribute="response.content", return_type="bytes")
TEXT_SOURCE = _ResponseSource(attribute="response.text", return_type="str")
NONE_SOURCE = _ResponseSource(attribute="None", return_type="None")


class HTTPStatusPattern:
    """Status code patterns come in three flavors, in order of precedence:
    1. Specific status codes, such as 200. This is represented by `min` and `max` being the same.
    2. Ranges of status codes, such as 2XX. This is represented by `min` and `max` being different.
    3. The special `default` status code, which is used when no other status codes match. `range` is `None` in this case.

    https://github.com/openapi-generators/openapi-python-client/blob/61b6c54994e2a6285bb422ee3b864c45b5d88c15/openapi_python_client/schema/3.1.0.md#responses-object
    """

    pattern: str
    range: tuple[int, int] | None

    def __init__(self, *, pattern: str, code_range: tuple[int, int] | None):
        """Initialize with a range of status codes or None for the default case."""
        self.pattern = pattern
        self.range = code_range

    @staticmethod
    def parse(pattern: UntrustedString) -> "HTTPStatusPattern | ParseError":
        """Parse a status code pattern such as 2XX or 404"""
        untrusted = pattern.get_untrusted_value()
        if untrusted == "default":
            return HTTPStatusPattern(pattern=untrusted, code_range=None)

        if len(untrusted) == 3 and untrusted.endswith("XX") and untrusted[0].isdigit():
            first_digit = int(untrusted[0])
            return HTTPStatusPattern(pattern=untrusted, code_range=(first_digit * 100, first_digit * 100 + 99))

        try:
            code = int(untrusted)
            return HTTPStatusPattern(pattern=untrusted, code_range=(code, code))
        except ValueError:
            return ParseError(
                detail=(
                    f"Invalid response status code pattern: {untrusted}, response will be omitted from generated client"
                )
            )

    def is_range(self) -> bool:
        """Check if this is a range of status codes, such as 2XX"""
        return self.range is not None and self.range[0] != self.range[1]

    def __lt__(self, other: "HTTPStatusPattern") -> bool:
        """Compare two HTTPStatusPattern objects based on the order they should be applied in"""
        if self.range is None:
            return False  # Default gets applied last
        if other.range is None:
            return True  # Other is default, so this one gets applied first

        # Specific codes appear before ranges
        if self.is_range() and not other.is_range():
            return False
        if not self.is_range() and other.is_range():
            return True

        # Order specific codes numerically
        return self.range[0] < other.range[0]

    def __eq__(self, other: object) -> bool:  # pragma: no cover
        if not isinstance(other, HTTPStatusPattern):
            return False
        return self.range == other.range

    def __hash__(self) -> int:  # pragma: no cover
        return hash(self.range)

    def __repr__(self) -> str:  # pragma: no cover
        return f"<HTTPStatusPattern {self.pattern}>"


@define(order=False)
class Response:
    """Describes a single response for an endpoint"""

    status_code: HTTPStatusPattern
    prop: Property
    source: _ResponseSource
    data: oai.Response | oai.Reference  # Original data which created this response, useful for custom templates
    events: "list[StreamEvent] | None" = field(default=None, kw_only=True)
    """The events of a `text/event-stream` response described by `itemSchema`, or None for other responses"""
    byte_stream: bool = field(default=False, kw_only=True)
    """Whether this is an `application/octet-stream` response, which can be streamed"""

    def is_default(self) -> bool:
        return self.status_code.range is None

    def __lt__(self, other: "Response") -> bool:
        """Compare two responses based on the order in which they should be applied in"""
        return self.status_code < other.status_code


@define
class StreamEvent:
    """One kind of event in a `text/event-stream` response"""

    name: UntrustedString | None
    """The event name this kind matches, or None to match any event"""
    prop: Property | None
    """The type the event's JSON data decodes to, or None to keep the data as a string"""


EVENT_STREAM = "text/event-stream"
BYTE_STREAM = "application/octet-stream"


def _is_json(content_type: UntrustedString | None, config: Config) -> bool:
    if content_type is None:
        return False
    parsed = strings.get_content_type(content_type.get_untrusted_value(), config)
    return parsed is not None and (parsed == "application/json" or parsed.endswith("+json"))


def _stream_events(
    *,
    item_schema: oai.Schema | oai.Reference,
    schemas: Schemas,
    parent_name: str,
    config: Config,
) -> tuple[list[StreamEvent] | ParseError, Schemas]:
    """Read the kinds of event an OpenAPI 3.2 `itemSchema` describes for a `text/event-stream` response

    Each alternative of a `oneOf` or `anyOf` is one kind of event, otherwise the schema itself is. A kind fixes its
    event name with `const` on `event`, and describes JSON data with `contentMediaType` and `contentSchema` on `data`.
    """
    if isinstance(item_schema, oai.Reference):
        return ParseError(data=item_schema, detail="A $ref itemSchema for an event stream is not supported"), schemas
    alternatives = item_schema.oneOf or item_schema.anyOf or [item_schema]
    events: list[StreamEvent] = []
    for alternative in alternatives:
        if isinstance(alternative, oai.Reference):
            return ParseError(data=alternative, detail="A $ref event in an event stream is not supported"), schemas
        properties = alternative.properties or {}
        event_schema = properties.get(UntrustedString("event"))
        name = None
        if isinstance(event_schema, oai.Schema) and isinstance(event_schema.const, str):
            name = UntrustedString(event_schema.const)
        data_schema = properties.get(UntrustedString("data"))
        prop = None
        if (
            isinstance(data_schema, oai.Schema)
            and data_schema.contentSchema is not None
            and _is_json(data_schema.contentMediaType, config)
        ):
            event_prop, schemas = property_from_data(
                name=UntrustedString(f"{name.get_untrusted_value() if name else 'event'}_data"),
                required=True,
                data=data_schema.contentSchema,
                schemas=schemas,
                parent_name=parent_name,
                config=config,
            )
            if isinstance(event_prop, PropertyError):
                return ParseError(data=data_schema, detail=event_prop.detail), schemas
            prop = event_prop
        events.append(StreamEvent(name=name, prop=prop))
    return events, schemas


def _event_stream_response(
    *,
    status_code: HTTPStatusPattern,
    response_name: UntrustedString,
    data: oai.Response,
    schemas: Schemas,
    parent_name: str,
    config: Config,
) -> tuple[Response | ParseError | None, Schemas]:
    """The response's `text/event-stream` content described by `itemSchema`, or None if it has none"""
    for content_type, media_type in (data.content or {}).items():
        if (
            media_type is None
            or media_type.item_schema is None
            or (strings.get_content_type(content_type.get_untrusted_value(), config) != EVENT_STREAM)
        ):
            continue
        events, schemas = _stream_events(
            item_schema=media_type.item_schema, schemas=schemas, parent_name=parent_name, config=config
        )
        if isinstance(events, ParseError):
            return events, schemas
        response = empty_response(status_code=status_code, response_name=response_name, config=config, data=data)
        response.events = events
        return response, schemas
    return None, schemas


def _source_by_content_type(content_type: str, config: Config) -> _ResponseSource | None:
    parsed_content_type = strings.get_content_type(content_type, config)
    if parsed_content_type is None:
        return None

    if parsed_content_type.startswith("text/"):
        return TEXT_SOURCE

    known_content_types = {
        "application/json": JSON_SOURCE,
        "application/octet-stream": BYTES_SOURCE,
    }
    source = known_content_types.get(parsed_content_type)
    if source is None and parsed_content_type.endswith("+json"):
        # Implements https://www.rfc-editor.org/rfc/rfc6838#section-4.2.8 for the +json suffix
        source = JSON_SOURCE
    return source


def empty_response(
    *,
    status_code: HTTPStatusPattern,
    response_name: UntrustedString,
    config: Config,
    data: oai.Response | oai.Reference,
) -> Response:
    """Return an untyped response, for when no response type is defined"""
    return Response(
        data=data,
        status_code=status_code,
        prop=AnyProperty(
            name=response_name,
            default=None,
            required=True,
            python_name=PythonIdentifier(value=response_name, prefix=config.field_prefix),
            description=data.description if isinstance(data, oai.Response) else None,
            example=None,
        ),
        source=NONE_SOURCE,
    )


def response_from_data(  # noqa: PLR0911
    *,
    status_code: HTTPStatusPattern,
    data: oai.Response | oai.Reference,
    schemas: Schemas,
    responses: dict[UntrustedString, oai.Response | oai.Reference],
    parent_name: str,
    config: Config,
) -> tuple[Response | ParseError, Schemas]:
    """Generate a Response from the OpenAPI dictionary representation of it"""

    response_name = UntrustedString(f"response_{status_code.pattern}")
    if isinstance(data, oai.Reference):
        ref_path = parse_reference_path(data.ref)
        if isinstance(ref_path, ParseError):  # pragma: no cover
            return ref_path, schemas
        if not ref_path.startswith("/components/responses/"):  # pragma: no cover
            return ParseError(
                data=data, detail=f"$ref to {data.ref.get_untrusted_value()} not allowed in responses"
            ), schemas
        resp_data = responses.get(get_reference_simple_name(ref_path), None)
        if not resp_data:  # pragma: no cover
            return ParseError(data=data, detail=f"Could not find reference: {data.ref.get_untrusted_value()}"), schemas
        if not isinstance(resp_data, oai.Response):  # pragma: no cover
            return ParseError(data=data, detail="Top-level $ref inside components/responses is not supported"), schemas
        data = resp_data

    content = data.content
    if not content:
        return (
            empty_response(
                status_code=status_code,
                response_name=response_name,
                config=config,
                data=data,
            ),
            schemas,
        )

    event_stream, schemas = _event_stream_response(
        status_code=status_code,
        response_name=response_name,
        data=data,
        schemas=schemas,
        parent_name=parent_name,
        config=config,
    )
    if event_stream is not None:
        return event_stream, schemas

    for content_type, media_type in content.items():
        source = _source_by_content_type(content_type.get_untrusted_value(), config)
        if source is not None:
            schema_data = media_type.media_type_schema
            break
    else:
        return (
            ParseError(
                data=data,
                detail=f"Unsupported content_type {', '.join(key.get_untrusted_value() for key in content)}",
            ),
            schemas,
        )

    byte_stream = source is BYTES_SOURCE
    if schema_data is None:
        response = empty_response(
            status_code=status_code,
            response_name=response_name,
            config=config,
            data=data,
        )
        response.byte_stream = byte_stream
        return response, schemas

    prop, schemas = property_from_data(
        name=response_name,
        required=True,
        data=schema_data,
        schemas=schemas,
        parent_name=parent_name,
        config=config,
    )

    if isinstance(prop, PropertyError):
        return prop, schemas

    return Response(status_code=status_code, prop=prop, source=source, data=data, byte_stream=byte_stream), schemas
