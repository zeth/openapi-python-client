from unittest.mock import MagicMock

import pytest

import openapi_python_client.schema as oai
from openapi_python_client import UntrustedString
from openapi_python_client.parser import responses
from openapi_python_client.parser.errors import ParseError, PropertyError
from openapi_python_client.parser.properties import Schemas
from openapi_python_client.parser.responses import (
    JSON_SOURCE,
    NONE_SOURCE,
    HTTPStatusPattern,
    Response,
    response_from_data,
)

MODULE_NAME = "openapi_python_client.parser.responses"


def test_response_from_data_no_content(any_property_factory):
    data = oai.Response.model_construct(description="")

    response, _schemas = response_from_data(
        status_code=HTTPStatusPattern(pattern="200", code_range=(200, 200)),
        data=data,
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=MagicMock(),
    )

    assert response == Response(
        status_code=HTTPStatusPattern(pattern="200", code_range=(200, 200)),
        prop=any_property_factory(
            name="response_200",
            default=None,
            required=True,
            description="",
        ),
        source=NONE_SOURCE,
        data=data,
    )


status_code = HTTPStatusPattern(pattern="200", code_range=(200, 200))


def test_response_from_data_unsupported_content_type():
    data = oai.Response.model_construct(description="", content={oai.UntrustedString("blah"): None})
    config = MagicMock()
    config.content_type_overrides = {}
    response, _schemas = response_from_data(
        status_code=status_code,
        data=data,
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=config,
    )

    assert response == ParseError(data=data, detail="Unsupported content_type blah")


def test_response_from_data_no_content_schema(any_property_factory):
    data = oai.Response.model_construct(
        description="",
        content={oai.UntrustedString("application/vnd.api+json; version=2.2"): oai.MediaType.model_construct()},
    )
    config = MagicMock()
    config.content_type_overrides = {}
    response, _schemas = response_from_data(
        status_code=status_code,
        data=data,
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=config,
    )

    assert response == Response(
        status_code=status_code,
        prop=any_property_factory(
            name="response_200",
            default=None,
            required=True,
            description=data.description,
        ),
        source=NONE_SOURCE,
        data=data,
    )


def test_response_from_data_property_error(mocker):
    property_from_data = mocker.patch.object(responses, "property_from_data", return_value=(PropertyError(), Schemas()))
    data = oai.Response.model_construct(
        description="",
        content={oai.UntrustedString("application/json"): oai.MediaType.model_construct(media_type_schema="something")},
    )
    config = MagicMock()
    config.content_type_overrides = {}

    response, _schemas = responses.response_from_data(
        status_code=HTTPStatusPattern(pattern="400", code_range=(400, 400)),
        data=data,
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=config,
    )

    assert response == PropertyError()
    property_from_data.assert_called_once_with(
        name="response_400",
        required=True,
        data="something",
        schemas=Schemas(),
        parent_name="parent",
        config=config,
    )


def test_response_from_data_property(mocker, any_property_factory):
    prop = any_property_factory()
    property_from_data = mocker.patch.object(responses, "property_from_data", return_value=(prop, Schemas()))
    data = oai.Response.model_construct(
        description="",
        content={oai.UntrustedString("application/json"): oai.MediaType.model_construct(media_type_schema="something")},
    )
    config = MagicMock()
    config.content_type_overrides = {}
    status_code = HTTPStatusPattern(pattern="400", code_range=(400, 400))

    response, _schemas = responses.response_from_data(
        status_code=status_code,
        data=data,
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=config,
    )

    assert response == responses.Response(
        status_code=status_code,
        prop=prop,
        source=JSON_SOURCE,
        data=data,
    )
    property_from_data.assert_called_once_with(
        name="response_400",
        required=True,
        data="something",
        schemas=Schemas(),
        parent_name="parent",
        config=config,
    )


def test_response_from_data_content_type_overrides(any_property_factory):
    data = oai.Response.model_construct(
        description="",
        content={oai.UntrustedString("application/zip"): oai.MediaType.model_construct()},
    )
    config = MagicMock()
    config.content_type_overrides = {"application/zip": "application/octet-stream"}
    response, _schemas = response_from_data(
        status_code=HTTPStatusPattern(pattern="200", code_range=(200, 200)),
        data=data,
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=config,
    )

    assert response == Response(
        status_code=HTTPStatusPattern(pattern="200", code_range=(200, 200)),
        prop=any_property_factory(
            name="response_200",
            default=None,
            required=True,
            description=data.description,
        ),
        source=NONE_SOURCE,
        data=data,
        byte_stream=True,
    )


def _event_stream_response(item_schema: oai.Schema | oai.Reference) -> oai.Response:
    return oai.Response.model_construct(
        description="",
        content={UntrustedString("text/event-stream"): oai.MediaType.model_construct(item_schema=item_schema)},
    )


def _event_stream(item_schema: oai.Schema | oai.Reference, config):
    return response_from_data(
        status_code=HTTPStatusPattern(pattern="200", code_range=(200, 200)),
        data=_event_stream_response(item_schema),
        schemas=Schemas(),
        responses={},
        parent_name="parent",
        config=config,
    )


def _event(name: str | None, data: oai.Schema) -> oai.Schema:
    properties: dict[str, oai.Schema] = {"data": data}
    if name is not None:
        properties["event"] = oai.Schema(const=name)
    return oai.Schema(type=oai.DataType.OBJECT, properties=properties)


def test_response_from_data_event_stream(config):
    json_data = oai.Schema(
        type=oai.DataType.STRING,
        contentMediaType="application/json",
        contentSchema=oai.Schema(type=oai.DataType.INTEGER),
    )
    text_data = oai.Schema(type=oai.DataType.STRING)
    response, _schemas = _event_stream(oai.Schema(oneOf=[_event("count", json_data), _event(None, text_data)]), config)

    assert isinstance(response, Response)
    assert response.source == NONE_SOURCE
    assert response.events is not None
    assert [event.name for event in response.events] == [UntrustedString("count"), None]
    assert response.events[0].prop is not None
    assert response.events[0].prop.get_type_string().as_unembedded_code() == "int"
    assert response.events[1].prop is None


@pytest.mark.parametrize("content_media_type", ["text/csv", None])
def test_response_from_data_event_stream_ignores_data_that_is_not_json(config, content_media_type):
    data = oai.Schema(
        type=oai.DataType.STRING,
        contentMediaType=content_media_type,
        contentSchema=oai.Schema(type=oai.DataType.INTEGER),
    )
    response, _schemas = _event_stream(_event("row", data), config)

    assert isinstance(response, Response)
    assert response.events == [responses.StreamEvent(name=UntrustedString("row"), prop=None)]


@pytest.mark.parametrize(
    "item_schema, detail",
    [
        (oai.Reference(ref="#/components/schemas/Event"), "A $ref itemSchema for an event stream is not supported"),
        (
            oai.Schema(oneOf=[oai.Reference(ref="#/components/schemas/Event")]),
            "A $ref event in an event stream is not supported",
        ),
    ],
)
def test_response_from_data_event_stream_references(config, item_schema, detail):
    response, _schemas = _event_stream(item_schema, config)

    assert isinstance(response, ParseError)
    assert response.detail == detail


def test_response_from_data_event_stream_property_error(config, mocker):
    mocker.patch.object(responses, "property_from_data", return_value=(PropertyError(detail="bad"), Schemas()))
    data = oai.Schema(
        type=oai.DataType.STRING,
        contentMediaType="application/json",
        contentSchema=oai.Schema(type=oai.DataType.INTEGER),
    )
    response, _schemas = _event_stream(_event("count", data), config)

    assert isinstance(response, ParseError)
    assert response.detail == "bad"


@pytest.mark.parametrize(
    "pattern1, pattern2, result",
    [
        ("400", "401", True),
        ("503", "500", False),
        ("default", "400", False),
        ("400", "default", True),
        ("2XX", "3XX", True),
        ("3XX", "2XX", False),
        ("2XX", "400", False),
    ],
)
def test_http_status_pattern_lt(pattern1: str, pattern2: str, result: bool) -> None:
    first = HTTPStatusPattern.parse(UntrustedString(pattern1))
    second = HTTPStatusPattern.parse(UntrustedString(pattern2))
    assert isinstance(first, HTTPStatusPattern)
    assert isinstance(second, HTTPStatusPattern)
    assert (first < second) == result
