"""Runtime container adapters and reversible reduced-environment encoding.

The static schema analyzer records typed paths such as
``("records", 1, "x")``. This module gives each path component a runtime
meaning. Built-in ``dict``, ``list``, and ``tuple`` values are supported by
default, and third-party classes can be registered after their definition.
The registered class itself does not need to be modified.

The two persisted values intentionally have different responsibilities:

``env_schema``
    Contains every piece of information needed to interpret and rebuild the
    reduced environment: scalar types, container adapter IDs, typed selectors,
    sequence lengths, and custom structural metadata.

``reduced_env``
    Contains only nested dictionaries and the selected environment leaf data.
    Lists, tuples, and custom containers are represented as dictionaries while
    stored in JSONB. No adapter ID, selector record, length, metadata, format
    marker, or wrapper object is placed in ``reduced_env``.

For example, selecting ``("records", 1, "x")`` from a list produces a payload
like ``{"records": {"1": {"x": 3}}}``. The corresponding schema records that
``records`` is a list, that its original length was two, and that storage key
``"1"`` denotes integer selector ``1``. Deserialization uses only that schema
to reconstruct the original registered container types.
"""

from __future__ import annotations

import json
import math
import operator
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Hashable


EnvPath = tuple[Hashable, ...]

# Moving reconstruction metadata from reduced_env into env_schema changes the
# persisted schema contract, so this is deliberately a new schema version.
ENV_SCHEMA_FORMAT = "jmaps.environment-schema.v2"


class ContainerRegistrationError(ValueError):
    """Raised when a container adapter cannot be registered safely."""


class ContainerEncodingError(TypeError):
    """Raised when an environment cannot be encoded through the registry."""


class ContainerDecodingError(ValueError):
    """Raised when stored environment data does not match its schema."""


class _MissingEnvironmentValue:
    """Marker occupying entries omitted from a sparse reconstructed sequence."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<unused environment entry>"

    def __copy__(self) -> _MissingEnvironmentValue:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> _MissingEnvironmentValue:
        return self


MISSING_ENV_VALUE = _MissingEnvironmentValue()
"""Singleton used for sequence positions absent from a reduced environment.

A fully selected list or tuple contains no marker. A sparsely selected sequence
retains its original type and length, but positions whose values were not saved
contain ``MISSING_ENV_VALUE`` rather than an invented value such as ``None``.
"""


IterChildren = Callable[[Any], Iterable[tuple[Hashable, Any]]]
GetChild = Callable[[Any, Hashable], Any]
NormalizeSelector = Callable[[Any, Hashable], Hashable]
GetMetadata = Callable[[Any], Mapping[str, Any]]
GetStorageKey = Callable[[Any, Hashable], str]
BuildContainer = Callable[
    [Sequence[tuple[Hashable, Any]], Mapping[str, Any]],
    Any,
]


_SELECTOR_STORAGE_PREFIX = "__jmaps_selector__:"


def _encode_selector(selector: Hashable) -> dict[str, Any]:
    """Encode a hashable selector without losing its Python type."""
    selector_type = type(selector)
    if selector is None:
        return {"type": "none", "value": None}
    if selector_type is bool:
        return {"type": "bool", "value": selector}
    if selector_type is int:
        return {"type": "int", "value": selector}
    if selector_type is float:
        if not math.isfinite(selector):
            raise ContainerEncodingError(
                f"Non-finite selector {selector!r} is not JSONB-compatible."
            )
        return {"type": "float", "value": selector}
    if selector_type is str:
        return {"type": "str", "value": selector}
    if selector_type is tuple:
        return {
            "type": "tuple",
            "value": [_encode_selector(item) for item in selector],
        }
    raise ContainerEncodingError(
        f"Selector {selector!r} has unsupported type "
        f"{selector_type.__module__}.{selector_type.__qualname__}. "
        "Supported selectors are None, bool, int, float, str, and tuples "
        "containing those types."
    )


def _decode_selector(encoded: Any) -> Hashable:
    """Decode a selector stored in env_schema."""
    if not isinstance(encoded, dict):
        raise ContainerDecodingError(
            f"Encoded selector must be a dictionary, got {encoded!r}."
        )

    selector_type = encoded.get("type")
    value = encoded.get("value")

    if selector_type == "none":
        if value is not None:
            raise ContainerDecodingError("Invalid encoded None selector.")
        return None
    if selector_type == "bool":
        if type(value) is not bool:
            raise ContainerDecodingError("Invalid encoded bool selector.")
        return value
    if selector_type == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ContainerDecodingError("Invalid encoded int selector.")
        return value
    if selector_type == "float":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ContainerDecodingError("Invalid encoded float selector.")
        decoded = float(value)
        if not math.isfinite(decoded):
            raise ContainerDecodingError("Invalid non-finite float selector.")
        return decoded
    if selector_type == "str":
        if not isinstance(value, str):
            raise ContainerDecodingError("Invalid encoded str selector.")
        return value
    if selector_type == "tuple":
        if not isinstance(value, list):
            raise ContainerDecodingError("Invalid encoded tuple selector.")
        return tuple(_decode_selector(item) for item in value)

    raise ContainerDecodingError(
        f"Unknown encoded selector type {selector_type!r}."
    )


def _selector_token(encoded: dict[str, Any]) -> str:
    """Return a deterministic token used for sorting and escaped keys."""
    return json.dumps(
        encoded,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _default_storage_key(value: Any, selector: Hashable) -> str:
    """Map a selector to a collision-free JSON object key.

    Ordinary string keys remain unchanged for readable JSONB. Non-string keys,
    and strings beginning with JourneyMAPS' reserved prefix, are escaped with a
    typed selector token. The original selector is also stored in env_schema,
    so decoding never depends on parsing this key.
    """
    if type(selector) is str and not selector.startswith(_SELECTOR_STORAGE_PREFIX):
        return selector
    return _SELECTOR_STORAGE_PREFIX + _selector_token(_encode_selector(selector))


def _sequence_storage_key(value: Any, selector: Hashable) -> str:
    """Represent a canonical sequence index as a JSON object key."""
    if isinstance(selector, bool) or not isinstance(selector, int) or selector < 0:
        raise ContainerEncodingError(
            f"Canonical sequence selector must be a non-negative integer, "
            f"got {selector!r}."
        )
    return str(selector)


@dataclass(frozen=True)
class ContainerAdapter:
    """Describe how one runtime class contains and reconstructs children.

    Parameters
    ----------
    container_type
        Exact Python class handled by this adapter unless
        ``include_subclasses=True``.
    type_id
        Stable persisted identifier. It must remain registered whenever rows
        containing this schema are loaded.
    iter_children
        Return ``(selector, value)`` pairs for every contained entry.
    get_child
        Retrieve one child using a selector from an ``EnvPath``.
    normalize_selector
        Convert a runtime selector into the canonical selector stored in the
        schema. Sequence adapters use this to convert negative indices.
    get_metadata
        Return JSON-compatible *structural* metadata required by ``build``.
        This metadata is stored only in ``env_schema``. Because it participates
        in the schema, changing it creates a different path version. Values that
        affect computation should therefore be exposed as children rather than
        hidden in metadata.
    get_storage_key
        Convert a canonical selector into the string key used in the nested
        ``reduced_env`` dictionaries. The typed selector itself remains in the
        schema.
    build
        Reconstruct an instance from decoded selected children and schema
        metadata.
    include_subclasses
        Whether this adapter may handle unregistered subclasses. Exact
        registrations always take precedence.
    """

    container_type: type[Any]
    type_id: str
    iter_children: IterChildren
    get_child: GetChild
    normalize_selector: NormalizeSelector
    get_metadata: GetMetadata
    get_storage_key: GetStorageKey
    build: BuildContainer
    include_subclasses: bool = False


class ContainerRegistry:
    """Bidirectional registry from Python classes and persisted type IDs."""

    def __init__(self) -> None:
        self._by_exact_type: dict[type[Any], ContainerAdapter] = {}
        self._by_type_id: dict[str, ContainerAdapter] = {}
        self._subclass_adapters: list[ContainerAdapter] = []

    def register(
        self,
        adapter: ContainerAdapter,
        *,
        replace: bool = False,
    ) -> ContainerAdapter:
        """Register ``adapter`` and return it.

        Duplicate classes or persisted IDs raise unless ``replace=True``.
        Replacing an adapter removes both sides of the old registration so one
        persisted ID can never decode as two different classes.
        """
        if not isinstance(adapter.container_type, type):
            raise ContainerRegistrationError(
                "container_type must be a Python class."
            )
        if not adapter.type_id or not isinstance(adapter.type_id, str):
            raise ContainerRegistrationError(
                "type_id must be a non-empty string."
            )

        existing_type = self._by_exact_type.get(adapter.container_type)
        existing_id = self._by_type_id.get(adapter.type_id)
        if not replace:
            if existing_type is not None:
                raise ContainerRegistrationError(
                    f"{adapter.container_type!r} already has container adapter "
                    f"{existing_type.type_id!r}."
                )
            if existing_id is not None:
                raise ContainerRegistrationError(
                    f"Container type_id {adapter.type_id!r} is already used by "
                    f"{existing_id.container_type!r}."
                )

        if existing_type is not None:
            self._remove(existing_type)
        if existing_id is not None and existing_id is not existing_type:
            self._remove(existing_id)

        self._by_exact_type[adapter.container_type] = adapter
        self._by_type_id[adapter.type_id] = adapter
        if adapter.include_subclasses:
            self._subclass_adapters.append(adapter)
        return adapter

    def _remove(self, adapter: ContainerAdapter) -> None:
        self._by_exact_type.pop(adapter.container_type, None)
        self._by_type_id.pop(adapter.type_id, None)
        self._subclass_adapters = [
            item for item in self._subclass_adapters if item is not adapter
        ]

    def for_value(self, value: Any) -> ContainerAdapter | None:
        """Return the adapter for ``value``, preferring an exact class match."""
        exact = self._by_exact_type.get(type(value))
        if exact is not None:
            return exact

        # Later subclass registrations take precedence, allowing a narrower
        # adapter to override a broad base registration.
        for adapter in reversed(self._subclass_adapters):
            if isinstance(value, adapter.container_type):
                return adapter
        return None

    def for_type_id(self, type_id: str) -> ContainerAdapter:
        """Return the adapter used to decode ``type_id``."""
        try:
            return self._by_type_id[type_id]
        except KeyError as exc:
            raise ContainerDecodingError(
                f"No container adapter is registered for persisted type_id "
                f"{type_id!r}. Register that class before loading the "
                "environment."
            ) from exc

    def unregister(self, container_type: type[Any]) -> None:
        """Remove the exact registration for ``container_type``."""
        adapter = self._by_exact_type.get(container_type)
        if adapter is None:
            raise ContainerRegistrationError(
                f"No adapter is registered for {container_type!r}."
            )
        self._remove(adapter)


DEFAULT_CONTAINER_REGISTRY = ContainerRegistry()


def _default_type_id(container_type: type[Any]) -> str:
    return f"{container_type.__module__}.{container_type.__qualname__}"


def register_container_adapter(
    container_type: type[Any],
    *,
    iter_children: IterChildren,
    build: BuildContainer,
    get_child: GetChild = operator.getitem,
    normalize_selector: NormalizeSelector = lambda value, selector: selector,
    get_metadata: GetMetadata = lambda value: {},
    get_storage_key: GetStorageKey = _default_storage_key,
    type_id: str | None = None,
    include_subclasses: bool = False,
    replace: bool = False,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> ContainerAdapter:
    """Post-register an arbitrary container class.

    Registration describes containment externally. It does not monkey-patch,
    subclass, or otherwise modify ``container_type``.

    ``get_metadata`` must return structural reconstruction metadata only. Its
    output is written to ``env_schema`` and never to ``reduced_env``.
    """
    adapter = ContainerAdapter(
        container_type=container_type,
        type_id=type_id or _default_type_id(container_type),
        iter_children=iter_children,
        get_child=get_child,
        normalize_selector=normalize_selector,
        get_metadata=get_metadata,
        get_storage_key=get_storage_key,
        build=build,
        include_subclasses=include_subclasses,
    )
    return registry.register(adapter, replace=replace)


def _build_dense_sequence(
    children: Sequence[tuple[Hashable, Any]],
    metadata: Mapping[str, Any],
) -> list[Any]:
    try:
        length = metadata["length"]
    except KeyError as exc:
        raise ContainerDecodingError(
            "Sequence container schema metadata has no 'length'."
        ) from exc

    if isinstance(length, bool) or not isinstance(length, int) or length < 0:
        raise ContainerDecodingError(
            f"Sequence length must be a non-negative integer, got {length!r}."
        )

    values = [MISSING_ENV_VALUE] * length
    seen: set[int] = set()
    for selector, child in children:
        if (
            isinstance(selector, bool)
            or not isinstance(selector, int)
            or selector < 0
            or selector >= length
        ):
            raise ContainerDecodingError(
                f"Sequence selector {selector!r} is invalid for length {length}."
            )
        if selector in seen:
            raise ContainerDecodingError(
                f"Duplicate sequence selector {selector}."
            )
        seen.add(selector)
        values[selector] = child
    return values


def register_sequence_container(
    container_type: type[Any],
    *,
    constructor: Callable[[Iterable[Any]], Any] | None = None,
    get_child: GetChild = operator.getitem,
    get_length: Callable[[Any], int] = len,
    type_id: str | None = None,
    include_subclasses: bool = False,
    replace: bool = False,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> ContainerAdapter:
    """Post-register a finite integer-indexed container.

    The original length is stored in ``env_schema``. The nested
    ``reduced_env`` dictionary stores only selected indices as string keys.

    ``constructor`` receives a dense iterable containing
    ``MISSING_ENV_VALUE`` at omitted positions. It defaults to the registered
    class, which is suitable for classes constructed from an iterable.
    """
    actual_constructor = constructor or container_type

    def checked_length(value: Any) -> int:
        length = get_length(value)
        if isinstance(length, bool) or not isinstance(length, int) or length < 0:
            raise ContainerEncodingError(
                f"{container_type.__qualname__} returned invalid length "
                f"{length!r}."
            )
        return length

    def iter_children(value: Any) -> Iterable[tuple[Hashable, Any]]:
        length = checked_length(value)
        return ((index, get_child(value, index)) for index in range(length))

    def normalize_selector(value: Any, selector: Hashable) -> int:
        if isinstance(selector, bool) or not isinstance(selector, int):
            raise TypeError(
                f"Sequence selector must be an integer, got {selector!r}."
            )
        length = checked_length(value)
        normalized = selector + length if selector < 0 else selector
        if normalized < 0 or normalized >= length:
            raise IndexError(selector)
        return normalized

    def get_metadata(value: Any) -> Mapping[str, Any]:
        return {"length": checked_length(value)}

    def build(
        children: Sequence[tuple[Hashable, Any]],
        metadata: Mapping[str, Any],
    ) -> Any:
        return actual_constructor(_build_dense_sequence(children, metadata))

    return register_container_adapter(
        container_type,
        type_id=type_id,
        iter_children=iter_children,
        get_child=get_child,
        normalize_selector=normalize_selector,
        get_metadata=get_metadata,
        get_storage_key=_sequence_storage_key,
        build=build,
        include_subclasses=include_subclasses,
        replace=replace,
        registry=registry,
    )


def _dict_build(
    children: Sequence[tuple[Hashable, Any]],
    metadata: Mapping[str, Any],
) -> dict[Hashable, Any]:
    if metadata:
        raise ContainerDecodingError(
            f"The built-in dict adapter received unexpected schema metadata "
            f"{metadata!r}."
        )

    output: dict[Hashable, Any] = {}
    for selector, child in children:
        if selector in output:
            raise ContainerDecodingError(
                f"Duplicate dictionary selector {selector!r}."
            )
        output[selector] = child
    return output


# Exact-type registrations are deliberate. An unregistered subclass may have
# additional invariants and should not silently be reconstructed as a built-in.
register_container_adapter(
    dict,
    type_id="builtins.dict",
    iter_children=lambda value: value.items(),
    get_child=operator.getitem,
    get_metadata=lambda value: {},
    get_storage_key=_default_storage_key,
    build=_dict_build,
)
register_sequence_container(
    list,
    type_id="builtins.list",
    constructor=list,
)
register_sequence_container(
    tuple,
    type_id="builtins.tuple",
    constructor=tuple,
)


def format_env_path(path: Sequence[Hashable]) -> str:
    """Format a typed path unambiguously for diagnostics."""
    return "env" + "".join(f"[{selector!r}]" for selector in path)


def _validate_metadata(
    metadata: Mapping[str, Any],
    *,
    adapter: ContainerAdapter,
) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        raise ContainerEncodingError(
            f"Adapter {adapter.type_id!r} get_metadata() must return a mapping, "
            f"got {type(metadata).__name__}."
        )

    copied = dict(metadata)
    try:
        json.dumps(copied, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ContainerEncodingError(
            f"Adapter {adapter.type_id!r} returned metadata that is not "
            "JSON-compatible."
        ) from exc
    return copied


@dataclass
class _PathTrie:
    terminal: bool = False
    children: dict[Hashable, _PathTrie] = field(default_factory=dict)


def _make_path_trie(paths: Iterable[EnvPath]) -> _PathTrie:
    root = _PathTrie()
    for raw_path in paths:
        path = tuple(raw_path)
        node = root
        for selector in path:
            try:
                hash(selector)
            except TypeError as exc:
                raise ContainerEncodingError(
                    f"Environment path selector {selector!r} is not hashable."
                ) from exc
            node = node.children.setdefault(selector, _PathTrie())
        node.terminal = True
    return root


def _merge_path_tries(destination: _PathTrie, source: _PathTrie) -> None:
    destination.terminal = destination.terminal or source.terminal
    for selector, source_child in source.children.items():
        destination_child = destination.children.setdefault(selector, _PathTrie())
        _merge_path_tries(destination_child, source_child)


def serialize_reduced_environment(
    env: Any,
    paths: Iterable[EnvPath],
    *,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Encode selected paths into ``env_schema`` and a data-only payload.

    The returned ``reduced_env`` is a plain nested dictionary containing only
    selected leaf values in their SQL/JSON-compatible representation. Every
    selector, scalar type, adapter ID, sequence length, and custom structural
    metadata value is stored exclusively in ``env_schema``.
    """
    # Import lazily so the static analyzer can use the adapter registry without
    # importing SQLAlchemy through jmalc during module import.
    from jmaps.journey.jmalc import cast_sql_type, get_sql_type

    trie = _make_path_trie(paths)
    active_container_ids: set[int] = set()

    def encode_all_children(value: Any) -> _PathTrie:
        adapter = registry.for_value(value)
        full = _PathTrie(terminal=True)
        if adapter is None:
            return full

        object_id = id(value)
        if object_id in active_container_ids:
            raise ContainerEncodingError(
                "Cyclic environment container encountered while expanding a "
                "fully used value."
            )

        active_container_ids.add(object_id)
        try:
            full.terminal = False
            for selector, child in adapter.iter_children(value):
                canonical_selector = adapter.normalize_selector(value, selector)
                child_trie = encode_all_children(child)
                existing = full.children.setdefault(
                    canonical_selector,
                    _PathTrie(),
                )
                _merge_path_tries(existing, child_trie)

            # An empty container is still a selected value. Its adapter and
            # metadata live in the schema while its payload is simply {}.
            if not full.children:
                full.terminal = True
            return full
        finally:
            active_container_ids.remove(object_id)

    def encode_node(
        value: Any,
        selected: _PathTrie,
        path: EnvPath,
    ) -> tuple[dict[str, Any], Any]:
        adapter = registry.for_value(value)

        # A terminal container means the complete value was selected. EnvTree
        # normally expands it first, but supporting it here keeps this function
        # independently correct and preserves empty containers.
        if selected.terminal and adapter is not None:
            selected = encode_all_children(value)

        if adapter is None:
            if selected.children:
                raise ContainerEncodingError(
                    f"Environment path {format_env_path(path)} traverses into "
                    f"non-container {type(value).__name__}."
                )
            if not selected.terminal:
                raise ContainerEncodingError(
                    f"Environment path {format_env_path(path)} was not selected."
                )

            sql_type = get_sql_type(value)
            if sql_type not in {"bool", "float", "int", "str", "datetime"}:
                raise ContainerEncodingError(
                    f"Environment value at {format_env_path(path)} is treated "
                    f"as SQL type {sql_type!r}, but its container class is not "
                    "registered. Register it with register_container_adapter() "
                    "or register_sequence_container()."
                )
            return (
                {"kind": "scalar", "type": sql_type},
                cast_sql_type(value),
            )

        object_id = id(value)
        if object_id in active_container_ids:
            raise ContainerEncodingError(
                f"Cyclic environment container encountered at "
                f"{format_env_path(path)}."
            )

        active_container_ids.add(object_id)
        try:
            canonical_children: dict[Hashable, _PathTrie] = {}
            for requested_selector, requested_trie in selected.children.items():
                try:
                    selector = adapter.normalize_selector(
                        value,
                        requested_selector,
                    )
                    adapter.get_child(value, selector)
                except (KeyError, IndexError, TypeError, ValueError) as exc:
                    raise ContainerEncodingError(
                        f"Selector {requested_selector!r} does not exist at "
                        f"{format_env_path(path)} for adapter "
                        f"{adapter.type_id!r}."
                    ) from exc

                target = canonical_children.setdefault(selector, _PathTrie())
                _merge_path_tries(target, requested_trie)

            schema_children: dict[str, dict[str, Any]] = {}
            data_children: dict[str, Any] = {}

            selected_items = sorted(
                canonical_children.items(),
                key=lambda item: _selector_token(_encode_selector(item[0])),
            )
            for selector, child_trie in selected_items:
                try:
                    storage_key = adapter.get_storage_key(value, selector)
                except ContainerEncodingError:
                    raise
                except Exception as exc:
                    raise ContainerEncodingError(
                        f"Adapter {adapter.type_id!r} could not create a storage "
                        f"key for selector {selector!r} at "
                        f"{format_env_path(path)}."
                    ) from exc

                if not isinstance(storage_key, str):
                    raise ContainerEncodingError(
                        f"Adapter {adapter.type_id!r} returned non-string storage "
                        f"key {storage_key!r} for selector {selector!r}."
                    )
                if storage_key in schema_children:
                    raise ContainerEncodingError(
                        f"Adapter {adapter.type_id!r} maps more than one selector "
                        f"to reduced-environment key {storage_key!r} at "
                        f"{format_env_path(path)}."
                    )

                child_value = adapter.get_child(value, selector)
                child_schema, child_data = encode_node(
                    child_value,
                    child_trie,
                    (*path, selector),
                )
                schema_children[storage_key] = {
                    "selector": _encode_selector(selector),
                    "schema": child_schema,
                }
                data_children[storage_key] = child_data

            metadata = _validate_metadata(
                adapter.get_metadata(value),
                adapter=adapter,
            )
            return (
                {
                    "kind": "container",
                    "adapter": adapter.type_id,
                    "metadata": metadata,
                    "children": schema_children,
                },
                data_children,
            )
        finally:
            active_container_ids.remove(object_id)

    root_schema, root_data = encode_node(env, trie, ())
    if not isinstance(root_data, dict):
        raise ContainerEncodingError(
            "The JourneyMAPS environment root must encode as a dictionary."
        )

    return (
        {
            "format": ENV_SCHEMA_FORMAT,
            "root": root_schema,
        },
        root_data,
    )


def deserialize_reduced_environment(
    schema: Mapping[str, Any],
    reduced_env: Mapping[str, Any],
    *,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> Any:
    """Reconstruct a reduced environment using metadata from ``env_schema``.

    ``reduced_env`` is expected to be the plain nested dictionary stored in
    ``DBResult.environment``. It contains no format marker or reconstruction
    metadata. Fully selected containers reconstruct exactly. Omitted positions
    in sparse sequences become ``MISSING_ENV_VALUE`` while preserving original
    sequence type and length.
    """
    from jmaps.journey.jmalc import restore_sql_type

    if schema.get("format") != ENV_SCHEMA_FORMAT:
        raise ContainerDecodingError(
            f"Unsupported environment schema format {schema.get('format')!r}."
        )
    if not isinstance(reduced_env, Mapping):
        raise ContainerDecodingError(
            "The reduced environment root must be a mapping."
        )

    def decode_node(schema_node: Any, data_node: Any, path: EnvPath) -> Any:
        if not isinstance(schema_node, dict):
            raise ContainerDecodingError(
                f"Invalid schema node at {format_env_path(path)}."
            )

        kind = schema_node.get("kind")
        if kind == "scalar":
            return restore_sql_type(data_node, schema_node.get("type"))

        if kind != "container":
            raise ContainerDecodingError(
                f"Unknown schema node kind {kind!r} at "
                f"{format_env_path(path)}."
            )
        if not isinstance(data_node, Mapping):
            raise ContainerDecodingError(
                f"Container data at {format_env_path(path)} must be a mapping."
            )

        adapter_id = schema_node.get("adapter")
        if not isinstance(adapter_id, str):
            raise ContainerDecodingError(
                f"Container schema at {format_env_path(path)} has no adapter ID."
            )
        adapter = registry.for_type_id(adapter_id)

        schema_children = schema_node.get("children")
        if not isinstance(schema_children, dict):
            raise ContainerDecodingError(
                f"Container schema children at {format_env_path(path)} must be "
                "a dictionary."
            )
        if not all(isinstance(key, str) for key in schema_children):
            raise ContainerDecodingError(
                f"Container schema child keys at {format_env_path(path)} must "
                "all be strings."
            )
        if not all(isinstance(key, str) for key in data_node):
            raise ContainerDecodingError(
                f"Reduced-environment keys at {format_env_path(path)} must all "
                "be strings."
            )

        schema_keys = set(schema_children)
        data_keys = set(data_node)
        if schema_keys != data_keys:
            missing = sorted(schema_keys - data_keys)
            extra = sorted(data_keys - schema_keys)
            raise ContainerDecodingError(
                f"Stored children do not match the schema at "
                f"{format_env_path(path)}; missing={missing!r}, extra={extra!r}."
            )

        decoded_children: list[tuple[Hashable, Any]] = []
        seen_selectors: set[Hashable] = set()
        for storage_key in sorted(schema_children):
            child_entry = schema_children[storage_key]
            if not isinstance(child_entry, dict):
                raise ContainerDecodingError(
                    f"Invalid child schema for storage key {storage_key!r} at "
                    f"{format_env_path(path)}."
                )
            if "selector" not in child_entry or "schema" not in child_entry:
                raise ContainerDecodingError(
                    f"Incomplete child schema for storage key {storage_key!r} "
                    f"at {format_env_path(path)}."
                )

            selector = _decode_selector(child_entry["selector"])
            if selector in seen_selectors:
                raise ContainerDecodingError(
                    f"Duplicate decoded selector {selector!r} at "
                    f"{format_env_path(path)}."
                )
            seen_selectors.add(selector)

            decoded_children.append(
                (
                    selector,
                    decode_node(
                        child_entry["schema"],
                        data_node[storage_key],
                        (*path, selector),
                    ),
                )
            )

        metadata = schema_node.get("metadata")
        if not isinstance(metadata, dict):
            raise ContainerDecodingError(
                f"Container schema metadata at {format_env_path(path)} must be "
                "a dictionary."
            )

        try:
            return adapter.build(decoded_children, metadata)
        except ContainerDecodingError:
            raise
        except Exception as exc:
            raise ContainerDecodingError(
                f"Adapter {adapter.type_id!r} could not rebuild the container "
                f"at {format_env_path(path)}."
            ) from exc

    return decode_node(schema["root"], reduced_env, ())


def contains_missing_environment_values(value: Any) -> bool:
    """Return whether a reconstructed reduced environment is sparse."""
    if value is MISSING_ENV_VALUE:
        return True

    adapter = DEFAULT_CONTAINER_REGISTRY.for_value(value)
    if adapter is None:
        return False
    return any(
        contains_missing_environment_values(child)
        for _, child in adapter.iter_children(value)
    )
