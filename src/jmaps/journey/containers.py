"""Runtime container adapters and reversible reduced-environment encoding.

The static schema analyzer records paths such as ``("records", 1, "x")``.
This module supplies the runtime meaning of each path component. Built-in
``dict``, ``list``, and ``tuple`` values are supported by default. Third-party
classes can be registered after their definition; the class itself does not
need to inherit from a JourneyMAPS type or be modified in any way.

Environment data is encoded as a sparse tree. Container type information lives
in the environment schema, while runtime reconstruction metadata (for example,
sequence length) lives in the reduced environment. This preserves list-versus-
tuple identity and allows individual sequence entries to remain independent
cache dependencies.
"""

from __future__ import annotations

import json
import math
import operator
from dataclasses import dataclass, field
from typing import Any, Callable, Hashable, Iterable, Mapping, Sequence


EnvPath = tuple[Hashable, ...]

ENV_SCHEMA_FORMAT = "jmaps.environment-schema.v1"
REDUCED_ENV_FORMAT = "jmaps.reduced-environment.v1"


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
"""Singleton used for sequence entries absent from a reduced environment.

A completely used list or tuple reconstructs without this marker. A sparsely
used sequence keeps its original length and type, and omitted positions contain
``MISSING_ENV_VALUE`` because their original values were intentionally not
saved.
"""


IterChildren = Callable[[Any], Iterable[tuple[Hashable, Any]]]
GetChild = Callable[[Any, Hashable], Any]
NormalizeSelector = Callable[[Any, Hashable], Hashable]
GetMetadata = Callable[[Any], Mapping[str, Any]]
BuildContainer = Callable[
    [Sequence[tuple[Hashable, Any]], Mapping[str, Any]],
    Any,
]


@dataclass(frozen=True)
class ContainerAdapter:
    """Describe how one runtime class contains and reconstructs children.

    Parameters
    ----------
    container_type
        Exact Python class handled by this adapter unless
        ``include_subclasses=True``.
    type_id
        Stable persisted identifier. It must remain available when old rows are
        decoded. The default registration helpers use
        ``"<module>.<qualname>"``.
    iter_children
        Return ``(selector, value)`` pairs for all contained entries.
    get_child
        Retrieve one child by the selector appearing in an ``EnvPath``.
    normalize_selector
        Convert a valid runtime selector to the canonical selector persisted in
        the schema. Sequence adapters use this to turn negative indices into
        non-negative positions.
    get_metadata
        Return JSON-compatible runtime metadata required by ``build``. List and
        tuple adapters store their original length here.
    build
        Reconstruct an instance from decoded selected children and metadata.
    include_subclasses
        Whether the adapter may handle subclasses that do not have their own
        exact registration. Exact registrations always take precedence.
    """

    container_type: type[Any]
    type_id: str
    iter_children: IterChildren
    get_child: GetChild
    normalize_selector: NormalizeSelector
    get_metadata: GetMetadata
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
        Replacing an adapter removes both sides of the old registration so the
        registry cannot decode one ID as two different classes.
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

        # Later subclass registrations take precedence, which lets users
        # override a broad base-class adapter without replacing it globally.
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
    type_id: str | None = None,
    include_subclasses: bool = False,
    replace: bool = False,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> ContainerAdapter:
    """Post-register an arbitrary container class.

    This is the general escape hatch for a class that cannot be modified.
    Registration only describes containment externally; it does not monkey
    patch or subclass ``container_type``.
    """
    adapter = ContainerAdapter(
        container_type=container_type,
        type_id=type_id or _default_type_id(container_type),
        iter_children=iter_children,
        get_child=get_child,
        normalize_selector=normalize_selector,
        get_metadata=get_metadata,
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
            "Sequence container metadata has no 'length'."
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
                f"Invalid sequence selector {selector!r} for length {length}."
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

    ``constructor`` receives a dense iterable with ``MISSING_ENV_VALUE`` at
    positions that were not part of the reduced environment. It defaults to the
    registered class itself, which is suitable for classes constructed from an
    iterable.
    """
    actual_constructor = constructor or container_type

    def iter_children(value: Any) -> Iterable[tuple[Hashable, Any]]:
        length = get_length(value)
        if isinstance(length, bool) or not isinstance(length, int) or length < 0:
            raise ContainerEncodingError(
                f"{container_type.__qualname__} returned invalid length "
                f"{length!r}."
            )
        return ((index, get_child(value, index)) for index in range(length))

    def normalize_selector(value: Any, selector: Hashable) -> int:
        if isinstance(selector, bool) or not isinstance(selector, int):
            raise TypeError(
                f"Sequence selector must be an integer, got {selector!r}."
            )
        length = get_length(value)
        normalized = selector + length if selector < 0 else selector
        if normalized < 0 or normalized >= length:
            raise IndexError(selector)
        return normalized

    def get_metadata(value: Any) -> Mapping[str, Any]:
        length = get_length(value)
        if isinstance(length, bool) or not isinstance(length, int) or length < 0:
            raise ContainerEncodingError(
                f"{container_type.__qualname__} returned invalid length "
                f"{length!r}."
            )
        return {"length": length}

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
            f"The built-in dict adapter received unexpected metadata {metadata!r}."
        )
    output: dict[Hashable, Any] = {}
    for selector, child in children:
        if selector in output:
            raise ContainerDecodingError(
                f"Duplicate dictionary selector {selector!r}."
            )
        output[selector] = child
    return output


# Exact-type registrations are deliberate. A subclass can have additional
# invariants, so silently decoding it as a plain built-in would not be reversible.
register_container_adapter(
    dict,
    type_id="builtins.dict",
    iter_children=lambda value: value.items(),
    get_child=operator.getitem,
    get_metadata=lambda value: {},
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


def _encode_selector(selector: Hashable) -> dict[str, Any]:
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
    return json.dumps(
        encoded,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _validate_metadata(metadata: Mapping[str, Any], *, adapter: ContainerAdapter) -> dict[str, Any]:
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


def serialize_reduced_environment(
    env: Any,
    paths: Iterable[EnvPath],
    *,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Encode selected environment paths into a schema and JSONB-safe data.

    Container classes are represented by their registered ``type_id`` in the
    schema. Container data stores only selected children plus reconstruction
    metadata. Consequently a sparse list or tuple preserves its exact type and
    original length without saving values that were not used.
    """
    # Import lazily so static AST analysis can use the adapter registry without
    # importing SQLAlchemy through jmalc.
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
                full.children[selector] = encode_all_children(child)
            # An empty container remains a terminal selected value.
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

        # A terminal container means the whole value was selected. Normally
        # EnvTree has already expanded it, but accepting this form makes the
        # serializer independently useful and preserves empty containers.
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
            schema_children: list[dict[str, Any]] = []
            data_children: list[dict[str, Any]] = []

            canonical_children: dict[Hashable, _PathTrie] = {}
            for requested_selector, requested_trie in selected.children.items():
                try:
                    selector = adapter.normalize_selector(
                        value,
                        requested_selector,
                    )
                    child_value = adapter.get_child(value, selector)
                except (KeyError, IndexError, TypeError, ValueError) as exc:
                    raise ContainerEncodingError(
                        f"Selector {requested_selector!r} does not exist at "
                        f"{format_env_path(path)} for adapter "
                        f"{adapter.type_id!r}."
                    ) from exc

                target = canonical_children.setdefault(selector, _PathTrie())
                target.terminal = target.terminal or requested_trie.terminal
                stack = [(target, requested_trie)]
                while stack:
                    destination, source = stack.pop()
                    destination.terminal = destination.terminal or source.terminal
                    for child_selector, source_child in source.children.items():
                        destination_child = destination.children.setdefault(
                            child_selector,
                            _PathTrie(),
                        )
                        stack.append((destination_child, source_child))

            selected_items = sorted(
                canonical_children.items(),
                key=lambda item: _selector_token(_encode_selector(item[0])),
            )
            for selector, child_trie in selected_items:
                encoded_selector = _encode_selector(selector)
                child_value = adapter.get_child(value, selector)
                child_schema, child_data = encode_node(
                    child_value,
                    child_trie,
                    (*path, selector),
                )
                schema_children.append(
                    {
                        "selector": encoded_selector,
                        "schema": child_schema,
                    }
                )
                data_children.append(
                    {
                        "selector": encoded_selector,
                        "value": child_data,
                    }
                )

            metadata = _validate_metadata(
                adapter.get_metadata(value),
                adapter=adapter,
            )
            return (
                {
                    "kind": "container",
                    "adapter": adapter.type_id,
                    "children": schema_children,
                },
                {
                    "metadata": metadata,
                    "children": data_children,
                },
            )
        finally:
            active_container_ids.remove(object_id)

    root_schema, root_data = encode_node(env, trie, ())
    return (
        {
            "format": ENV_SCHEMA_FORMAT,
            "root": root_schema,
        },
        {
            "format": REDUCED_ENV_FORMAT,
            "root": root_data,
        },
    )


def deserialize_reduced_environment(
    schema: Mapping[str, Any],
    reduced_env: Mapping[str, Any],
    *,
    registry: ContainerRegistry = DEFAULT_CONTAINER_REGISTRY,
) -> Any:
    """Reconstruct a reduced environment using persisted container type IDs.

    Fully selected containers reconstruct exactly. Sparse sequence positions
    reconstruct as ``MISSING_ENV_VALUE`` while retaining the original sequence
    length and concrete registered class.
    """
    from jmaps.journey.jmalc import restore_sql_type

    if schema.get("format") != ENV_SCHEMA_FORMAT:
        raise ContainerDecodingError(
            f"Unsupported environment schema format {schema.get('format')!r}."
        )
    if reduced_env.get("format") != REDUCED_ENV_FORMAT:
        raise ContainerDecodingError(
            f"Unsupported reduced environment format "
            f"{reduced_env.get('format')!r}."
        )

    def child_map(
        entries: Any,
        *,
        field_name: str,
    ) -> dict[str, tuple[Hashable, Any]]:
        if not isinstance(entries, list):
            raise ContainerDecodingError(
                f"Container {field_name} must be a list."
            )
        output: dict[str, tuple[Hashable, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict) or "selector" not in entry:
                raise ContainerDecodingError(
                    f"Invalid container {field_name} entry {entry!r}."
                )
            encoded_selector = entry["selector"]
            selector = _decode_selector(encoded_selector)
            token = _selector_token(encoded_selector)
            if token in output:
                raise ContainerDecodingError(
                    f"Duplicate encoded selector {selector!r}."
                )
            output[token] = (selector, entry)
        return output

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
        if not isinstance(data_node, dict):
            raise ContainerDecodingError(
                f"Container data at {format_env_path(path)} must be a mapping."
            )

        adapter_id = schema_node.get("adapter")
        if not isinstance(adapter_id, str):
            raise ContainerDecodingError(
                f"Container schema at {format_env_path(path)} has no adapter ID."
            )
        adapter = registry.for_type_id(adapter_id)

        schema_entries = child_map(
            schema_node.get("children"),
            field_name="schema children",
        )
        data_entries = child_map(
            data_node.get("children"),
            field_name="data children",
        )
        if set(schema_entries) != set(data_entries):
            missing = set(schema_entries) - set(data_entries)
            extra = set(data_entries) - set(schema_entries)
            raise ContainerDecodingError(
                f"Stored children do not match the schema at "
                f"{format_env_path(path)}; missing={sorted(missing)!r}, "
                f"extra={sorted(extra)!r}."
            )

        decoded_children: list[tuple[Hashable, Any]] = []
        for token in sorted(schema_entries):
            selector, schema_entry = schema_entries[token]
            data_selector, data_entry = data_entries[token]
            if selector != data_selector:
                raise ContainerDecodingError(
                    f"Selector mismatch at {format_env_path(path)}."
                )
            if "schema" not in schema_entry or "value" not in data_entry:
                raise ContainerDecodingError(
                    f"Incomplete child entry at {format_env_path(path)}."
                )
            decoded_children.append(
                (
                    selector,
                    decode_node(
                        schema_entry["schema"],
                        data_entry["value"],
                        (*path, selector),
                    ),
                )
            )

        metadata = data_node.get("metadata")
        if not isinstance(metadata, dict):
            raise ContainerDecodingError(
                f"Container metadata at {format_env_path(path)} must be a "
                "dictionary."
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

    return decode_node(schema["root"], reduced_env["root"], ())


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
