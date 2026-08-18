from __future__ import annotations

import ast
import inspect
import json
import textwrap
from dataclasses import dataclass, field
from typing import Any, Hashable, Iterable, Literal, NamedTuple, Sequence, Dict
import logging 

logger = logging.getLogger(__name__)

EnvPath = tuple[Hashable, ...]

try:  # DeepDiff improves diagnostics, but the analyzer remains self-contained.
    from deepdiff import DeepDiff  # type: ignore
except ImportError:  # pragma: no cover - exercised when DeepDiff is not installed.
    DeepDiff = None  # type: ignore[assignment]


class SchemaAnalysisError(RuntimeError):
    """Base class for static env-schema analysis errors."""


class SourceUnavailableError(SchemaAnalysisError):
    """Raised when Python source cannot be recovered for a function handle."""


class DynamicKeyError(SchemaAnalysisError):
    """Raised when an env key cannot be determined statically."""


class PathExplosionError(SchemaAnalysisError):
    """Raised when control-flow expansion exceeds the configured path limit."""


class DynamicSchemaError(SchemaAnalysisError):
    """Raised when different execution paths produce different schemas.

    Attributes
    ----------
    branch_differences
        A list of dictionaries. Each dictionary identifies the baseline path,
        the compared path(s), and a structural diff between their outputs.
    """

    def __init__(self, branch_differences: list[dict[str, Any]]) -> None:
        self.branch_differences = branch_differences
        message = self._build_message(branch_differences)
        super().__init__(message)

    @staticmethod
    def _build_message(branch_differences: list[dict[str, Any]]) -> str:
        lines = [
            "Dynamic env schema detected: logical execution paths do not "
            "produce identical EnvTree/FunctionCall outputs."
        ]
        for index, item in enumerate(branch_differences, start=1):
            lines.append(f"\nDifference group {index}:")
            lines.append(
                "  baseline path(s): " + ", ".join(item["baseline_paths"])
            )
            lines.append(
                "  compared path(s): " + ", ".join(item["compared_paths"])
            )
            lines.append(
                textwrap.indent(
                    json.dumps(item["diff"], indent=2, default=repr), "  "
                )
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class NodeRef:
    """Stable reference to a node in one symbolic EnvTree."""

    tree_id: int
    path: tuple[Hashable, ...] = ()


@dataclass(frozen=True)
class CopyOrigin:
    """Identifies the tree node from which a symbolic deep copy was made."""

    tree_id: int
    path: tuple[Hashable, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "tree_id": self.tree_id,
            "path": [_encode_key(key) for key in self.path],
        }


@dataclass
class EnvNode:
    """One node in a symbolic dictionary tree.

    Parameters
    ----------
    key
        The dictionary key used to reach this node from its parent. The root
        stores a descriptive root key instead.
    used
        Whether this node has been used according to the analyzer's rules.
    overwritten
        Whether this node's value has been replaced. Once true, future reads
        cannot change ``used`` for this node or any overwritten descendant.
    parent
        Parent node, or ``None`` for a tree root.
    children
        Child nodes keyed by their literal dictionary keys.
    """

    key: Hashable
    used: bool = False
    overwritten: bool = False
    parent: EnvNode | None = field(default=None, repr=False, compare=False)
    children: dict[Hashable, EnvNode] = field(default_factory=dict)

    def ensure_child(self, key: Hashable) -> EnvNode:
        """Return an existing child, or create one with inherited overwrite state."""
        child = self.children.get(key)
        if child is None:
            child = EnvNode(
                key=key,
                used=False,
                overwritten=self.overwritten,
                parent=self,
            )
            self.children[key] = child
        return child

    def mark_used(self) -> None:
        """Mark this node and eligible existing descendants as used.

        Traversal stops at every overwritten node. Existing ``used=True``
        values are never cleared.
        """
        if self.overwritten:
            return
        self.used = True
        for child in self.children.values():
            child.mark_used()

    def mark_overwritten(self) -> None:
        """Mark this node and all existing descendants as overwritten."""
        self.overwritten = True
        for child in self.children.values():
            child.mark_overwritten()

    def clone(self, parent: EnvNode | None = None) -> EnvNode:
        """Deep-copy this node and its descendants while repairing parents."""
        cloned = EnvNode(
            key=self.key,
            used=self.used,
            overwritten=self.overwritten,
            parent=parent,
        )
        cloned.children = {
            key: child.clone(parent=cloned) for key, child in self.children.items()
        }
        return cloned

    def path(self) -> tuple[Hashable, ...]:
        """Return this node's path relative to its tree root."""
        keys: list[Hashable] = []
        current: EnvNode | None = self
        while current is not None and current.parent is not None:
            keys.append(current.key)
            current = current.parent
        keys.reverse()
        return tuple(keys)

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic, comparison-friendly representation."""
        ordered_children = sorted(
            self.children.values(), key=lambda node: _key_sort_token(node.key)
        )
        return {
            "key": _encode_key(self.key),
            "used": self.used,
            "overwritten": self.overwritten,
            "children": [child.to_dict() for child in ordered_children],
        }


@dataclass
class EnvTree:
    """A symbolic env dictionary or an independently tracked deep copy."""

    tree_id: int
    root: EnvNode
    copied_from: CopyOrigin | None = None
    reduced_to_overwrites: bool = False

    def get_node(self, path: Sequence[Hashable], *, create: bool = True) -> EnvNode:
        """Resolve a path relative to the root."""
        node = self.root
        for key in path:
            if create:
                node = node.ensure_child(key)
            else:
                try:
                    node = node.children[key]
                except KeyError as exc:
                    raise KeyError(tuple(path)) from exc
        return node

    def clone(self) -> EnvTree:
        """Deep-copy this complete tree."""
        return EnvTree(
            tree_id=self.tree_id,
            root=self.root.clone(),
            copied_from=self.copied_from,
            reduced_to_overwrites=self.reduced_to_overwrites,
        )

    def reduced_overwrite_copy(self) -> EnvTree:
        """Snapshot only ancestor paths leading to overwritten nodes.

        The root is always retained so the result remains a valid tree. If no
        overwritten node exists, the reduced tree consists of the root alone.
        """

        def prune(node: EnvNode, parent: EnvNode | None, *, keep_root: bool) -> EnvNode | None:
            kept_children: dict[Hashable, EnvNode] = {}
            placeholder = EnvNode(
                key=node.key,
                used=node.used,
                overwritten=node.overwritten,
                parent=parent,
            )
            for key, child in node.children.items():
                kept = prune(child, placeholder, keep_root=False)
                if kept is not None:
                    kept_children[key] = kept
            should_keep = keep_root or node.overwritten or bool(kept_children)
            if not should_keep:
                return None
            placeholder.children = kept_children
            return placeholder

        reduced_root = prune(self.root, None, keep_root=True)
        assert reduced_root is not None
        return EnvTree(
            tree_id=self.tree_id,
            root=reduced_root,
            copied_from=self.copied_from,
            reduced_to_overwrites=True,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic representation suitable for DeepDiff."""
        return {
            "tree_id": self.tree_id,
            "copied_from": (
                None if self.copied_from is None else self.copied_from.to_dict()
            ),
            "reduced_to_overwrites": self.reduced_to_overwrites,
            "root": self.root.to_dict(),
        }
    
    def get_used_leaves(
        self,
        env: dict[Hashable, Any] | None = None,
    ) -> set[EnvPath]:
        """Return typed paths for all used leaf nodes in this tree.

        When ``env`` is supplied, a used dictionary branch is expanded to the
        actual leaves below that branch. Path entries retain their original
        types, so ``env["records"][1]["x"]`` is represented as
        ``("records", 1, "x")``.

        The tree root is excluded from returned paths.
        """
        used_paths: set[EnvPath] = set()
        resolve_env = env is not None

        def add_leaves(
            deeper_env: dict[Hashable, Any],
            parent_path: EnvPath,
        ) -> None:
            for key, value in deeper_env.items():
                child_path = (*parent_path, key)
                if isinstance(value, dict):
                    add_leaves(value, child_path)
                else:
                    used_paths.add(child_path)

        def visit(
            node: EnvNode,
            current_env: Any,
            parent_path: EnvPath,
        ) -> None:
            for child in node.children.values():
                child_path = (*parent_path, child.key)
                deeper_env: Any = None

                if resolve_env:
                    if not isinstance(current_env, dict):
                        raise ValueError(
                            f"Environment path {parent_path!r} is not a "
                            f"dictionary, so it cannot contain {child.key!r}."
                        )
                    if child.key not in current_env:
                        raise ValueError(
                            f"Key {child.key!r} does not exist at environment "
                            f"path {parent_path!r}."
                        )
                    deeper_env = current_env[child.key]

                if child.used and not child.children:
                    if resolve_env and isinstance(deeper_env, dict):
                        add_leaves(deeper_env, child_path)
                    else:
                        used_paths.add(child_path)

                # Always inspect descendants. A child can be used even when its
                # parent is not, such as env["branch"]["leaf"].
                visit(child, deeper_env, child_path)

        visit(self.root, env, ())
        return used_paths

    def get_overwritten_leaves(
        self,
        env: dict[Hashable, Any] | None = None,
    ) -> set[EnvPath]:
        """Return typed paths for all overwritten leaf nodes in this tree.

        When ``env`` is supplied, an overwritten dictionary branch is expanded
        to the actual leaves below that branch. Path entries retain their
        original types.
        """
        overwritten_paths: set[EnvPath] = set()
        resolve_env = env is not None

        def add_leaves(
            deeper_env: dict[Hashable, Any],
            parent_path: EnvPath,
        ) -> None:
            for key, value in deeper_env.items():
                child_path = (*parent_path, key)
                if isinstance(value, dict):
                    add_leaves(value, child_path)
                else:
                    overwritten_paths.add(child_path)

        def visit(
            node: EnvNode,
            current_env: Any,
            parent_path: EnvPath,
        ) -> None:
            for child in node.children.values():
                child_path = (*parent_path, child.key)
                deeper_env: Any = None

                if resolve_env:
                    if not isinstance(current_env, dict):
                        raise ValueError(
                            f"Environment path {parent_path!r} is not a "
                            f"dictionary, so it cannot contain {child.key!r}."
                        )
                    if child.key not in current_env:
                        raise ValueError(
                            f"Key {child.key!r} does not exist at environment "
                            f"path {parent_path!r}."
                        )
                    deeper_env = current_env[child.key]

                if child.overwritten and not child.children:
                    if resolve_env and isinstance(deeper_env, dict):
                        add_leaves(deeper_env, child_path)
                    else:
                        overwritten_paths.add(child_path)

                visit(child, deeper_env, child_path)

        visit(self.root, env, ())
        return overwritten_paths
        
    def __repr__(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=repr)

def get_qualified_name(local_name, namespace: dict[str, Any]) -> str | None:
    local_path = local_name.split(".")
    obj = namespace.get(local_path[0])
    if obj is None:
        return None
    for attr in local_path[1:]:
        obj = getattr(obj, attr, None)
        if obj is None:
            return None
    return f"{obj.__module__}.{obj.__qualname__}"

class FunctionCall(NamedTuple):
    """A function call receiving a tracked tree root.

    Fields
    ------
    function_name
        Static dotted/unparsed name of the called function.
    overwritten_tree
        Independent reduced snapshot containing only paths to overwritten
        nodes at the time the argument is evaluated.
    """

    function_name: str
    overwritten_tree: EnvTree

    def to_dict(self) -> dict[str, Any]:
        return {"function_name": self.function_name, "tree": self.overwritten_tree.to_dict()}

    def get_runtime_name(self, namespace: dict[str, Any]) -> str:
        """Return the fully qualified name of the function at runtime."""
        runtime_name = get_qualified_name(self.function_name, namespace)
        if runtime_name is None:
            raise ValueError(f"Function call {self.function_name} not found in globals.")
        return runtime_name

    def get_overwritten_leaves(
        self, env: dict[Hashable, Any] | None = None
    ) -> set[EnvPath]:
        """Return the set of paths that are overwritten in this function call."""
        return self.overwritten_tree.get_overwritten_leaves(env=env)


@dataclass
class _AnalysisState:
    trees: list[EnvTree]
    active_aliases: dict[str, NodeRef]
    function_calls: list[FunctionCall]
    next_tree_id: int

    def clone(self) -> _AnalysisState:
        return _AnalysisState(
            trees=[tree.clone() for tree in self.trees],
            active_aliases=dict(self.active_aliases),
            function_calls=[
                FunctionCall(call.function_name, call.tree.clone())
                for call in self.function_calls
            ],
            next_tree_id=self.next_tree_id,
        )

    def tree(self, tree_id: int) -> EnvTree:
        try:
            return self.trees[tree_id]
        except (IndexError, TypeError) as exc:
            raise SchemaAnalysisError(f"Unknown symbolic tree id {tree_id}") from exc

    def node(self, ref: NodeRef, *, create: bool = True) -> EnvNode:
        return self.tree(ref.tree_id).get_node(ref.path, create=create)

    def snapshot(self) -> dict[str, Any]:
        """Return only the user-requested outputs, not transient aliases."""
        return {
            "trees": [tree.to_dict() for tree in self.trees],
            "function_calls": [call.to_dict() for call in self.function_calls],
        }


FlowStatus = Literal["normal", "return", "raise", "break", "continue"]


@dataclass
class _Flow:
    state: _AnalysisState
    status: FlowStatus = "normal"
    provenance: tuple[str, ...] = ("entry",)

    def branched(self, label: str) -> _Flow:
        return _Flow(
            state=self.state.clone(),
            status=self.status,
            provenance=(*self.provenance, label),
        )

    @property
    def label(self) -> str:
        return " -> ".join(self.provenance)


@dataclass(frozen=True)
class _ReferenceResult:
    ref: NodeRef
    pure_deepcopy_result: bool = False


class _EnvSchemaAnalyzer:
    """Path-sensitive symbolic interpreter for one function AST."""

    def __init__(
        self,
        function: Any,
        *,
        env_parameter: str,
        max_paths: int,
        deepcopy_names: Iterable[str],
    ) -> None:
        self.function = inspect.unwrap(function)
        self.env_parameter = env_parameter
        self.max_paths = max_paths
        self.deepcopy_names = frozenset(deepcopy_names)
        self.function_ast, self.source_start_line = self._extract_function_ast(
            self.function
        )
        self._validate_env_parameter()

    def analyze(self) -> tuple[list[EnvTree], list[FunctionCall]]:
        original_tree = EnvTree(
            tree_id=0,
            root=EnvNode(key=f"<root:{self.env_parameter}>"),
        )
        initial_state = _AnalysisState(
            trees=[original_tree],
            active_aliases={self.env_parameter: NodeRef(0, ())},
            function_calls=[],
            next_tree_id=1,
        )
        final_flows = self._exec_block(
            self.function_ast.body,
            [_Flow(state=initial_state)],
        )
        if not final_flows:
            final_flows = [_Flow(state=initial_state)]

        self._ensure_consistent_outputs(final_flows)
        canonical = final_flows[0].state
        return canonical.trees, canonical.function_calls

    @staticmethod
    def _extract_function_ast(function: Any) -> tuple[ast.FunctionDef | ast.AsyncFunctionDef, int]:
        try:
            source_lines, start_line = inspect.getsourcelines(function)
        except (OSError, IOError, TypeError) as exc:
            raise SourceUnavailableError(
                "Could not recover source for the supplied function handle. "
                "Define it in a .py file, or register notebook-generated source "
                "in linecache before analysis."
            ) from exc

        source = textwrap.dedent("".join(source_lines))
        try:
            module = ast.parse(source)
        except SyntaxError as exc:
            raise SourceUnavailableError("Recovered source could not be parsed.") from exc

        candidates = [
            node
            for node in ast.walk(module)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == getattr(function, "__name__", None)
        ]
        if not candidates:
            raise SourceUnavailableError(
                f"Could not locate AST for function {getattr(function, '__name__', function)!r}."
            )
        # inspect.getsource normally places the requested definition first.
        candidates.sort(key=lambda node: (node.lineno, node.col_offset))
        return candidates[0], start_line

    def _validate_env_parameter(self) -> None:
        args = self.function_ast.args
        parameter_names = {
            arg.arg
            for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]
        }
        if args.vararg is not None:
            parameter_names.add(args.vararg.arg)
        if args.kwarg is not None:
            parameter_names.add(args.kwarg.arg)
        if self.env_parameter not in parameter_names:
            raise SchemaAnalysisError(
                f"Function {self.function_ast.name!r} has no parameter "
                f"named {self.env_parameter!r}."
            )

    def _line(self, node: ast.AST) -> int:
        return self.source_start_line + getattr(node, "lineno", 1) - 1

    def _check_path_count(self, flows: Sequence[_Flow]) -> None:
        if len(flows) > self.max_paths:
            raise PathExplosionError(
                f"Static control-flow expansion produced {len(flows)} paths, "
                f"exceeding max_paths={self.max_paths}."
            )

    def _exec_block(self, statements: Sequence[ast.stmt], flows: list[_Flow]) -> list[_Flow]:
        current = flows
        for statement in statements:
            next_flows: list[_Flow] = []
            for flow in current:
                if flow.status == "normal":
                    next_flows.extend(self._exec_stmt(statement, flow))
                else:
                    next_flows.append(flow)
            self._check_path_count(next_flows)
            current = next_flows
        return current

    def _exec_stmt(self, statement: ast.stmt, flow: _Flow) -> list[_Flow]:
        state = flow.state

        if isinstance(statement, ast.Assign):
            self._handle_assignment(statement.targets, statement.value, state)
            return [flow]

        if isinstance(statement, ast.AnnAssign):
            # A value-less local annotation does not rebind the existing name.
            if statement.value is not None:
                self._handle_assignment([statement.target], statement.value, state)
            if statement.annotation is not None:
                self._process_expr(statement.annotation, state)
            return [flow]

        if isinstance(statement, ast.AugAssign):
            # Per the requested rules, replacing a tracked node is an overwrite,
            # not a read, even though Python's runtime augmented assignment reads.
            self._handle_overwrite_target(statement.target, state)
            self._process_expr(statement.value, state)
            return [flow]

        if isinstance(statement, ast.Expr):
            self._process_expr(statement.value, state)
            return [flow]

        if isinstance(statement, ast.Delete):
            for target in statement.targets:
                self._handle_delete_target(target, state)
            return [flow]

        if isinstance(statement, ast.Return):
            self._process_expr(statement.value, state)
            flow.status = "return"
            return [flow]

        if isinstance(statement, ast.Raise):
            self._process_expr(statement.exc, state)
            self._process_expr(statement.cause, state)
            flow.status = "raise"
            return [flow]

        if isinstance(statement, ast.Assert):
            self._process_expr(statement.test, state)
            self._process_expr(statement.msg, state)
            return [flow]

        if isinstance(statement, ast.If):
            self._process_expr(statement.test, state)
            line = self._line(statement)
            body_flow = flow.branched(f"if@{line}:body")
            else_flow = flow.branched(f"if@{line}:else")
            body_outputs = self._exec_block(statement.body, [body_flow])
            else_outputs = self._exec_block(statement.orelse, [else_flow])
            return [*body_outputs, *else_outputs]

        if isinstance(statement, (ast.For, ast.AsyncFor)):
            return self._exec_for(statement, flow)

        if isinstance(statement, ast.While):
            return self._exec_while(statement, flow)

        if isinstance(statement, (ast.With, ast.AsyncWith)):
            for item in statement.items:
                self._process_expr(item.context_expr, state)
                if item.optional_vars is not None:
                    self._invalidate_target_aliases(item.optional_vars, state)
            return self._exec_block(statement.body, [flow])

        if isinstance(statement, ast.Match):
            return self._exec_match(statement, flow)

        if isinstance(statement, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            return self._exec_try(statement, flow)

        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # A nested function body is not executed when defined. Decorators,
            # defaults, and annotations are evaluated now and therefore count.
            for decorator in statement.decorator_list:
                self._process_expr(decorator, state)
            for default in [*statement.args.defaults, *statement.args.kw_defaults]:
                self._process_expr(default, state)
            for arg in [
                *statement.args.posonlyargs,
                *statement.args.args,
                *statement.args.kwonlyargs,
            ]:
                self._process_expr(arg.annotation, state)
            self._process_expr(statement.returns, state)
            state.active_aliases.pop(statement.name, None)
            return [flow]

        if isinstance(statement, ast.ClassDef):
            for decorator in statement.decorator_list:
                self._process_expr(decorator, state)
            for base in statement.bases:
                self._process_expr(base, state)
            for keyword in statement.keywords:
                self._process_expr(keyword.value, state)
            state.active_aliases.pop(statement.name, None)
            return [flow]

        if isinstance(statement, (ast.Import, ast.ImportFrom)):
            for alias in statement.names:
                bound = alias.asname or alias.name.split(".", 1)[0]
                state.active_aliases.pop(bound, None)
            return [flow]

        if isinstance(statement, ast.Break):
            flow.status = "break"
            return [flow]

        if isinstance(statement, ast.Continue):
            flow.status = "continue"
            return [flow]

        if isinstance(statement, (ast.Pass, ast.Global, ast.Nonlocal)):
            return [flow]

        # Generic fallback: process executable expression children without
        # descending into nested statement blocks a second time.
        for child in ast.iter_child_nodes(statement):
            if isinstance(child, ast.expr):
                self._process_expr(child, state)
        return [flow]

    def _exec_for(self, statement: ast.For | ast.AsyncFor, flow: _Flow) -> list[_Flow]:
        state = flow.state
        self._process_expr(statement.iter, state)
        line = self._line(statement)

        zero = flow.branched(f"for@{line}:zero-iterations")
        one = flow.branched(f"for@{line}:one-or-more-iterations")
        self._invalidate_target_aliases(statement.target, one.state)

        zero_outputs = self._exec_block(statement.orelse, [zero])
        one_outputs = self._exec_block(statement.body, [one])

        normalized: list[_Flow] = []
        for candidate in one_outputs:
            if candidate.status == "break":
                candidate.status = "normal"
                candidate.provenance = (*candidate.provenance, "loop-break")
                normalized.append(candidate)
            elif candidate.status == "continue":
                candidate.status = "normal"
                candidate.provenance = (*candidate.provenance, "loop-continue/exhaust")
                normalized.extend(self._exec_block(statement.orelse, [candidate]))
            elif candidate.status == "normal":
                normalized.extend(self._exec_block(statement.orelse, [candidate]))
            else:
                normalized.append(candidate)
        return [*zero_outputs, *normalized]

    def _exec_while(self, statement: ast.While, flow: _Flow) -> list[_Flow]:
        state = flow.state
        self._process_expr(statement.test, state)
        line = self._line(statement)

        zero = flow.branched(f"while@{line}:zero-iterations")
        one = flow.branched(f"while@{line}:one-or-more-iterations")
        zero_outputs = self._exec_block(statement.orelse, [zero])
        one_outputs = self._exec_block(statement.body, [one])

        normalized: list[_Flow] = []
        for candidate in one_outputs:
            if candidate.status == "break":
                candidate.status = "normal"
                candidate.provenance = (*candidate.provenance, "loop-break")
                normalized.append(candidate)
            elif candidate.status == "continue":
                candidate.status = "normal"
                candidate.provenance = (*candidate.provenance, "loop-continue/exhaust")
                normalized.extend(self._exec_block(statement.orelse, [candidate]))
            elif candidate.status == "normal":
                normalized.extend(self._exec_block(statement.orelse, [candidate]))
            else:
                normalized.append(candidate)
        return [*zero_outputs, *normalized]

    def _exec_match(self, statement: ast.Match, flow: _Flow) -> list[_Flow]:
        self._process_expr(statement.subject, flow.state)
        line = self._line(statement)
        outputs: list[_Flow] = []

        for index, case in enumerate(statement.cases):
            case_flow = flow.branched(f"match@{line}:case-{index}")
            self._invalidate_pattern_bindings(case.pattern, case_flow.state)
            self._process_pattern_values(case.pattern, case_flow.state)
            self._process_expr(case.guard, case_flow.state)
            outputs.extend(self._exec_block(case.body, [case_flow]))

        if not self._match_is_exhaustive(statement.cases):
            outputs.append(flow.branched(f"match@{line}:no-match"))
        return outputs

    def _exec_try(self, statement: ast.Try, flow: _Flow) -> list[_Flow]:
        line = self._line(statement)

        success = flow.branched(f"try@{line}:body-success")
        success_outputs = self._exec_block(statement.body, [success])
        with_else: list[_Flow] = []
        for candidate in success_outputs:
            if candidate.status == "normal":
                with_else.extend(self._exec_block(statement.orelse, [candidate]))
            else:
                with_else.append(candidate)

        handler_outputs: list[_Flow] = []
        for index, handler in enumerate(statement.handlers):
            handler_flow = flow.branched(f"try@{line}:except-{index}")
            self._process_expr(handler.type, handler_flow.state)
            if handler.name:
                handler_flow.state.active_aliases.pop(handler.name, None)
            handler_outputs.extend(self._exec_block(handler.body, [handler_flow]))

        alternatives = [*with_else, *handler_outputs]
        if not statement.handlers:
            alternatives = with_else

        if statement.finalbody:
            alternatives = self._apply_finally(statement.finalbody, alternatives)
        return alternatives

    def _apply_finally(
        self, finalbody: Sequence[ast.stmt], flows: Sequence[_Flow]
    ) -> list[_Flow]:
        outputs: list[_Flow] = []
        for flow in flows:
            incoming_status = flow.status
            temporary = _Flow(
                state=flow.state,
                status="normal",
                provenance=(*flow.provenance, "finally"),
            )
            final_outputs = self._exec_block(finalbody, [temporary])
            for candidate in final_outputs:
                if candidate.status == "normal":
                    candidate.status = incoming_status
                outputs.append(candidate)
        return outputs

    def _handle_assignment(
        self,
        targets: Sequence[ast.expr],
        value: ast.expr,
        state: _AnalysisState,
    ) -> None:
        # Python evaluates the RHS before rebinding name targets. Resolve it
        # first so expressions such as ``alias = alias + 1`` still see the old
        # active alias. Exact tracked references are classified without usage.
        reference = self._eval_reference(value, state)
        all_simple_names = all(isinstance(target, ast.Name) for target in targets)

        if all_simple_names:
            if reference is None:
                self._process_expr(value, state)
                for target in targets:
                    assert isinstance(target, ast.Name)
                    state.active_aliases.pop(target.id, None)
            else:
                # Pure name-to-node assignment is alias creation and never use.
                for target in targets:
                    assert isinstance(target, ast.Name)
                    state.active_aliases[target.id] = reference.ref
            return

        # Pre-mark only dictionary slots that are already known aliases. This
        # suppresses reads of the overwritten node on the same line without
        # prematurely rebinding names in tuple/list targets before the RHS.
        premarked_targets: set[int] = set()
        for target in targets:
            if not isinstance(target, ast.Name):
                self._premark_assignment_overwrites(
                    target, state, premarked_targets
                )

        if reference is None:
            self._process_expr(value, state)
        elif not reference.pure_deepcopy_result:
            state.node(reference.ref).mark_used()

        # Python applies assignment targets after evaluating the RHS. Direct
        # name targets can become aliases; names nested in unpacking targets
        # cannot be assumed to receive the complete tracked value.
        for target in targets:
            if isinstance(target, ast.Name):
                if reference is None:
                    state.active_aliases.pop(target.id, None)
                else:
                    state.active_aliases[target.id] = reference.ref
            else:
                self._finish_assignment_target(
                    target, state, premarked_targets
                )

    def _premark_assignment_overwrites(
        self,
        target: ast.expr,
        state: _AnalysisState,
        premarked_targets: set[int],
    ) -> None:
        """Mark tracked slot targets before RHS usage analysis."""
        if isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._premark_assignment_overwrites(
                    element, state, premarked_targets
                )
            return
        if isinstance(target, ast.Starred):
            self._premark_assignment_overwrites(
                target.value, state, premarked_targets
            )
            return
        if isinstance(target, ast.Subscript):
            reference = self._eval_reference(target, state)
            if reference is not None:
                state.node(reference.ref).mark_overwritten()
                premarked_targets.add(id(target))

    def _finish_assignment_target(
        self,
        target: ast.expr,
        state: _AnalysisState,
        premarked_targets: set[int],
    ) -> None:
        """Apply a non-name assignment target after RHS evaluation."""
        if isinstance(target, ast.Name):
            state.active_aliases.pop(target.id, None)
            return
        if isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._finish_assignment_target(
                    element, state, premarked_targets
                )
            return
        if isinstance(target, ast.Starred):
            self._finish_assignment_target(
                target.value, state, premarked_targets
            )
            return
        if isinstance(target, ast.Subscript):
            if id(target) in premarked_targets:
                return
            reference = self._eval_reference(target, state)
            if reference is not None:
                state.node(reference.ref).mark_overwritten()
            else:
                self._process_expr(target.value, state)
                self._process_expr(target.slice, state)
            return
        if isinstance(target, ast.Attribute):
            self._process_expr(target.value, state)
            return
        self._invalidate_target_aliases(target, state)

    def _handle_overwrite_target(self, target: ast.expr, state: _AnalysisState) -> None:
        if isinstance(target, ast.Name):
            # Rebinding a Python variable overrides an alias; it does not mutate
            # the symbolic dictionary node to which the alias used to point.
            state.active_aliases.pop(target.id, None)
            return

        if isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._handle_overwrite_target(element, state)
            return

        if isinstance(target, ast.Starred):
            self._handle_overwrite_target(target.value, state)
            return

        if isinstance(target, ast.Subscript):
            reference = self._eval_reference(target, state)
            if reference is not None:
                state.node(reference.ref).mark_overwritten()
                return
            # Untracked container assignment can still use env in its base/key.
            self._process_expr(target.value, state)
            self._process_expr(target.slice, state)
            return

        if isinstance(target, ast.Attribute):
            # Assigning an attribute mutates the object stored in a node, not
            # the dictionary slot itself, so the base expression is a usage.
            self._process_expr(target.value, state)
            return

        self._invalidate_target_aliases(target, state)

    def _handle_delete_target(self, target: ast.expr, state: _AnalysisState) -> None:
        if isinstance(target, ast.Name):
            state.active_aliases.pop(target.id, None)
            return
        if isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._handle_delete_target(element, state)
            return
        if isinstance(target, ast.Subscript):
            reference = self._eval_reference(target, state)
            if reference is not None:
                state.node(reference.ref).mark_overwritten()
                return
        self._handle_overwrite_target(target, state)

    def _invalidate_target_aliases(self, target: ast.AST, state: _AnalysisState) -> None:
        if isinstance(target, ast.Name):
            state.active_aliases.pop(target.id, None)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._invalidate_target_aliases(element, state)
        elif isinstance(target, ast.Starred):
            self._invalidate_target_aliases(target.value, state)
        elif isinstance(target, ast.Subscript):
            self._handle_overwrite_target(target, state)
        elif isinstance(target, ast.Attribute):
            self._process_expr(target.value, state)

    def _process_expr(self, expression: ast.AST | None, state: _AnalysisState) -> None:
        if expression is None:
            return

        if isinstance(expression, ast.NamedExpr):
            reference = self._eval_reference(expression.value, state)
            if isinstance(expression.target, ast.Name):
                if reference is None:
                    # The old alias remains visible while the value is evaluated.
                    self._process_expr(expression.value, state)
                    state.active_aliases.pop(expression.target.id, None)
                else:
                    state.active_aliases[expression.target.id] = reference.ref
            else:
                self._handle_overwrite_target(expression.target, state)
                if reference is None:
                    self._process_expr(expression.value, state)
                elif not reference.pure_deepcopy_result:
                    state.node(reference.ref).mark_used()
            return

        reference = self._eval_reference(expression, state)
        if reference is not None:
            if not reference.pure_deepcopy_result:
                state.node(reference.ref).mark_used()
            return

        if isinstance(expression, ast.Call):
            self._process_call(expression, state)
            return

        if isinstance(expression, ast.Lambda):
            # Merely creating a lambda does not execute its body.
            return

        if isinstance(expression, ast.IfExp):
            # Expression-level branching is analyzed conservatively in place.
            # The statement-level path engine handles the control-flow forms
            # explicitly required by the API.
            self._process_expr(expression.test, state)
            left = state.clone()
            right = state.clone()
            self._process_expr(expression.body, left)
            self._process_expr(expression.orelse, right)
            self._merge_expression_branches(state, left, right, expression)
            return

        if isinstance(expression, ast.BoolOp):
            # Every operand is a possible evaluated operand. Analyze each so no
            # access is silently omitted; statement-level branching remains the
            # source of DynamicSchemaError path diagnostics.
            for value in expression.values:
                self._process_expr(value, state)
            return

        if isinstance(expression, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
            self._process_comprehension(expression, state)
            return

        if isinstance(expression, ast.DictComp):
            for generator in expression.generators:
                self._process_expr(generator.iter, state)
                for condition in generator.ifs:
                    self._process_expr(condition, state)
            self._process_expr(expression.key, state)
            self._process_expr(expression.value, state)
            return

        # For regular expressions, recursively analyze expression children. A
        # nested maximal env reference is consumed by the recursive call, so a
        # chain such as env['a']['b'] marks only the final node, not 'a'.
        for child in ast.iter_child_nodes(expression):
            if isinstance(child, ast.expr):
                self._process_expr(child, state)
            elif isinstance(child, ast.comprehension):
                self._process_expr(child.iter, state)
                for condition in child.ifs:
                    self._process_expr(condition, state)

    def _process_comprehension(
        self,
        expression: ast.ListComp | ast.SetComp | ast.GeneratorExp,
        state: _AnalysisState,
    ) -> None:
        for generator in expression.generators:
            self._process_expr(generator.iter, state)
            for condition in generator.ifs:
                self._process_expr(condition, state)
        self._process_expr(expression.elt, state)

    def _merge_expression_branches(
        self,
        destination: _AnalysisState,
        left: _AnalysisState,
        right: _AnalysisState,
        expression: ast.IfExp,
    ) -> None:
        left_snapshot = left.snapshot()
        right_snapshot = right.snapshot()
        if left_snapshot != right_snapshot:
            diff = _make_diff(left_snapshot, right_snapshot)
            line = self._line(expression)
            details = [
                {
                    "baseline_paths": [f"if-expression@{line}:body"],
                    "compared_paths": [f"if-expression@{line}:else"],
                    "diff": diff,
                }
            ]
            error = DynamicSchemaError(details)
            logger.error("%s", error)
            raise error
        replacement = left.clone()
        destination.trees = replacement.trees
        destination.active_aliases = replacement.active_aliases
        destination.function_calls = replacement.function_calls
        destination.next_tree_id = replacement.next_tree_id

    def _process_call(self, call: ast.Call, state: _AnalysisState) -> None:
        function_name = _call_name(call.func)

        # The callable expression itself may use a tracked node, e.g.
        # env['factory'](...). It is not a root argument.
        self._process_expr(call.func, state)

        for argument in call.args:
            if isinstance(argument, ast.Starred):
                self._process_expr(argument.value, state)
                continue
            self._process_call_argument(argument, function_name, state)

        for keyword in call.keywords:
            if keyword.arg is None:  # **mapping
                self._process_expr(keyword.value, state)
            else:
                self._process_call_argument(keyword.value, function_name, state)

    def _process_call_argument(
        self,
        argument: ast.expr,
        function_name: str,
        state: _AnalysisState,
    ) -> None:
        reference = self._eval_reference(argument, state)
        if reference is None:
            self._process_expr(argument, state)
            return

        if reference.ref.path == ():
            tree = state.tree(reference.ref.tree_id)
            state.function_calls.append(
                FunctionCall(function_name, tree.reduced_overwrite_copy())
            )
            return

        # Only roots receive the special FunctionCall treatment. Passing a branch
        # is a normal use of that branch.
        if not reference.pure_deepcopy_result:
            state.node(reference.ref).mark_used()

    def _eval_reference(
        self, expression: ast.AST, state: _AnalysisState
    ) -> _ReferenceResult | None:
        """Resolve an expression that evaluates exactly to a tracked node.

        This method builds missing schema nodes but never marks usage. It may
        create a new independent EnvTree when the expression is a recognized
        ``deepcopy`` call.
        """
        if isinstance(expression, ast.Name):
            ref = state.active_aliases.get(expression.id)
            return None if ref is None else _ReferenceResult(ref)

        if isinstance(expression, ast.NamedExpr) and isinstance(
            expression.target, ast.Name
        ):
            value = self._eval_reference(expression.value, state)
            if value is None:
                return None
            state.active_aliases[expression.target.id] = value.ref
            return value

        if isinstance(expression, ast.Subscript):
            base = self._eval_reference(expression.value, state)
            if base is None:
                return None
            key = _literal_key(expression.slice, line=self._line(expression))
            parent = state.node(base.ref)
            parent.ensure_child(key)
            return _ReferenceResult(
                NodeRef(base.ref.tree_id, (*base.ref.path, key)),
                pure_deepcopy_result=False,
            )

        if isinstance(expression, ast.Call):
            if self._is_deepcopy_call(expression):
                if not expression.args:
                    return None
                source = self._eval_reference(expression.args[0], state)
                if source is None:
                    return None
                # Additional arguments are still direct arguments to deepcopy,
                # so apply the ordinary root-pass rule to each of them.
                deepcopy_name = _call_name(expression.func)
                for extra in expression.args[1:]:
                    if isinstance(extra, ast.Starred):
                        self._process_expr(extra.value, state)
                    else:
                        self._process_call_argument(extra, deepcopy_name, state)
                for keyword in expression.keywords:
                    if keyword.arg is None:
                        self._process_expr(keyword.value, state)
                    else:
                        self._process_call_argument(
                            keyword.value, deepcopy_name, state
                        )
                # deepcopy is itself a function call. Under the requested rule,
                # passing a tracked root records a FunctionCall in addition to
                # creating the independent copied tree. Deepcopying a branch is
                # not a root pass and therefore creates no FunctionCall.
                if source.ref.path == ():
                    source_tree = state.tree(source.ref.tree_id)
                    state.function_calls.append(
                        FunctionCall(
                            deepcopy_name,
                            source_tree.reduced_overwrite_copy(),
                        )
                    )
                new_ref = self._create_deepcopy(source.ref, state)
                return _ReferenceResult(new_ref, pure_deepcopy_result=True)

            if self._is_tracked_get_call(expression):
                attribute = expression.func
                assert isinstance(attribute, ast.Attribute)
                base = self._eval_reference(attribute.value, state)
                if base is None:
                    return None
                key = _literal_key(expression.args[0], line=self._line(expression))
                state.node(base.ref).ensure_child(key)
                return _ReferenceResult(
                    NodeRef(base.ref.tree_id, (*base.ref.path, key)),
                    pure_deepcopy_result=False,
                )

        return None

    def _create_deepcopy(self, source_ref: NodeRef, state: _AnalysisState) -> NodeRef:
        source_node = state.node(source_ref)
        new_id = state.next_tree_id
        state.next_tree_id += 1
        new_tree = EnvTree(
            tree_id=new_id,
            root=source_node.clone(parent=None),
            copied_from=CopyOrigin(source_ref.tree_id, source_ref.path),
        )
        # IDs are deliberately contiguous so list index and tree_id coincide.
        if new_id != len(state.trees):
            raise SchemaAnalysisError(
                "Internal tree-id invariant failed while creating deepcopy."
            )
        state.trees.append(new_tree)
        return NodeRef(new_id, ())

    def _is_deepcopy_call(self, call: ast.Call) -> bool:
        return _call_name(call.func) in self.deepcopy_names

    def _is_tracked_get_call(self, call: ast.Call) -> bool:
        return (
            isinstance(call.func, ast.Attribute)
            and call.func.attr == "get"
            and len(call.args) == 1
            and not call.keywords
            and self._eval_reference_without_side_effects(call.func.value) is not None
        )

    def _eval_reference_without_side_effects(self, expression: ast.AST) -> bool | None:
        """Syntactic precheck used only to recognize potential tracked .get calls."""
        if isinstance(expression, ast.Name):
            return True
        if isinstance(expression, ast.Subscript):
            return self._eval_reference_without_side_effects(expression.value)
        if isinstance(expression, ast.Call) and self._is_deepcopy_call(expression):
            return True
        return None

    def _invalidate_pattern_bindings(
        self, pattern: ast.pattern, state: _AnalysisState
    ) -> None:
        for name in _pattern_bound_names(pattern):
            state.active_aliases.pop(name, None)

    def _process_pattern_values(
        self, pattern: ast.pattern, state: _AnalysisState
    ) -> None:
        if isinstance(pattern, ast.MatchValue):
            self._process_expr(pattern.value, state)
        elif isinstance(pattern, ast.MatchClass):
            self._process_expr(pattern.cls, state)
            for child in [*pattern.patterns, *pattern.kwd_patterns]:
                self._process_pattern_values(child, state)
        elif isinstance(pattern, ast.MatchMapping):
            for key in pattern.keys:
                self._process_expr(key, state)
            for child in pattern.patterns:
                self._process_pattern_values(child, state)
        elif isinstance(pattern, ast.MatchSequence):
            for child in pattern.patterns:
                self._process_pattern_values(child, state)
        elif isinstance(pattern, ast.MatchOr):
            for child in pattern.patterns:
                self._process_pattern_values(child, state)
        elif isinstance(pattern, ast.MatchAs) and pattern.pattern is not None:
            self._process_pattern_values(pattern.pattern, state)

    @staticmethod
    def _match_is_exhaustive(cases: Sequence[ast.match_case]) -> bool:
        if not cases:
            return False
        last = cases[-1]
        if last.guard is not None:
            return False
        pattern = last.pattern
        # ``case _`` and an unguarded capture pattern are irrefutable.
        return isinstance(pattern, ast.MatchAs) and pattern.pattern is None

    def _ensure_consistent_outputs(self, flows: Sequence[_Flow]) -> None:
        groups: list[dict[str, Any]] = []
        for flow in flows:
            snapshot = flow.state.snapshot()
            matching = next(
                (group for group in groups if group["snapshot"] == snapshot), None
            )
            if matching is None:
                groups.append({"snapshot": snapshot, "paths": [flow.label]})
            else:
                matching["paths"].append(flow.label)

        if len(groups) <= 1:
            return

        baseline = groups[0]
        differences: list[dict[str, Any]] = []
        for group in groups[1:]:
            differences.append(
                {
                    "baseline_paths": baseline["paths"],
                    "compared_paths": group["paths"],
                    "diff": _make_diff(baseline["snapshot"], group["snapshot"]),
                }
            )

        error = DynamicSchemaError(differences)
        logger.error("%s", error)
        raise error


def analyze_env_schema(
    function: Any,
    *,
    env_parameter: str = "env",
    max_paths: int = 256,
    deepcopy_names: Iterable[str] = ("copy.deepcopy", "deepcopy"),
) -> tuple[list[EnvTree], list[FunctionCall]]:
    """Statically analyze how a function accesses an env dictionary tree.

    Parameters
    ----------
    function
        Function or unbound-method handle whose source can be recovered with
        :mod:`inspect`.
    env_parameter
        Name of the parameter representing the root dictionary.
    max_paths
        Maximum number of symbolic execution paths allowed after expanding
        control flow.
    deepcopy_names
        Static call names treated as deep-copy operations. The defaults support
        both ``copy.deepcopy(env)`` and ``from copy import deepcopy``.

    Returns
    -------
    trees, function_calls
        ``trees`` contains the original symbolic env plus every independently
        tracked deep copy. ``function_calls`` contains snapshots for every direct
        function argument that is a tracked tree root.

    Raises
    ------
    DynamicSchemaError
        If different logical execution paths finish with different tree/call
        outputs.
    DynamicKeyError
        If a tracked dictionary access uses a non-literal key.
    SourceUnavailableError
        If source code cannot be recovered from the function handle.

    Notes
    -----
    The analyzer recognizes literal subscription keys and ``tracked.get(key)``
    with exactly one literal argument. It treats direct root arguments specially
    as requested; branches passed to functions are ordinary uses. Loops are
    conservatively expanded as zero iterations versus one-or-more iterations.
    """
    analyzer = _EnvSchemaAnalyzer(
        function,
        env_parameter=env_parameter,
        max_paths=max_paths,
        deepcopy_names=deepcopy_names,
    )
    return analyzer.analyze()


def analysis_to_dict(
    trees: Sequence[EnvTree], calls: Sequence[FunctionCall]
) -> dict[str, Any]:
    """Convert analyzer output to plain deterministic dictionaries."""
    return {
        "trees": [tree.to_dict() for tree in trees],
        "function_calls": [call.to_dict() for call in calls],
    }


def print_analysis(trees: Sequence[EnvTree], calls: Sequence[FunctionCall]) -> None:
    """Pretty-print analyzer output."""
    print(json.dumps(analysis_to_dict(trees, calls), indent=2, default=repr))


def _literal_key(node: ast.AST, *, line: int) -> Hashable:
    try:
        key = ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError) as exc:
        expression = ast.unparse(node) if hasattr(ast, "unparse") else ast.dump(node)
        raise DynamicKeyError(
            f"Env key at line {line} is not statically literal: {expression}"
        ) from exc
    try:
        hash(key)
    except TypeError as exc:
        raise DynamicKeyError(
            f"Env key at line {line} is not hashable: {key!r}"
        ) from exc
    return key


def _call_name(function: ast.expr) -> str:
    if isinstance(function, ast.Name):
        return function.id
    if isinstance(function, ast.Attribute):
        parts: list[str] = []
        current: ast.AST = function
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
            return ".".join(reversed(parts))
    try:
        return ast.unparse(function)
    except Exception:  # pragma: no cover - ast.unparse is available on 3.9+.
        return ast.dump(function, include_attributes=False)


def _pattern_bound_names(pattern: ast.pattern) -> set[str]:
    names: set[str] = set()
    if isinstance(pattern, ast.MatchAs):
        if pattern.name is not None:
            names.add(pattern.name)
        if pattern.pattern is not None:
            names.update(_pattern_bound_names(pattern.pattern))
    elif isinstance(pattern, ast.MatchStar):
        if pattern.name is not None:
            names.add(pattern.name)
    elif isinstance(pattern, ast.MatchMapping):
        if pattern.rest is not None:
            names.add(pattern.rest)
        for child in pattern.patterns:
            names.update(_pattern_bound_names(child))
    elif isinstance(pattern, ast.MatchSequence):
        for child in pattern.patterns:
            names.update(_pattern_bound_names(child))
    elif isinstance(pattern, ast.MatchClass):
        for child in [*pattern.patterns, *pattern.kwd_patterns]:
            names.update(_pattern_bound_names(child))
    elif isinstance(pattern, ast.MatchOr):
        for child in pattern.patterns:
            names.update(_pattern_bound_names(child))
    return names


def _key_sort_token(key: Hashable) -> tuple[str, str]:
    return type(key).__name__, repr(key)


def _encode_key(key: Hashable) -> dict[str, str]:
    return {"type": type(key).__name__, "repr": repr(key)}


def _make_diff(left: Any, right: Any) -> Any:
    if DeepDiff is not None:
        diff = DeepDiff(left, right, ignore_order=False, verbose_level=2)
        try:
            return diff.to_dict()
        except AttributeError:  # pragma: no cover - compatibility fallback.
            return dict(diff)
    return {"fallback_structural_diff": _structural_diff(left, right)}


def _structural_diff(left: Any, right: Any, path: str = "root") -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    if type(left) is not type(right):
        return [
            {
                "path": path,
                "kind": "type_changed",
                "left": type(left).__name__,
                "right": type(right).__name__,
            }
        ]

    if isinstance(left, dict):
        left_keys = set(left)
        right_keys = set(right)
        for key in sorted(left_keys - right_keys, key=repr):
            differences.append(
                {"path": f"{path}[{key!r}]", "kind": "removed", "left": left[key]}
            )
        for key in sorted(right_keys - left_keys, key=repr):
            differences.append(
                {"path": f"{path}[{key!r}]", "kind": "added", "right": right[key]}
            )
        for key in sorted(left_keys & right_keys, key=repr):
            differences.extend(
                _structural_diff(left[key], right[key], f"{path}[{key!r}]")
            )
        return differences

    if isinstance(left, list):
        common = min(len(left), len(right))
        for index in range(common):
            differences.extend(
                _structural_diff(left[index], right[index], f"{path}[{index}]")
            )
        for index in range(common, len(left)):
            differences.append(
                {"path": f"{path}[{index}]", "kind": "removed", "left": left[index]}
            )
        for index in range(common, len(right)):
            differences.append(
                {"path": f"{path}[{index}]", "kind": "added", "right": right[index]}
            )
        return differences

    if left != right:
        differences.append(
            {"path": path, "kind": "value_changed", "left": left, "right": right}
        )
    return differences




class FunctionalNormalizer(ast.NodeTransformer):
    """
    Normalize a Python function AST by removing source-level details that
    do not affect its basic computational structure.
    """

    def visit_FunctionDef(self, node):
        # Function name itself does not affect the function body.
        node.name = "<function>"

        # Decorators affect how the function object is constructed, but if
        # we're comparing the function implementation itself, ignore them.
        node.decorator_list = []

        # Remove docstring.
        if (
            node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        ):
            node.body.pop(0)

        self.generic_visit(node)
        return node

    def visit_AsyncFunctionDef(self, node):
        node.name = "<function>"
        node.decorator_list = []

        if (
            node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        ):
            node.body.pop(0)

        self.generic_visit(node)
        return node


def normalized_function_ast(func):
    """
    Return a normalized AST representation of a function.

    Ignores:
        - Whitespace
        - Comments
        - Formatting
        - Function name
        - Docstring
        - Decorators
        - Source locations / line numbers

    Parameters
    ----------
    func : callable
        Function handle to normalize.

    Returns
    -------
    str
        Canonical AST representation of the function.
    """
    source = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(source)

    tree = FunctionalNormalizer().visit(tree)
    ast.fix_missing_locations(tree)

    return ast.dump(
        tree,
        annotate_fields=True,
        include_attributes=False,
    )