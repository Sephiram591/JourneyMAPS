from __future__ import annotations

import ast
import inspect
import textwrap
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence


class RunBranchConsistencyError(RuntimeError):
    """Raised when ``_run`` has path-dependent env or journey usage."""

    def __init__(self, analysis: dict[str, Any]) -> None:
        violations = analysis["violations"]

        self.violating_env_accesses = list(violations["env_leaves"])
        self.violating_journey_paths = list(violations["journey_paths"])
        self.violating_unresolved_env_accesses = list(
            violations["unresolved_env_accesses"]
        )
        self.violating_unresolved_journey_paths = list(
            violations["unresolved_journey_paths"]
        )
        self.analysis = analysis

        sections = ["AST branch-consistency inspection of _run failed."]
        groups = (
            (
                "Environment accesses not present on every path:",
                self.violating_env_accesses,
            ),
            (
                "journey.run path names not present on every path:",
                self.violating_journey_paths,
            ),
            (
                "Unresolved environment accesses not present on every path:",
                self.violating_unresolved_env_accesses,
            ),
            (
                "Unresolved journey.run path expressions not present on "
                "every path:",
                self.violating_unresolved_journey_paths,
            ),
        )

        for heading, values in groups:
            if values:
                sections.append(heading)
                sections.extend(f"  - {value}" for value in values)

        super().__init__("\n".join(sections))


@dataclass(frozen=True, order=True)
class _Event:
    """A semantic dependency found in ``_run``."""

    kind: str
    name: str


@dataclass(frozen=True)
class _Summary:
    """Events that may occur and must occur over a collection of paths."""

    exists: bool
    may: frozenset[_Event] = frozenset()
    must: frozenset[_Event] = frozenset()


_NONE = _Summary(False)
_EMPTY = _Summary(True)
_FLOW_KINDS = (
    "normal",
    "return",
    "raise",
    "break",
    "continue",
    "nonterminating",
)


def _alternate(*summaries: _Summary) -> _Summary:
    """Combine alternative paths."""
    present = [summary for summary in summaries if summary.exists]
    if not present:
        return _NONE

    may: set[_Event] = set()
    must = set(present[0].must)
    for summary in present:
        may.update(summary.may)
        must.intersection_update(summary.must)

    return _Summary(True, frozenset(may), frozenset(must))


def _sequence(first: _Summary, second: _Summary) -> _Summary:
    """Combine two pieces of control flow that execute sequentially."""
    if not first.exists or not second.exists:
        return _NONE

    return _Summary(
        True,
        first.may | second.may,
        first.must | second.must,
    )


def _event_summary(event: _Event | None) -> _Summary:
    if event is None:
        return _EMPTY
    events = frozenset((event,))
    return _Summary(True, events, events)


def _empty_flow() -> dict[str, _Summary]:
    return {kind: _NONE for kind in _FLOW_KINDS}


def _normal_flow(summary: _Summary = _EMPTY) -> dict[str, _Summary]:
    flow = _empty_flow()
    flow["normal"] = summary
    return flow


def _single_flow(kind: str, summary: _Summary) -> dict[str, _Summary]:
    flow = _empty_flow()
    flow[kind] = summary
    return flow


def _merge_flows(*flows: dict[str, _Summary]) -> dict[str, _Summary]:
    return {
        kind: _alternate(*(flow[kind] for flow in flows))
        for kind in _FLOW_KINDS
    }


def _prepend(prefix: _Summary, flow: dict[str, _Summary]) -> dict[str, _Summary]:
    return {
        kind: _sequence(prefix, summary)
        for kind, summary in flow.items()
    }


@dataclass(frozen=True)
class _AliasValue:
    """Possible canonical ``env`` prefixes represented by a local name."""

    paths: frozenset[tuple[str, ...]]
    unresolved: bool = False
    definite: bool = True


def _merge_alias_maps(
    maps: Sequence[dict[str, _AliasValue]],
) -> dict[str, _AliasValue]:
    """Merge aliases from alternative control-flow paths conservatively."""
    if not maps:
        return {}

    merged: dict[str, _AliasValue] = {}
    all_names = set().union(*(mapping.keys() for mapping in maps))

    for name in all_names:
        values = [mapping.get(name) for mapping in maps]
        present = [value for value in values if value is not None]

        if not present:
            continue

        paths = frozenset().union(*(value.paths for value in present))
        unresolved = any(value.unresolved for value in present)
        definite = (
            len(present) == len(values)
            and all(value.definite for value in present)
        )

        if paths or unresolved:
            merged[name] = _AliasValue(
                paths=paths,
                unresolved=unresolved,
                definite=definite,
            )

    return merged


class _EnvAliasAnnotator:
    """
    Attach path-sensitive ``env`` alias information to local-name loads.

    The pass runs before dependency analysis.  Every time a local variable is
    assigned ``env``, an existing env alias, or a branch rooted at either, the
    variable is associated with the corresponding canonical env prefix.  The
    association is followed transitively for an arbitrary number of alias
    assignments.

    Alternative control-flow paths are merged conservatively.  If an alias can
    refer to different env branches, all possible canonical prefixes are kept;
    the later dependency analyzer therefore marks each resulting leaf as a
    may-access rather than a must-access.
    """

    def __init__(self, function: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._function = function

    def annotate(self) -> None:
        """Annotate the function body in execution order."""
        self._block(
            self._function.body,
            {"env": _AliasValue(frozenset(((),)))},
        )

    @staticmethod
    def _literal_string(node: ast.AST | None) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    @staticmethod
    def _literal_truth(node: ast.AST) -> bool | None:
        if isinstance(node, ast.Constant):
            return bool(node.value)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            if not node.elts:
                return False
            if any(not isinstance(element, ast.Starred) for element in node.elts):
                return True
            return None
        if isinstance(node, ast.Dict):
            if not node.keys:
                return False
            if any(key is not None for key in node.keys):
                return True
            return None
        return None

    @staticmethod
    def _append_key(
        base: _AliasValue,
        key: str | None,
    ) -> _AliasValue:
        if key is None:
            return _AliasValue(
                paths=frozenset(),
                unresolved=True,
                definite=base.definite,
            )

        return _AliasValue(
            paths=frozenset(path + (key,) for path in base.paths),
            unresolved=base.unresolved,
            definite=base.definite,
        )

    def _resolve(
        self,
        node: ast.AST,
        aliases: dict[str, _AliasValue],
    ) -> _AliasValue | None:
        """Resolve an expression that is itself an env object or branch."""
        if isinstance(node, ast.Name):
            return aliases.get(node.id)

        if isinstance(node, ast.Subscript):
            base = self._resolve(node.value, aliases)
            if base is None:
                return None
            return self._append_key(base, self._literal_string(node.slice))

        if isinstance(node, ast.Call):
            # env_alias.get("key")
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
            ):
                base = self._resolve(node.func.value, aliases)
                if base is None:
                    return None
                key_node = node.args[0] if node.args else next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "key"
                    ),
                    None,
                )
                return self._append_key(
                    base,
                    self._literal_string(key_node),
                )

            # env_alias.copy()
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "copy"
                and not node.args
                and not node.keywords
            ):
                return self._resolve(node.func.value, aliases)

            # copy.copy(env_alias) and copy.deepcopy(env_alias)
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "copy"
                and node.func.attr in {"copy", "deepcopy"}
                and len(node.args) == 1
                and not node.keywords
            ):
                return self._resolve(node.args[0], aliases)

            # dict(env_alias)
            if (
                isinstance(node.func, ast.Name)
                and node.func.id == "dict"
                and len(node.args) == 1
                and not node.keywords
            ):
                return self._resolve(node.args[0], aliases)

        return None

    @staticmethod
    def _bound_names(node: ast.AST) -> set[str]:
        names: set[str] = set()

        def visit(target: ast.AST) -> None:
            if isinstance(target, ast.Name):
                names.add(target.id)
            elif isinstance(target, (ast.Tuple, ast.List)):
                for element in target.elts:
                    visit(element)
            elif isinstance(target, ast.Starred):
                visit(target.value)

        visit(node)
        return names

    def _invalidate_target(
        self,
        node: ast.AST,
        aliases: dict[str, _AliasValue],
    ) -> None:
        for name in self._bound_names(node):
            aliases.pop(name, None)

    def _bind_target(
        self,
        target: ast.AST,
        value: ast.AST,
        aliases: dict[str, _AliasValue],
    ) -> None:
        """Update aliases after one assignment target is written."""
        if isinstance(target, ast.Name):
            resolved = self._resolve(value, aliases)
            if resolved is None:
                aliases.pop(target.id, None)
            else:
                aliases[target.id] = resolved
            return

        if (
            isinstance(target, (ast.Tuple, ast.List))
            and isinstance(value, (ast.Tuple, ast.List))
            and len(target.elts) == len(value.elts)
        ):
            for child_target, child_value in zip(target.elts, value.elts):
                self._bind_target(child_target, child_value, aliases)
            return

        self._invalidate_target(target, aliases)

    def _annotate_name(
        self,
        node: ast.Name,
        aliases: dict[str, _AliasValue],
    ) -> None:
        if isinstance(node.ctx, ast.Load):
            alias = aliases.get(node.id)
            if alias is not None:
                setattr(node, "_env_alias_value", alias)

    def _expr(
        self,
        node: ast.AST | None,
        aliases: dict[str, _AliasValue],
    ) -> dict[str, _AliasValue]:
        """Annotate an expression and return aliases after its evaluation."""
        if node is None:
            return aliases

        if isinstance(node, ast.Name):
            self._annotate_name(node, aliases)
            return aliases

        if isinstance(node, ast.Constant):
            return aliases

        if isinstance(node, ast.NamedExpr):
            aliases = self._expr(node.value, aliases)
            self._bind_target(node.target, node.value, aliases)
            return aliases

        if isinstance(node, ast.IfExp):
            tested = self._expr(node.test, aliases)
            truth = self._literal_truth(node.test)

            if truth is True:
                return self._expr(node.body, dict(tested))
            if truth is False:
                return self._expr(node.orelse, dict(tested))

            body_aliases = self._expr(node.body, dict(tested))
            else_aliases = self._expr(node.orelse, dict(tested))
            return _merge_alias_maps((body_aliases, else_aliases))

        if isinstance(node, ast.BoolOp):
            if not node.values:
                return aliases

            current = self._expr(node.values[0], aliases)
            for value in node.values[1:]:
                evaluated = self._expr(value, dict(current))
                current = _merge_alias_maps((current, evaluated))
            return current

        if isinstance(node, ast.Compare):
            current = self._expr(node.left, aliases)
            for index, comparator in enumerate(node.comparators):
                evaluated = self._expr(comparator, dict(current))
                current = (
                    evaluated
                    if index == 0
                    else _merge_alias_maps((current, evaluated))
                )
            return current

        if isinstance(node, ast.Lambda):
            for default in node.args.defaults:
                aliases = self._expr(default, aliases)
            for default in node.args.kw_defaults:
                aliases = self._expr(default, aliases)
            return aliases

        if isinstance(
            node,
            (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp),
        ):
            local = dict(aliases)
            for generator in node.generators:
                local = self._expr(generator.iter, local)
                self._invalidate_target(generator.target, local)
                for condition in generator.ifs:
                    local = self._expr(condition, local)

            if isinstance(node, ast.DictComp):
                local = self._expr(node.key, local)
                self._expr(node.value, local)
            else:
                self._expr(node.elt, local)

            # Comprehension induction variables do not leak into the enclosing
            # scope.  The outer iterable expressions above have still been
            # annotated correctly.
            return aliases

        # Evaluate child expressions in AST field order.  Statement and pattern
        # children are handled by the statement walker instead.
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.expr):
                aliases = self._expr(child, aliases)
            elif isinstance(child, ast.keyword):
                aliases = self._expr(child.value, aliases)
            elif isinstance(child, ast.comprehension):
                aliases = self._expr(child.iter, aliases)

        return aliases

    def _pattern(
        self,
        node: ast.pattern,
        aliases: dict[str, _AliasValue],
    ) -> dict[str, _AliasValue]:
        if isinstance(node, ast.MatchValue):
            return self._expr(node.value, aliases)
        if isinstance(node, ast.MatchMapping):
            for key in node.keys:
                aliases = self._expr(key, aliases)
            for pattern in node.patterns:
                aliases = self._pattern(pattern, aliases)
            if node.rest is not None:
                aliases.pop(node.rest, None)
            return aliases
        if isinstance(node, ast.MatchClass):
            aliases = self._expr(node.cls, aliases)
            for pattern in (*node.patterns, *node.kwd_patterns):
                aliases = self._pattern(pattern, aliases)
            return aliases
        if isinstance(node, (ast.MatchSequence, ast.MatchOr)):
            for pattern in node.patterns:
                aliases = self._pattern(pattern, aliases)
            return aliases
        if isinstance(node, ast.MatchAs):
            if node.pattern is not None:
                aliases = self._pattern(node.pattern, aliases)
            if node.name is not None:
                aliases.pop(node.name, None)
            return aliases
        if isinstance(node, ast.MatchStar) and node.name is not None:
            aliases.pop(node.name, None)
        return aliases

    @staticmethod
    def _irrefutable_pattern(node: ast.pattern) -> bool:
        if isinstance(node, ast.MatchAs):
            return (
                node.pattern is None
                or _EnvAliasAnnotator._irrefutable_pattern(node.pattern)
            )
        if isinstance(node, ast.MatchOr):
            return any(
                _EnvAliasAnnotator._irrefutable_pattern(pattern)
                for pattern in node.patterns
            )
        return False

    def _block(
        self,
        statements: Sequence[ast.stmt],
        incoming: dict[str, _AliasValue],
    ) -> tuple[dict[str, _AliasValue], bool]:
        aliases = dict(incoming)
        falls_through = True

        for statement in statements:
            if not falls_through:
                break
            aliases, falls_through = self._statement(statement, aliases)

        return aliases, falls_through

    def _statement(
        self,
        node: ast.stmt,
        aliases: dict[str, _AliasValue],
    ) -> tuple[dict[str, _AliasValue], bool]:
        if isinstance(node, ast.Expr):
            return self._expr(node.value, aliases), True

        if isinstance(node, ast.Assign):
            aliases = self._expr(node.value, aliases)
            for target in node.targets:
                aliases = self._expr(target, aliases)
            for target in node.targets:
                self._bind_target(target, node.value, aliases)
            return aliases, True

        if isinstance(node, ast.AnnAssign):
            aliases = self._expr(node.annotation, aliases)
            aliases = self._expr(node.value, aliases)
            aliases = self._expr(node.target, aliases)
            if node.value is None:
                self._invalidate_target(node.target, aliases)
            else:
                self._bind_target(node.target, node.value, aliases)
            return aliases, True

        if isinstance(node, ast.AugAssign):
            aliases = self._expr(node.target, aliases)
            aliases = self._expr(node.value, aliases)
            self._invalidate_target(node.target, aliases)
            return aliases, True

        if isinstance(node, ast.Delete):
            for target in node.targets:
                aliases = self._expr(target, aliases)
                self._invalidate_target(target, aliases)
            return aliases, True

        if isinstance(node, ast.Return):
            return self._expr(node.value, aliases), False

        if isinstance(node, ast.Raise):
            aliases = self._expr(node.exc, aliases)
            aliases = self._expr(node.cause, aliases)
            return aliases, False

        if isinstance(node, (ast.Break, ast.Continue)):
            return aliases, False

        if isinstance(node, ast.If):
            tested = self._expr(node.test, aliases)
            truth = self._literal_truth(node.test)
            branches: list[dict[str, _AliasValue]] = []

            if truth is not False:
                body, body_falls = self._block(node.body, dict(tested))
                if body_falls:
                    branches.append(body)

            if truth is not True:
                if node.orelse:
                    other, other_falls = self._block(
                        node.orelse,
                        dict(tested),
                    )
                else:
                    other, other_falls = dict(tested), True
                if other_falls:
                    branches.append(other)

            if not branches:
                return tested, False
            return _merge_alias_maps(branches), True

        if isinstance(node, ast.Match):
            subject_aliases = self._expr(node.subject, aliases)
            branches: list[dict[str, _AliasValue]] = []
            exhaustive = False

            for case in node.cases:
                case_aliases = self._pattern(
                    case.pattern,
                    dict(subject_aliases),
                )
                case_aliases = self._expr(case.guard, case_aliases)
                body_aliases, body_falls = self._block(
                    case.body,
                    case_aliases,
                )
                if body_falls:
                    branches.append(body_aliases)
                if (
                    case.guard is None
                    and self._irrefutable_pattern(case.pattern)
                ):
                    exhaustive = True

            if not exhaustive:
                branches.append(subject_aliases)
            if not branches:
                return subject_aliases, False
            return _merge_alias_maps(branches), True

        if isinstance(node, (ast.For, ast.AsyncFor)):
            before = self._expr(node.iter, aliases)
            body_input = dict(before)
            self._invalidate_target(node.target, body_input)
            body, body_falls = self._block(node.body, body_input)
            else_aliases, else_falls = self._block(node.orelse, dict(before))

            branches = [before]
            if body_falls:
                branches.append(body)
            if else_falls:
                branches.append(else_aliases)
            return _merge_alias_maps(branches), True

        if isinstance(node, ast.While):
            before = self._expr(node.test, aliases)
            truth = self._literal_truth(node.test)
            branches: list[dict[str, _AliasValue]] = []

            if truth is not False:
                body, body_falls = self._block(node.body, dict(before))
                if body_falls:
                    branches.append(body)

            if truth is not True:
                other, other_falls = self._block(node.orelse, dict(before))
                if other_falls:
                    branches.append(other)

            # Unless the loop is statically known to execute forever, zero
            # iterations or eventual termination preserves a pre-loop path.
            if truth is not True:
                branches.append(before)

            if not branches:
                return before, False
            return _merge_alias_maps(branches), True

        if isinstance(node, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            body, body_falls = self._block(node.body, dict(aliases))
            branches: list[dict[str, _AliasValue]] = []

            if body_falls:
                body_else, else_falls = self._block(node.orelse, body)
                if else_falls:
                    branches.append(body_else)

            for handler in node.handlers:
                handler_aliases = dict(aliases)
                handler_aliases = self._expr(handler.type, handler_aliases)
                if handler.name is not None:
                    handler_aliases.pop(handler.name, None)
                handled, handled_falls = self._block(
                    handler.body,
                    handler_aliases,
                )
                if handled_falls:
                    branches.append(handled)

            if not branches:
                branches.append(dict(aliases))

            merged = _merge_alias_maps(branches)
            if node.finalbody:
                return self._block(node.finalbody, merged)
            return merged, True

        if isinstance(node, (ast.With, ast.AsyncWith)):
            current = aliases
            for item in node.items:
                current = self._expr(item.context_expr, current)
                if item.optional_vars is not None:
                    self._invalidate_target(item.optional_vars, current)
            return self._block(node.body, current)

        if isinstance(node, ast.Assert):
            aliases = self._expr(node.test, aliases)
            aliases = self._expr(node.msg, aliases)
            return aliases, True

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                aliases = self._expr(decorator, aliases)
            for default in node.args.defaults:
                aliases = self._expr(default, aliases)
            for default in node.args.kw_defaults:
                aliases = self._expr(default, aliases)
            aliases.pop(node.name, None)
            return aliases, True

        if isinstance(node, ast.ClassDef):
            for decorator in node.decorator_list:
                aliases = self._expr(decorator, aliases)
            for base in node.bases:
                aliases = self._expr(base, aliases)
            for keyword in node.keywords:
                aliases = self._expr(keyword.value, aliases)
            self._block(node.body, dict(aliases))
            aliases.pop(node.name, None)
            return aliases, True

        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for item in node.names:
                local_name = item.asname or item.name.split(".", 1)[0]
                aliases.pop(local_name, None)
            return aliases, True

        if isinstance(node, (ast.Pass, ast.Global, ast.Nonlocal)):
            return aliases, True

        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.expr):
                aliases = self._expr(child, aliases)
        return aliases, True


class _RunAstAnalyzer:
    """Perform static env/journey usage and branch-consistency analysis."""

    def __init__(self, run_callable: Any) -> None:
        try:
            run_callable = inspect.unwrap(run_callable)
            lines, self._source_start = inspect.getsourcelines(run_callable)
            source = textwrap.dedent("".join(lines))
        except (OSError, TypeError) as exc:
            raise RuntimeError(
                "Could not retrieve the source for _run. The method must be "
                "defined in a source-backed Python module."
            ) from exc

        try:
            module = ast.parse(source)
        except SyntaxError as exc:
            raise RuntimeError("Could not parse the source for _run.") from exc

        run_name = getattr(run_callable, "__name__", "_run")
        self._run = next(
            (
                node
                for node in module.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == run_name
            ),
            None,
        )
        if self._run is None:
            raise RuntimeError(f"Could not locate the AST node for {run_name!r}.")

        _EnvAliasAnnotator(self._run).annotate()

        self._parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(self._run):
            for child in ast.iter_child_nodes(parent):
                self._parents[child] = parent

    def analyze(self) -> dict[str, Any]:
        """Analyze ``_run`` and raise once if any dependency is conditional."""
        flow = self._block(self._run.body)
        complete = _alternate(*(flow[kind] for kind in _FLOW_KINDS))
        if not complete.exists:
            complete = _EMPTY

        env_names = {
            event.name for event in complete.may if event.kind == "env"
        }
        env_leaf_names = {
            name
            for name in env_names
            if not any(
                other != name and other.startswith(name + ".")
                for other in env_names
            )
        }

        relevant = {
            event
            for event in complete.may
            if event.kind != "env" or event.name in env_leaf_names
        }
        violations = {
            event for event in relevant if event not in complete.must
        }

        def names(kind: str, events: Iterable[_Event]) -> tuple[str, ...]:
            return tuple(sorted(event.name for event in events if event.kind == kind))

        result = {
            "env_leaves": tuple(sorted(env_leaf_names)),
            "journey_paths": names("journey", relevant),
            "unresolved_env_accesses": names("unresolved_env", relevant),
            "unresolved_journey_paths": names("unresolved_journey", relevant),
            "control_flow_outcomes": tuple(
                kind for kind in _FLOW_KINDS if flow[kind].exists
            ),
            "violations": {
                "env_leaves": names("env", violations),
                "journey_paths": names("journey", violations),
                "unresolved_env_accesses": names(
                    "unresolved_env",
                    violations,
                ),
                "unresolved_journey_paths": names(
                    "unresolved_journey",
                    violations,
                ),
            },
        }

        if violations:
            raise RunBranchConsistencyError(result)
        return result

    def _line(self, node: ast.AST) -> int | str:
        relative = getattr(node, "lineno", None)
        return "?" if relative is None else self._source_start + relative - 1

    @staticmethod
    def _literal_string(node: ast.AST | None) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    @staticmethod
    def _literal_truth(node: ast.AST) -> bool | None:
        if isinstance(node, ast.Constant):
            return bool(node.value)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            if not node.elts:
                return False
            if any(not isinstance(element, ast.Starred) for element in node.elts):
                return True
            return None
        if isinstance(node, ast.Dict):
            if not node.keys:
                return False
            if any(key is not None for key in node.keys):
                return True
            return None
        return None

    def _extract_env_path(
        self,
        node: ast.AST,
    ) -> _AliasValue | None:
        """Resolve a direct or aliased dictionary chain back to ``env``."""
        if isinstance(node, ast.Name):
            alias = getattr(node, "_env_alias_value", None)
            if isinstance(alias, _AliasValue):
                return alias

            # Fallback for unusual ASTs that were not passed through the alias
            # annotator.  In normal operation, the original env parameter is
            # annotated just like every other alias.
            if node.id == "env":
                return _AliasValue(frozenset(((),)))

            return None

        if isinstance(node, ast.Subscript):
            base = self._extract_env_path(node.value)
            if base is None:
                return None

            key = self._literal_string(node.slice)
            if key is None:
                return _AliasValue(
                    paths=frozenset(),
                    unresolved=True,
                    definite=base.definite,
                )

            return _AliasValue(
                paths=frozenset(path + (key,) for path in base.paths),
                unresolved=base.unresolved,
                definite=base.definite,
            )

        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
        ):
            base = self._extract_env_path(node.func.value)
            if base is None:
                return None

            key_node = node.args[0] if node.args else next(
                (
                    keyword.value
                    for keyword in node.keywords
                    if keyword.arg == "key"
                ),
                None,
            )
            key = self._literal_string(key_node)
            if key is None:
                return _AliasValue(
                    paths=frozenset(),
                    unresolved=True,
                    definite=base.definite,
                )

            return _AliasValue(
                paths=frozenset(path + (key,) for path in base.paths),
                unresolved=base.unresolved,
                definite=base.definite,
            )

        return None

    def _continues_env_chain(self, node: ast.AST) -> bool:
        parent = self._parents.get(node)
        if isinstance(parent, ast.Subscript) and parent.value is node:
            return True
        if (
            isinstance(parent, ast.Attribute)
            and parent.value is node
            and parent.attr == "get"
        ):
            grandparent = self._parents.get(parent)
            return isinstance(grandparent, ast.Call) and grandparent.func is parent
        return False

    def _env_summary(self, node: ast.AST) -> _Summary:
        """Return may/must events for one terminal direct or aliased access."""
        resolved = self._extract_env_path(node)
        if resolved is None or self._continues_env_chain(node):
            return _EMPTY

        events: set[_Event] = {
            _Event("env", ".".join(path))
            for path in resolved.paths
            if path
        }

        if resolved.unresolved:
            events.add(
                _Event(
                    "unresolved_env",
                    f"line {self._line(node)}: {ast.unparse(node)}",
                )
            )

        if not events:
            # A bare env object or alias is not itself a leaf access.
            return _EMPTY

        frozen_events = frozenset(events)

        # One exact semantic event is guaranteed only when the expression is
        # env-derived on every incoming path.  Multiple possible canonical
        # prefixes represent a branch-dependent alias, so each individual leaf
        # remains a may-access rather than a must-access.
        must = (
            frozen_events
            if resolved.definite and len(frozen_events) == 1
            else frozenset()
        )

        return _Summary(
            exists=True,
            may=frozen_events,
            must=must,
        )

    def _journey_event(self, node: ast.Call) -> _Event | None:
        if not (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "run"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "journey"
        ):
            return None

        path_node = next(
            (
                keyword.value
                for keyword in node.keywords
                if keyword.arg == "path_name"
            ),
            None,
        )
        if path_node is None and len(node.args) >= 2:
            path_node = node.args[1]

        path = self._literal_string(path_node)
        if path is not None:
            return _Event("journey", path)

        expression = node if path_node is None else path_node
        return _Event(
            "unresolved_journey",
            f"line {self._line(expression)}: {ast.unparse(expression)}",
        )

    def _exprs(self, nodes: Iterable[ast.AST | None]) -> _Summary:
        result = _EMPTY
        for node in nodes:
            if node is not None:
                result = _sequence(result, self._expr(node))
        return result

    def _condition(self, node: ast.AST) -> tuple[_Summary, _Summary]:
        """Return summaries for the true and false outcomes of an expression."""
        literal = self._literal_truth(node)
        if literal is not None:
            evaluated = self._expr(node)
            return (evaluated, _NONE) if literal else (_NONE, evaluated)

        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            true, false = self._condition(node.operand)
            return false, true

        if isinstance(node, ast.BoolOp):
            active = _EMPTY
            completed = _NONE

            if isinstance(node.op, ast.And):
                for value in node.values:
                    true, false = self._condition(value)
                    completed = _alternate(
                        completed,
                        _sequence(active, false),
                    )
                    active = _sequence(active, true)
                return active, completed

            for value in node.values:
                true, false = self._condition(value)
                completed = _alternate(
                    completed,
                    _sequence(active, true),
                )
                active = _sequence(active, false)
            return completed, active

        if isinstance(node, ast.IfExp):
            test_true, test_false = self._condition(node.test)
            body_true, body_false = self._condition(node.body)
            else_true, else_false = self._condition(node.orelse)
            return (
                _alternate(
                    _sequence(test_true, body_true),
                    _sequence(test_false, else_true),
                ),
                _alternate(
                    _sequence(test_true, body_false),
                    _sequence(test_false, else_false),
                ),
            )

        if isinstance(node, ast.Compare):
            active = self._expr(node.left)
            false_paths = _NONE
            for comparator in node.comparators:
                evaluated = _sequence(active, self._expr(comparator))
                false_paths = _alternate(false_paths, evaluated)
                active = evaluated
            return active, false_paths

        if isinstance(node, ast.NamedExpr):
            true, false = self._condition(node.value)
            target = self._target(node.target)
            return _sequence(true, target), _sequence(false, target)

        evaluated = self._expr(node)
        return evaluated, evaluated

    def _expr(self, node: ast.AST) -> _Summary:
        if isinstance(node, (ast.Name, ast.Constant)):
            return _EMPTY

        if isinstance(node, ast.Attribute):
            return self._expr(node.value)

        if isinstance(node, ast.Subscript):
            return _sequence(
                self._exprs((node.value, node.slice)),
                self._env_summary(node),
            )

        if isinstance(node, ast.Call):
            result = self._expr(node.func)
            result = _sequence(result, self._exprs(node.args))
            result = _sequence(
                result,
                self._exprs(keyword.value for keyword in node.keywords),
            )
            result = _sequence(result, self._env_summary(node))
            return _sequence(result, _event_summary(self._journey_event(node)))

        if isinstance(node, (ast.BoolOp, ast.Compare)):
            return _alternate(*self._condition(node))

        if isinstance(node, ast.IfExp):
            test_true, test_false = self._condition(node.test)
            return _alternate(
                _sequence(test_true, self._expr(node.body)),
                _sequence(test_false, self._expr(node.orelse)),
            )

        if isinstance(node, ast.NamedExpr):
            return _sequence(self._expr(node.value), self._target(node.target))

        if isinstance(node, ast.BinOp):
            return self._exprs((node.left, node.right))

        if isinstance(node, ast.UnaryOp):
            return self._expr(node.operand)

        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return self._exprs(node.elts)

        if isinstance(node, ast.Dict):
            items: list[ast.AST] = []
            for key, value in zip(node.keys, node.values):
                if key is not None:
                    items.append(key)
                items.append(value)
            return self._exprs(items)

        if isinstance(node, ast.Slice):
            return self._exprs((node.lower, node.upper, node.step))

        if isinstance(node, ast.Starred):
            return self._expr(node.value)

        if isinstance(node, ast.FormattedValue):
            return self._exprs((node.value, node.format_spec))

        if isinstance(node, ast.JoinedStr):
            return self._exprs(node.values)

        if isinstance(node, (ast.Await, ast.Yield, ast.YieldFrom)):
            value = getattr(node, "value", None)
            return _EMPTY if value is None else self._expr(value)

        if isinstance(node, ast.Lambda):
            return self._exprs(
                list(node.args.defaults)
                + [
                    default
                    for default in node.args.kw_defaults
                    if default is not None
                ]
            )

        if isinstance(node, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
            return self._comprehension(
                node.generators,
                lambda: self._expr(node.elt),
            )

        if isinstance(node, ast.DictComp):
            return self._comprehension(
                node.generators,
                lambda: self._exprs((node.key, node.value)),
            )

        return self._exprs(
            child
            for child in ast.iter_child_nodes(node)
            if isinstance(child, ast.expr)
        )

    def _comprehension(
        self,
        generators: Sequence[ast.comprehension],
        element: Callable[[], _Summary],
    ) -> _Summary:
        def walk(index: int) -> _Summary:
            generator = generators[index]
            iterable = self._expr(generator.iter)
            result = iterable  # Zero-iteration path.
            active = _sequence(iterable, self._target(generator.target))

            for condition in generator.ifs:
                accepted, rejected = self._condition(condition)
                result = _alternate(result, _sequence(active, rejected))
                active = _sequence(active, accepted)

            tail = walk(index + 1) if index + 1 < len(generators) else element()
            return _alternate(result, _sequence(active, tail))

        return walk(0)

    def _target(self, node: ast.AST) -> _Summary:
        if isinstance(node, ast.Name):
            return _EMPTY
        if isinstance(node, (ast.Tuple, ast.List)):
            return self._exprs(node.elts)
        if isinstance(node, ast.Starred):
            return self._target(node.value)
        if isinstance(node, ast.Attribute):
            return self._expr(node.value)
        if isinstance(node, ast.Subscript):
            return _sequence(
                self._exprs((node.value, node.slice)),
                self._env_summary(node),
            )
        return _EMPTY

    def _block(self, statements: Sequence[ast.stmt]) -> dict[str, _Summary]:
        flow = _normal_flow()
        for statement in statements:
            prefix = flow["normal"]
            carried = dict(flow)
            carried["normal"] = _NONE
            if prefix.exists:
                flow = _merge_flows(
                    carried,
                    _prepend(prefix, self._statement(statement)),
                )
            else:
                flow = carried
        return flow

    def _statement(self, node: ast.stmt) -> dict[str, _Summary]:
        if isinstance(node, ast.Expr):
            return _normal_flow(self._expr(node.value))

        if isinstance(node, ast.Assign):
            summary = self._expr(node.value)
            for target in node.targets:
                summary = _sequence(summary, self._target(target))
            return _normal_flow(summary)

        if isinstance(node, ast.AnnAssign):
            summary = _EMPTY if node.value is None else self._expr(node.value)
            return _normal_flow(_sequence(summary, self._target(node.target)))

        if isinstance(node, ast.AugAssign):
            return _normal_flow(
                self._exprs((node.target, node.value))
            )

        if isinstance(node, ast.Delete):
            return _normal_flow(self._exprs(node.targets))

        if isinstance(node, ast.Return):
            summary = _EMPTY if node.value is None else self._expr(node.value)
            return _single_flow("return", summary)

        if isinstance(node, ast.Raise):
            return _single_flow("raise", self._exprs((node.exc, node.cause)))

        if isinstance(node, ast.Break):
            return _single_flow("break", _EMPTY)

        if isinstance(node, ast.Continue):
            return _single_flow("continue", _EMPTY)

        if isinstance(node, ast.If):
            true, false = self._condition(node.test)
            return _merge_flows(
                _prepend(true, self._block(node.body)),
                _prepend(false, self._block(node.orelse)),
            )

        if isinstance(node, ast.Match):
            return self._match(node)

        if isinstance(node, (ast.For, ast.AsyncFor)):
            return self._for_loop(node)

        if isinstance(node, ast.While):
            return self._while_loop(node)

        if isinstance(node, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            return self._try(node)

        if isinstance(node, (ast.With, ast.AsyncWith)):
            prefix = _EMPTY
            for item in node.items:
                prefix = _sequence(prefix, self._expr(item.context_expr))
                if item.optional_vars is not None:
                    prefix = _sequence(prefix, self._target(item.optional_vars))
            return _prepend(prefix, self._block(node.body))

        if isinstance(node, ast.Assert):
            true, false = self._condition(node.test)
            failed = false
            if node.msg is not None:
                failed = _sequence(failed, self._expr(node.msg))
            return _merge_flows(
                _normal_flow(true),
                _single_flow("raise", failed),
            )

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defaults = list(node.args.defaults) + [
                default
                for default in node.args.kw_defaults
                if default is not None
            ]
            return _normal_flow(
                self._exprs(list(node.decorator_list) + defaults)
            )

        if isinstance(node, ast.ClassDef):
            prefix = self._exprs(
                list(node.decorator_list)
                + list(node.bases)
                + [keyword.value for keyword in node.keywords]
            )
            return _prepend(prefix, self._block(node.body))

        if isinstance(
            node,
            (ast.Pass, ast.Import, ast.ImportFrom, ast.Global, ast.Nonlocal),
        ):
            return _normal_flow()

        return _normal_flow(
            self._exprs(
                child
                for child in ast.iter_child_nodes(node)
                if isinstance(child, ast.expr)
            )
        )

    def _match(self, node: ast.Match) -> dict[str, _Summary]:
        unmatched = self._expr(node.subject)
        output = _empty_flow()

        for case in node.cases:
            attempted = _sequence(unmatched, self._pattern(case.pattern))
            matched = attempted
            unmatched = _NONE if self._irrefutable(case.pattern) else attempted

            if case.guard is not None:
                guard_true, guard_false = self._condition(case.guard)
                output = _merge_flows(
                    output,
                    _prepend(
                        _sequence(matched, guard_true),
                        self._block(case.body),
                    ),
                )
                unmatched = _alternate(
                    unmatched,
                    _sequence(matched, guard_false),
                )
            else:
                output = _merge_flows(
                    output,
                    _prepend(matched, self._block(case.body)),
                )

        return _merge_flows(output, _normal_flow(unmatched))

    def _pattern(self, node: ast.pattern) -> _Summary:
        if isinstance(node, ast.MatchValue):
            return self._expr(node.value)
        if isinstance(node, ast.MatchMapping):
            result = self._exprs(node.keys)
            for pattern in node.patterns:
                result = _sequence(result, self._pattern(pattern))
            return result
        if isinstance(node, ast.MatchClass):
            result = self._expr(node.cls)
            for pattern in (*node.patterns, *node.kwd_patterns):
                result = _sequence(result, self._pattern(pattern))
            return result
        if isinstance(node, (ast.MatchSequence, ast.MatchOr)):
            result = _EMPTY
            for pattern in node.patterns:
                result = _sequence(result, self._pattern(pattern))
            return result
        if isinstance(node, ast.MatchAs) and node.pattern is not None:
            return self._pattern(node.pattern)
        return _EMPTY

    def _irrefutable(self, node: ast.pattern) -> bool:
        if isinstance(node, ast.MatchAs):
            return node.pattern is None or self._irrefutable(node.pattern)
        if isinstance(node, ast.MatchOr):
            return any(self._irrefutable(pattern) for pattern in node.patterns)
        return False

    def _for_loop(self, node: ast.For | ast.AsyncFor) -> dict[str, _Summary]:
        iterable = self._expr(node.iter)
        zero = _prepend(iterable, self._block(node.orelse))
        one_prefix = _sequence(iterable, self._target(node.target))
        body = _prepend(one_prefix, self._block(node.body))
        output = zero

        for kind, summary in body.items():
            if not summary.exists:
                continue
            if kind in {"normal", "continue"}:
                branch = _prepend(summary, self._block(node.orelse))
            elif kind == "break":
                branch = _normal_flow(summary)
            else:
                branch = _single_flow(kind, summary)
            output = _merge_flows(output, branch)

        return output

    def _while_loop(self, node: ast.While) -> dict[str, _Summary]:
        true, false = self._condition(node.test)
        output = _prepend(false, self._block(node.orelse))
        body = _prepend(true, self._block(node.body))
        literal = self._literal_truth(node.test)

        for kind, summary in body.items():
            if not summary.exists:
                continue
            if kind == "break":
                branch = _normal_flow(summary)
            elif kind in {"normal", "continue"}:
                if literal is True:
                    branch = _single_flow("nonterminating", summary)
                else:
                    exit_prefix = _sequence(summary, false)
                    branch = _prepend(exit_prefix, self._block(node.orelse))
            else:
                branch = _single_flow(kind, summary)
            output = _merge_flows(output, branch)

        return output

    def _try(self, node: Any) -> dict[str, _Summary]:
        body = self._block(node.body)
        output = _empty_flow()

        output = _merge_flows(
            output,
            _prepend(body["normal"], self._block(node.orelse)),
        )
        for kind in ("return", "break", "continue", "nonterminating"):
            output = _merge_flows(output, _single_flow(kind, body[kind]))

        if body["raise"].exists:
            output = _merge_flows(output, self._handlers(node, body["raise"]))

        # Conservatively include an exception path that reaches a handler
        # before the try suite completes normally.
        if node.handlers:
            output = _merge_flows(output, self._handlers(node, _EMPTY))

        if node.finalbody:
            output = self._finally(output, self._block(node.finalbody))
        return output

    def _handlers(self, node: Any, prefix: _Summary) -> dict[str, _Summary]:
        if not node.handlers:
            return _single_flow("raise", prefix)

        output = _empty_flow()
        for handler in node.handlers:
            handler_prefix = prefix
            if handler.type is not None:
                handler_prefix = _sequence(
                    handler_prefix,
                    self._expr(handler.type),
                )
            output = _merge_flows(
                output,
                _prepend(handler_prefix, self._block(handler.body)),
            )
        return output

    def _finally(
        self,
        incoming: dict[str, _Summary],
        finalbody: dict[str, _Summary],
    ) -> dict[str, _Summary]:
        output = _empty_flow()
        for original_kind, prefix in incoming.items():
            if not prefix.exists:
                continue
            executed = _prepend(prefix, finalbody)
            for final_kind, summary in executed.items():
                if not summary.exists:
                    continue
                outcome = original_kind if final_kind == "normal" else final_kind
                output = _merge_flows(output, _single_flow(outcome, summary))
        return output
