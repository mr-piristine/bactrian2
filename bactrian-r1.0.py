"""
Bactrian -- a small, Camel-inspired routing and orchestration engine.

The name: the Bactrian camel carried goods along the Silk Road, a network
of routes where cargo moved stage by stage. Bactrian does the same for
Exchanges, and its from -> process -> to model follows Apache Camel.

===========================================================================
1. Concepts
===========================================================================

    Orchestrator --> Pipeline (RouteSet) --> ExecutionPlan --> Route
                                                                 |
                              Channel --(batch)--> Processor --(batch)--> Channel

Exchange
    The envelope that flows through the system: a ``body`` (semantic
    payload), ``headers`` (application metadata), lineage (``id`` and
    ``parent_ids``) and the ``route_id`` / ``processor_id`` that produced it.

Channel
    A named FIFO queue of Exchanges. A channel transports Exchanges; it never
    interprets them. A channel may hold MANY Exchanges at once.

Processor
    Semantic unit of work. When its route is activated it receives ALL the
    Exchanges currently waiting in each of the route's input channels (a
    batch per channel) and returns zero, one or many Exchanges. It does not
    know where its input came from or where its output goes.

Route
    A declarative relationship ``from(channels) -> process -> to(channel)``.
    Input mode ALL needs every source channel to hold at least one Exchange;
    mode ANY needs at least one of them. An optional predicate (``.when``)
    can gate the whole batch.

RouteSet / Pipeline
    Structural grouping of routes. Does not execute anything.

Orchestrator
    Describes a topology (a Pipeline). Does no semantic processing.

ExecutionPlan
    Owns dependency resolution. It validates the route graph up front,
    derives a deterministic topological order and runs each route exactly
    once per ``execute()`` call, manages channel lifecycles, and reports what
    happened per route.


===========================================================================
2. One execution = one batch cycle
===========================================================================

``ExecutionPlan.execute(registry)`` processes everything currently queued on
the plan's input channels, exactly once, in dependency order.

Channels play one of three roles, derived from the route graph:

    external   consumed by some route, produced by none (plan INPUTS)
    internal   produced by a route AND consumed by another (plan-private)
    terminal   produced by a route, consumed by none (plan OUTPUTS)

    role       who fills it   who empties it          when
    ---------  -------------  ----------------------  ----------------------
    external   the caller     the plan                after a completed run
    internal   the plan       the plan                before and after a run
    terminal   the plan       the caller (drain)      whenever the caller
                                                       reads it

Because every route in a plan runs once and sees its input channels in full,
fan-out is free: if ``phase`` feeds both ``zone`` and ``trade``, both see the
complete ``phase`` batch. Nothing is consumed until the run ends.

Terminal channels are NOT cleared by the plan, so unread results are never
lost between runs. The caller should ``registry.drain(name)`` them (or use
``ExecutionResult.exchanges`` for what this run produced).


===========================================================================
3. Batch semantics for processors
===========================================================================

A processor receives a ``ChannelInputs``: for every input channel that holds
Exchanges, the list of those Exchanges in arrival order.

    inputs["bar"]            -> List[Exchange]  (all bars)
    inputs.single("bar")     -> Exchange        (error unless exactly one)
    inputs.exchanges()       -> every input Exchange, flattened
    inputs.bodies("bar")     -> [ex.body for ex in inputs["bar"]]

A processor may return:

    None or []          nothing (the route is recorded as FILTERED)
    one Exchange        a single result
    an iterable         many results (a list, a generator, ...)

Results must be NEW Exchanges (use ``Exchange.derive``). Returning an input
Exchange, or the same Exchange twice, is rejected.

Joining several channels is the processor's job: the framework hands over the
batches, and the processor decides how to pair them (by position, by key
header, or by lineage -- see ``ZoneProcessor`` in the examples).

Treat input Exchange bodies as READ-ONLY. Bodies are shared by reference
between every route that consumes the channel, and routes run sequentially,
so a mutation would silently change what later routes see.


===========================================================================
4. Outcomes and errors
===========================================================================

Each route ends a run with one RouteStatus:

    SUCCEEDED  produced at least one Exchange
    FILTERED   ran its check and produced nothing (predicate false, or the
               processor returned no Exchanges). A normal outcome, not an error.
    SKIPPED    could not run because its inputs were unavailable
               (``upstream_failed`` / ``upstream_filtered`` say why)
    FAILED     the predicate, processor or publish raised an exception

``ExecutionResult.ok``      no route failed
``ExecutionResult.clean``   ok, and no ANY route ran without a failed upstream

Error policy (``execute(..., on_error=...)``):

    HALT             (default) raise RouteExecutionError at the first failure.
                     External inputs are left queued so the caller can retry.
    SKIP_DEPENDENTS  record the failure, carry on. Routes needing the failed
                     route's output are SKIPPED; ANY routes run degraded.
                     External inputs ARE drained (the failure is in the result).

Delivery guarantees: outputs are published as routes complete, without
transactions. Under HALT, terminal channels may already hold outputs of routes
that finished before the failure, so a retry can re-deliver them
(at-least-once). Under SKIP_DEPENDENTS inputs are consumed regardless of
failures (at-most-once for the failed batch).


===========================================================================
5. Known limits
===========================================================================

* Synchronous and single-threaded; one batch cycle at a time.
* In-memory channels, unbounded (no backpressure or capacity limit).
* No persistence or replay, no per-processor timeouts, no metrics hooks.
* A route failure loses that route's whole batch. For per-item failures,
  handle them inside the processor (for example, emit an Exchange with
  ``exception`` set, or skip the bad item).
* All-or-nothing batch activation: a route fires once per run, never per
  arrival. Streaming "trigger on every event" needs a different executor;
  the Channel/Route/Processor model itself would carry over unchanged.


===========================================================================
6. Quick start
===========================================================================

    plan = ExecutionPlan.from_orchestrators(MarketOrchestrator())

    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)           # create MemoryChannels

    registry.publish_all("bar", [Exchange(body=b) for b in bars])
    result = plan.execute(registry)              # one batch cycle

    trades = registry.drain("trade")             # caller empties outputs
    assert result.ok

Run ``python bactrian.py`` to execute the self-checking examples at
the bottom of this file.
"""

from __future__ import annotations

import abc
import copy
from dataclasses import dataclass, field
from enum import Enum
from itertools import count
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)


# ============================================================
# Exchange
# ============================================================

class ExchangeIdGenerator:
    """Generates unique Exchange identifiers (process-wide)."""

    _counter = count(1)
    _prefix = "EXCH"

    @classmethod
    def next_id(cls) -> str:
        return f"{cls._prefix}-{next(cls._counter)}"


@dataclass
class Exchange:
    """
    Data + lineage envelope.

    Attributes
    ----------
    body
        Semantic payload. Treat as read-only once it is on a channel.
    headers
        Application metadata.
    exception
        Optional exception associated with this Exchange (for example, a
        processor can emit an Exchange describing a per-item failure).
    id
        Unique identity.
    parent_ids
        Ids of every Exchange this one was derived from. Multi-input
        processors (zone, trade) keep complete lineage.
    route_id, processor_id
        Stamped by the Route on every Exchange a processor returns.
        Identity of the producer lives here, not in headers.
    """

    body: Any
    headers: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[BaseException] = None

    id: str = field(default_factory=ExchangeIdGenerator.next_id)
    parent_ids: Tuple[str, ...] = ()

    route_id: Optional[str] = None
    processor_id: Optional[str] = None

    def set_header(self, key: str, value: Any) -> None:
        self.headers[key] = value

    def get_header(self, key: str, default: Any = None) -> Any:
        return self.headers.get(key, default)

    @classmethod
    def derive(
        cls,
        body: Any,
        *parents: Exchange,
        headers: Optional[Dict[str, Any]] = None,
        inherit_headers: bool = True,
    ) -> Exchange:
        """
        Create a new Exchange derived from zero or more parents.

        Inherited headers are deep-copied, so branches never share mutable
        header values. On key collisions between parents, later parents win;
        pass ``headers`` to resolve a collision explicitly, or
        ``inherit_headers=False`` to start clean. Route/processor metadata is
        not inherited; the Route stamps it on the result.

        Example
        -------
            zone = Exchange.derive(
                {"phase": phase.body, "levels": level.body},
                phase, level,                       # two parents
                headers={"kind": "zone"},
            )
            zone.parent_ids == (phase.id, level.id)
        """
        merged: Dict[str, Any] = {}
        if inherit_headers:
            for parent in parents:
                merged.update(copy.deepcopy(parent.headers))
        if headers:
            merged.update(headers)

        return cls(
            body=body,
            headers=merged,
            parent_ids=tuple(dict.fromkeys(p.id for p in parents)),
        )


# ============================================================
# Channels
# ============================================================

class Channel(abc.ABC):
    """
    Named FIFO queue of Exchanges.

    A Channel transports Exchanges and never interprets them. Implement
    ``publish``, ``peek_all``, ``drain``, ``size`` and ``clear`` to provide
    other storage (bounded queues, persistent queues, ...).
    """

    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def publish(self, exchange: Exchange) -> None:
        """Append one Exchange."""

    @abc.abstractmethod
    def peek_all(self) -> List[Exchange]:
        """Return every queued Exchange in order WITHOUT removing them."""

    @abc.abstractmethod
    def drain(self) -> List[Exchange]:
        """Remove and return every queued Exchange in order."""

    @abc.abstractmethod
    def size(self) -> int: ...

    @abc.abstractmethod
    def clear(self) -> None:
        """Discard every queued Exchange."""

    def publish_all(self, exchanges: Iterable[Exchange]) -> None:
        for exchange in exchanges:
            self.publish(exchange)

    def has_exchange(self) -> bool:
        return self.size() > 0


class MemoryChannel(Channel):
    """Unbounded in-memory FIFO queue."""

    def __init__(self, name: str):
        self._name = name
        self._queue: List[Exchange] = []

    @property
    def name(self) -> str:
        return self._name

    def publish(self, exchange: Exchange) -> None:
        self._queue.append(exchange)

    def peek_all(self) -> List[Exchange]:
        return list(self._queue)

    def drain(self) -> List[Exchange]:
        items, self._queue = self._queue, []
        return items

    def size(self) -> int:
        return len(self._queue)

    def clear(self) -> None:
        self._queue = []


class ChannelRegistry:
    """Registry of named Channels."""

    def __init__(self) -> None:
        self._channels: Dict[str, Channel] = {}

    # -- registration --------------------------------------------------

    def register(self, channel: Channel) -> None:
        if channel.name in self._channels:
            raise ValueError(f"Channel already registered: {channel.name}")
        self._channels[channel.name] = channel

    def ensure(self, name: str) -> Channel:
        """Get a channel, creating a MemoryChannel if it does not exist."""
        if name not in self._channels:
            self.register(MemoryChannel(name))
        return self._channels[name]

    def ensure_all(self, names: Iterable[str]) -> None:
        for name in names:
            self.ensure(name)

    def has_channel(self, name: str) -> bool:
        return name in self._channels

    def get(self, name: str) -> Channel:
        try:
            return self._channels[name]
        except KeyError:
            raise LookupError(f"Unknown channel: {name}") from None

    def names(self) -> List[str]:
        return list(self._channels)

    # -- queue operations ----------------------------------------------

    def publish(self, name: str, exchange: Exchange) -> None:
        self.get(name).publish(exchange)

    def publish_all(self, name: str, exchanges: Iterable[Exchange]) -> None:
        self.get(name).publish_all(exchanges)

    def peek_all(self, name: str) -> List[Exchange]:
        return self.get(name).peek_all()

    def drain(self, name: str) -> List[Exchange]:
        return self.get(name).drain()

    def size(self, name: str) -> int:
        return self.get(name).size()

    def has_exchange(self, name: str) -> bool:
        return self.get(name).has_exchange()

    def clear(self, names: Iterable[str]) -> None:
        for name in names:
            self.get(name).clear()


class ChannelInputs:
    """
    The batches handed to a Processor: channel name -> list of Exchanges.

    Only channels that currently hold Exchanges appear here, and each list
    is non-empty. (With ``from_any`` some source channels may be absent.)
    """

    def __init__(self) -> None:
        self._items: Dict[str, List[Exchange]] = {}

    def add(self, channel: str, exchanges: Iterable[Exchange]) -> None:
        self._items[channel] = list(exchanges)

    # -- access ---------------------------------------------------------

    def get(self, channel: str) -> List[Exchange]:
        """The channel's batch, or an empty list if it supplied nothing."""
        return list(self._items.get(channel, ()))

    def require(self, channel: str) -> List[Exchange]:
        """The channel's batch; LookupError if it supplied nothing."""
        try:
            return list(self._items[channel])
        except KeyError:
            raise LookupError(
                f"Required input channel '{channel}' was not supplied."
            ) from None

    def single(self, channel: str) -> Exchange:
        """The channel's only Exchange; ValueError unless exactly one."""
        batch = self.require(channel)
        if len(batch) != 1:
            raise ValueError(
                f"Channel '{channel}' supplied {len(batch)} Exchanges; "
                f"expected exactly one."
            )
        return batch[0]

    def bodies(self, channel: str) -> List[Any]:
        return [ex.body for ex in self.get(channel)]

    def exchanges(self) -> List[Exchange]:
        """Every input Exchange, flattened in channel order."""
        return [ex for batch in self._items.values() for ex in batch]

    def items(self) -> Iterable[Tuple[str, List[Exchange]]]:
        return ((c, list(b)) for c, b in self._items.items())

    def channels(self) -> List[str]:
        return list(self._items)

    def count(self, channel: Optional[str] = None) -> int:
        """Exchanges in one channel's batch, or in all batches combined."""
        if channel is not None:
            return len(self._items.get(channel, ()))
        return sum(len(b) for b in self._items.values())

    def __getitem__(self, channel: str) -> List[Exchange]:
        return self.require(channel)

    def __contains__(self, channel: object) -> bool:
        return channel in self._items

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        """Number of channels that supplied a batch."""
        return len(self._items)


# ============================================================
# Processor
# ============================================================

class Processor(abc.ABC):
    """
    Semantic unit of work.

    Receives the full batch from every input channel and returns:

        None / []        nothing            -> route recorded as FILTERED
        one Exchange     a single result
        an iterable      many results

    Results must be new Exchanges (``Exchange.derive``); returning an input
    Exchange is rejected. Input bodies are read-only by contract.

    A Processor does NOT know where its input came from, which channel
    supplied it, where its output goes, or which processor follows it.
    """

    @property
    @abc.abstractmethod
    def processor_id(self) -> str: ...

    @abc.abstractmethod
    def process(self, inputs: ChannelInputs) -> Any:
        """Return None, an Exchange, or an iterable of Exchanges."""


class FunctionProcessor(Processor):
    """Wrap a plain function ``fn(inputs) -> None | Exchange | Iterable``."""

    def __init__(self, processor_id: str, fn: Callable[[ChannelInputs], Any]):
        self._processor_id = processor_id
        self._fn = fn

    @property
    def processor_id(self) -> str:
        return self._processor_id

    def process(self, inputs: ChannelInputs) -> Any:
        return self._fn(inputs)


def _as_list(result: Any, processor_id: str) -> List[Exchange]:
    """Normalise a processor's return value to a list of Exchanges."""
    if result is None:
        return []
    if isinstance(result, Exchange):
        return [result]
    try:
        items = list(result)
    except TypeError:
        raise TypeError(
            f"Processor '{processor_id}' returned {type(result).__name__}; "
            f"expected None, an Exchange, or an iterable of Exchanges."
        ) from None
    return items


# ============================================================
# Route
# ============================================================

class InputMode(Enum):
    ALL = "all"   # every source channel must hold at least one Exchange
    ANY = "any"   # at least one source channel must hold an Exchange


@dataclass
class Route:
    """
    Declarative execution relationship.

        from_all("phase", "level") -> processor -> to("zone")
        from_any("phase", "level") -> processor -> to("signal")

    ALL: runs only if every source channel holds at least one Exchange.
    ANY: runs if at least one does; the processor receives whichever
         channels have Exchanges.
    predicate: optional batch-level gate ``predicate(inputs) -> bool``.
         When it returns False the processor is not called and the route
         is FILTERED. For per-item filtering, do it inside the processor.
    """

    route_id: str
    source_channels: List[str]
    processor: Processor
    target_channel: str
    input_mode: InputMode = InputMode.ALL
    predicate: Optional[Callable[[ChannelInputs], bool]] = None

    def is_ready(self, available: Set[str]) -> bool:
        """``available`` is the set of source channels that hold Exchanges."""
        present = [c in available for c in self.source_channels]
        return all(present) if self.input_mode is InputMode.ALL else any(present)

    def collect_inputs(self, registry: ChannelRegistry) -> ChannelInputs:
        """
        Read (without removing) the full batch of every available source
        channel. Queues are emptied by the ExecutionPlan at run boundaries,
        which is what lets several routes share a channel.
        """
        inputs = ChannelInputs()
        for channel in self.source_channels:
            batch = registry.peek_all(channel)
            if batch:
                inputs.add(channel, batch)
            elif self.input_mode is InputMode.ALL:
                raise LookupError(
                    f"Route '{self.route_id}' requires channel '{channel}'."
                )
        if len(inputs) == 0:
            raise LookupError(
                f"Route '{self.route_id}' has no available input Exchanges."
            )
        return inputs

    def execute(self, registry: ChannelRegistry) -> List[Exchange]:
        """
        Run this route once over the current contents of its sources.

        Returns the Exchanges published to the target channel (empty when
        the route was filtered). Input Exchanges are never modified; every
        output is stamped with this route's id and its processor's id.
        """
        available = {c for c in self.source_channels if registry.has_exchange(c)}
        if not self.is_ready(available):
            raise RuntimeError(f"Route '{self.route_id}' is not ready.")

        inputs = self.collect_inputs(registry)

        if self.predicate is not None and not self.predicate(inputs):
            return []

        pid = self.processor.processor_id
        outputs = _as_list(self.processor.process(inputs), pid)

        input_objects = {id(ex) for ex in inputs.exchanges()}
        seen: Set[int] = set()
        for ex in outputs:
            if not isinstance(ex, Exchange):
                raise TypeError(
                    f"Processor '{pid}' returned a {type(ex).__name__} "
                    f"inside its result; expected Exchange."
                )
            if id(ex) in input_objects:
                raise ValueError(
                    f"Processor '{pid}' returned an input Exchange; it must "
                    f"return new ones (use Exchange.derive)."
                )
            if id(ex) in seen:
                raise ValueError(
                    f"Processor '{pid}' returned the same Exchange twice."
                )
            seen.add(id(ex))

        for ex in outputs:
            ex.route_id = self.route_id
            ex.processor_id = pid

        registry.publish_all(self.target_channel, outputs)
        return outputs


# ============================================================
# Route Builder
# ============================================================

class RouteBuilder:
    """
    Camel-like DSL.

        RouteBuilder().from_("bar").process(PhaseProcessor()).to("phase") \\
            .build("bar-to-phase")

        RouteBuilder().from_all("phase", "level") \\
            .when(lambda inputs: inputs.count("phase") > 0) \\
            .process(ZoneProcessor()).to("zone").build("zone-route")

        RouteBuilder().from_any("phase", "level") \\
            .process(SignalProcessor()).to("signal").build("signal-route")

    from_all / from_any cannot be mixed on one builder.
    """

    def __init__(self) -> None:
        self._sources: List[str] = []
        self._mode: Optional[InputMode] = None
        self._processor: Optional[Processor] = None
        self._target: Optional[str] = None
        self._predicate: Optional[Callable[[ChannelInputs], bool]] = None

    def _declare(self, mode: InputMode, names: Tuple[str, ...]) -> RouteBuilder:
        if not names:
            raise ValueError(f"from_{mode.value}() requires at least one channel.")
        if self._mode is not None and self._mode is not mode:
            raise ValueError(
                f"Cannot mix input modes: builder already uses "
                f"{self._mode.value}, got {mode.value}."
            )
        self._mode = mode
        self._sources.extend(names)
        return self

    def from_(self, *names: str) -> RouteBuilder:
        """Alias for from_all()."""
        return self.from_all(*names)

    def from_all(self, *names: str) -> RouteBuilder:
        return self._declare(InputMode.ALL, names)

    def from_any(self, *names: str) -> RouteBuilder:
        return self._declare(InputMode.ANY, names)

    def when(self, predicate: Callable[[ChannelInputs], bool]) -> RouteBuilder:
        """Only call the processor when predicate(inputs) is true."""
        self._predicate = predicate
        return self

    def process(self, processor: Processor) -> RouteBuilder:
        self._processor = processor
        return self

    def to(self, channel_name: str) -> RouteBuilder:
        self._target = channel_name
        return self

    def build(self, route_id: str) -> Route:
        if not self._sources:
            raise ValueError("Route source channels are missing.")
        if self._processor is None:
            raise ValueError("Route processor is missing.")
        if self._target is None:
            raise ValueError("Route target channel is missing.")
        return Route(
            route_id=route_id,
            source_channels=list(dict.fromkeys(self._sources)),
            processor=self._processor,
            target_channel=self._target,
            input_mode=self._mode or InputMode.ALL,
            predicate=self._predicate,
        )


# ============================================================
# RouteSet / Pipeline / Orchestrator
# ============================================================

class RouteSet:
    """Structural grouping of routes. Does not execute anything."""

    def __init__(self, name: str):
        self.name = name
        self._routes: List[Route] = []

    def add(self, route: Route) -> RouteSet:
        if any(r.route_id == route.route_id for r in self._routes):
            raise ValueError(f"Duplicate route id: {route.route_id}")
        self._routes.append(route)
        return self

    def add_all(self, routes: Iterable[Route]) -> RouteSet:
        for route in routes:
            self.add(route)
        return self

    @property
    def routes(self) -> List[Route]:
        return list(self._routes)


class Pipeline(RouteSet):
    """Semantic alias for a RouteSet whose routes form a processing pipeline."""


class Orchestrator(abc.ABC):
    """Describes a topology (a Pipeline). Performs no semantic processing."""

    @property
    @abc.abstractmethod
    def orchestrator_id(self) -> str: ...

    @abc.abstractmethod
    def pipeline(self) -> Pipeline: ...


# ============================================================
# Results and errors
# ============================================================

class PlanValidationError(ValueError):
    """The route graph is structurally invalid."""


class MissingInputError(LookupError):
    """An external input channel holds no Exchange."""


class ErrorPolicy(Enum):
    HALT = "halt"                        # raise at the first failure
    SKIP_DEPENDENTS = "skip_dependents"  # record it and keep going


class RouteStatus(Enum):
    SUCCEEDED = "succeeded"  # produced at least one Exchange
    FAILED = "failed"        # raised an exception
    SKIPPED = "skipped"      # inputs unavailable; see upstream_* for why
    FILTERED = "filtered"    # ran, produced nothing (not an error)


@dataclass
class RouteResult:
    """
    Outcome of one route in one run.

    input_count
        Number of Exchanges available on the route's source channels.
    exchanges
        Exchanges the route published (empty unless SUCCEEDED).
    upstream_failed / upstream_filtered
        Source channels whose producing route FAILED / produced nothing in
        this run. For SKIPPED routes this is the cause. For SUCCEEDED ANY
        routes it lists inputs that were absent; ``upstream_failed`` marks
        the run as degraded, ``upstream_filtered`` is informational only.
    """

    route_id: str
    status: RouteStatus
    exchanges: List[Exchange] = field(default_factory=list)
    error: Optional[BaseException] = None
    reason: Optional[str] = None
    input_count: int = 0
    upstream_failed: List[str] = field(default_factory=list)
    upstream_filtered: List[str] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Everything that happened in one ``ExecutionPlan.execute`` call."""

    results: List[RouteResult] = field(default_factory=list)

    def _with(self, status: RouteStatus) -> List[RouteResult]:
        return [r for r in self.results if r.status is status]

    @property
    def ok(self) -> bool:
        """True when no route failed. Skipped/filtered are not errors."""
        return not self.failed

    @property
    def clean(self) -> bool:
        """Stricter than ok: nothing failed and nothing ran degraded."""
        return self.ok and not self.degraded

    @property
    def exchanges(self) -> List[Exchange]:
        """Every Exchange published during this run, in execution order."""
        return [ex for r in self.results for ex in r.exchanges]

    @property
    def succeeded(self) -> List[RouteResult]:
        return self._with(RouteStatus.SUCCEEDED)

    @property
    def failed(self) -> List[RouteResult]:
        return self._with(RouteStatus.FAILED)

    @property
    def skipped(self) -> List[RouteResult]:
        return self._with(RouteStatus.SKIPPED)

    @property
    def filtered(self) -> List[RouteResult]:
        return self._with(RouteStatus.FILTERED)

    @property
    def cascade_skipped(self) -> List[RouteResult]:
        """Skipped because an upstream route failed."""
        return [r for r in self.skipped if r.upstream_failed]

    @property
    def degraded(self) -> List[RouteResult]:
        """Succeeded (ANY routes) while a failed upstream's input was missing."""
        return [r for r in self.succeeded if r.upstream_failed]

    def by_route(self) -> Dict[str, RouteResult]:
        return {r.route_id: r for r in self.results}


class RouteExecutionError(RuntimeError):
    """Raised under ErrorPolicy.HALT. ``partial`` holds results so far."""

    def __init__(self, route_id: str, partial: ExecutionResult):
        super().__init__(f"Route '{route_id}' failed.")
        self.route_id = route_id
        self.partial = partial


# ============================================================
# Execution Plan
# ============================================================

class ExecutionPlan:
    """
    Owns dependency resolution and channel lifecycles.

    At construction the plan validates the graph and computes the order:

        * route ids are unique
        * every channel has at most one producing route
        * the dependency graph is acyclic
        * a deterministic topological order is derived (ties broken by the
          order in which routes were supplied)

    At ``execute(registry)``:

        1. the registry is checked (every channel registered; every route's
           external inputs present)
        2. internal channels are cleared
        3. routes run once each in topological order; each sees the FULL
           contents of its source channels
        4. internal channels are cleared; external channels are drained
           (see "Error policy" in the module docstring for failures)

    Terminal channels are left for the caller to drain.
    """

    def __init__(self, routes: Iterable[Route]):
        self._routes: List[Route] = list(routes)
        self._producers: Dict[str, str] = {}
        self._order: List[Route] = []
        self._validate()

    # -- construction ---------------------------------------------------

    @classmethod
    def from_route_set(cls, route_set: RouteSet) -> ExecutionPlan:
        return cls(route_set.routes)

    @classmethod
    def from_pipelines(cls, *pipelines: Pipeline) -> ExecutionPlan:
        return cls(r for p in pipelines for r in p.routes)

    @classmethod
    def from_orchestrators(cls, *orchestrators: Orchestrator) -> ExecutionPlan:
        return cls(r for o in orchestrators for r in o.pipeline().routes)

    # -- validation -----------------------------------------------------

    def _validate(self) -> None:
        seen_ids: Set[str] = set()
        for route in self._routes:
            if route.route_id in seen_ids:
                raise PlanValidationError(f"Duplicate route id: {route.route_id}")
            seen_ids.add(route.route_id)

        for route in self._routes:
            other = self._producers.get(route.target_channel)
            if other is not None:
                raise PlanValidationError(
                    f"Channel '{route.target_channel}' is written by both "
                    f"'{other}' and '{route.route_id}'."
                )
            self._producers[route.target_channel] = route.route_id

        deps: Dict[str, Set[str]] = {
            r.route_id: {
                self._producers[c] for c in r.source_channels if c in self._producers
            }
            for r in self._routes
        }

        done: Set[str] = set()
        while len(done) < len(self._routes):
            wave = [
                r for r in self._routes
                if r.route_id not in done and deps[r.route_id] <= done
            ]
            if not wave:
                cyclic = [r.route_id for r in self._routes if r.route_id not in done]
                raise PlanValidationError(f"Dependency cycle among routes: {cyclic}")
            for route in wave:
                self._order.append(route)
                done.add(route.route_id)

    def validate_registry(self, registry: ChannelRegistry) -> None:
        """
        Check the registry can support this plan. Raises LookupError for
        unregistered channels and MissingInputError for external inputs that
        hold nothing (ALL routes need all of theirs; an ANY route whose
        sources are all external needs at least one).
        """
        unknown = [c for c in self.channels if not registry.has_channel(c)]
        if unknown:
            raise LookupError(f"Channels not registered: {unknown}")

        external = set(self.external_channels)
        for route in self._routes:
            ext = [c for c in route.source_channels if c in external]
            if not ext:
                continue
            present = [c for c in ext if registry.has_exchange(c)]
            if route.input_mode is InputMode.ALL and len(present) < len(ext):
                missing = [c for c in ext if c not in present]
                raise MissingInputError(
                    f"Route '{route.route_id}' needs external input(s) {missing}."
                )
            if (
                route.input_mode is InputMode.ANY
                and len(ext) == len(route.source_channels)
                and not present
            ):
                raise MissingInputError(
                    f"Route '{route.route_id}' needs at least one of {ext}."
                )

    # -- inspection -----------------------------------------------------

    @property
    def routes(self) -> List[Route]:
        return list(self._routes)

    @property
    def order(self) -> List[str]:
        """Route ids in execution order."""
        return [r.route_id for r in self._order]

    @property
    def produced_channels(self) -> List[str]:
        return list(self._producers)

    @property
    def external_channels(self) -> List[str]:
        """Consumed but never produced: the plan's inputs (caller fills)."""
        return list(dict.fromkeys(
            c for r in self._routes for c in r.source_channels
            if c not in self._producers
        ))

    @property
    def internal_channels(self) -> List[str]:
        """Produced and consumed: plan-private, cleared around each run."""
        consumed = {c for r in self._routes for c in r.source_channels}
        return [c for c in self._producers if c in consumed]

    @property
    def terminal_channels(self) -> List[str]:
        """Produced but never consumed: the plan's outputs (caller drains)."""
        consumed = {c for r in self._routes for c in r.source_channels}
        return [c for c in self._producers if c not in consumed]

    @property
    def channels(self) -> List[str]:
        return list(dict.fromkeys(
            c for r in self._routes for c in (*r.source_channels, r.target_channel)
        ))

    # -- execution ------------------------------------------------------

    def execute(
        self,
        registry: ChannelRegistry,
        on_error: ErrorPolicy = ErrorPolicy.HALT,
    ) -> ExecutionResult:
        """
        Process everything currently queued on the plan's input channels.

        Each route runs at most once, in dependency order, over the full
        contents of its source channels. See the module docstring for the
        channel lifecycle and the delivery guarantees of each ErrorPolicy.

        Raises
        ------
        LookupError / MissingInputError
            The registry cannot support the plan (raised before anything runs).
        RouteExecutionError
            A route failed and ``on_error`` is HALT.
        """
        self.validate_registry(registry)
        registry.clear(self.internal_channels)

        outcome = ExecutionResult()
        failed_channels: Set[str] = set()    # producer failed / was cascaded
        filtered_channels: Set[str] = set()  # producer published nothing

        for route in self._order:
            available = {
                c for c in route.source_channels if registry.has_exchange(c)
            }
            input_count = sum(registry.size(c) for c in available)
            up_failed = [c for c in route.source_channels if c in failed_channels]
            up_filtered = [c for c in route.source_channels if c in filtered_channels]

            # ---- not runnable: inputs unavailable --------------------------
            if not route.is_ready(available):
                missing = [c for c in route.source_channels if c not in available]
                if up_failed:
                    why = f"upstream failure on {up_failed}"
                elif up_filtered:
                    why = f"upstream filtered on {up_filtered}"
                else:
                    why = f"missing input(s): {missing}"
                outcome.results.append(RouteResult(
                    route.route_id, RouteStatus.SKIPPED, reason=why,
                    input_count=input_count,
                    upstream_failed=up_failed, upstream_filtered=up_filtered,
                ))
                # A skipped route publishes nothing; propagate the cause so
                # transitive dependents report it too.
                if up_failed:
                    failed_channels.add(route.target_channel)
                elif up_filtered:
                    filtered_channels.add(route.target_channel)
                continue

            # ---- run -------------------------------------------------------
            try:
                produced = route.execute(registry)
            except Exception as exc:  # noqa: BLE001 - captured per route
                outcome.results.append(RouteResult(
                    route.route_id, RouteStatus.FAILED, error=exc,
                    reason=f"{type(exc).__name__}: {exc}",
                    input_count=input_count,
                ))
                failed_channels.add(route.target_channel)
                if on_error is ErrorPolicy.HALT:
                    # Leave external inputs queued so the caller can retry.
                    registry.clear(self.internal_channels)
                    raise RouteExecutionError(route.route_id, outcome) from exc
                continue

            if not produced:
                outcome.results.append(RouteResult(
                    route.route_id, RouteStatus.FILTERED,
                    reason="predicate or processor produced no Exchanges",
                    input_count=input_count,
                    upstream_failed=up_failed, upstream_filtered=up_filtered,
                ))
                filtered_channels.add(route.target_channel)
                continue

            outcome.results.append(RouteResult(
                route.route_id, RouteStatus.SUCCEEDED, exchanges=produced,
                input_count=input_count,
                upstream_failed=up_failed, upstream_filtered=up_filtered,
            ))

        # ---- run completed: consume inputs, drop plan-private state --------
        registry.clear(self.internal_channels)
        registry.clear(self.external_channels)
        return outcome


# ############################################################
#
#                         EXAMPLES
#
# Everything below is example code built on the library above.
# Each example_* function is self-checking (it asserts its claims).
#
# ############################################################


# ============================================================
# Example domain processors (batch-oriented)
# ============================================================

class PhaseProcessor(Processor):
    """One phase per bar. Batch in, batch out (a simple map)."""

    @property
    def processor_id(self) -> str:
        return "phase-processor"

    def process(self, inputs: ChannelInputs) -> List[Exchange]:
        return [
            Exchange.derive({"source_bar": bar.body, "phase": "example"}, bar)
            for bar in inputs["bar"]
        ]


class LevelProcessor(Processor):
    """One level set per bar."""

    @property
    def processor_id(self) -> str:
        return "level-processor"

    def process(self, inputs: ChannelInputs) -> List[Exchange]:
        return [
            Exchange.derive({"source_bar": bar.body, "levels": []}, bar)
            for bar in inputs["bar"]
        ]


class ZoneProcessor(Processor):
    """
    Joins phase and level batches.

    The framework hands over both full batches; pairing them is the
    processor's decision. Here they are paired by LINEAGE: a phase and a
    level derived from the same bar have identical ``parent_ids``. Unmatched
    items are dropped.
    """

    @property
    def processor_id(self) -> str:
        return "zone-processor"

    def process(self, inputs: ChannelInputs) -> List[Exchange]:
        levels_by_origin = {lv.parent_ids: lv for lv in inputs["level"]}
        zones = []
        for phase in inputs["phase"]:
            level = levels_by_origin.get(phase.parent_ids)
            if level is None:
                continue
            zones.append(Exchange.derive(
                {"phase": phase.body, "levels": level.body},
                phase, level,
            ))
        return zones


class TradeProcessor(Processor):
    """
    Per-item filtering: one trade for each zone whose bar closed above the
    threshold. Zones that do not qualify simply produce nothing.
    """

    def __init__(self, threshold: float = 3805.0):
        self.threshold = threshold

    @property
    def processor_id(self) -> str:
        return "trade-processor"

    def process(self, inputs: ChannelInputs) -> List[Exchange]:
        phases = {p.id: p for p in inputs["phase"]}
        trades = []
        for zone in inputs["zone"]:
            # zone.parent_ids == (phase.id, level.id)
            phase = next(phases[i] for i in zone.parent_ids if i in phases)
            close = phase.body["source_bar"]["close"]
            if close > self.threshold:
                trades.append(Exchange.derive(
                    {"close": close, "zone": zone.body, "decision": "buy"},
                    phase, zone,
                ))
        return trades


class MarketSignalProcessor(Processor):
    """ANY-mode processor: summarises whichever inputs are present."""

    @property
    def processor_id(self) -> str:
        return "market-signal-processor"

    def process(self, inputs: ChannelInputs) -> Exchange:
        counts = {channel: len(batch) for channel, batch in inputs.items()}
        return Exchange.derive(
            {"signal": "example", "counts": counts},
            *inputs.exchanges(),
        )


class MarketOrchestrator(Orchestrator):
    """
    Topology:

        bar ──► phase ──┬──► zone ──┬──► trade
           │            │           │
           └──► level ──┴───────────┘
                    (phase | level) ──► signal
    """

    @property
    def orchestrator_id(self) -> str:
        return "market-orchestrator"

    def pipeline(self) -> Pipeline:
        p = Pipeline(name="market-pipeline")

        p.add(RouteBuilder().from_("bar")
              .process(PhaseProcessor()).to("phase").build("bar-to-phase"))

        p.add(RouteBuilder().from_("bar")
              .process(LevelProcessor()).to("level").build("bar-to-level"))

        p.add(RouteBuilder().from_all("phase", "level")
              .process(ZoneProcessor()).to("zone").build("phase-level-to-zone"))

        p.add(RouteBuilder().from_all("phase", "zone")
              .process(TradeProcessor()).to("trade").build("phase-zone-to-trade"))

        p.add(RouteBuilder().from_any("phase", "level")
              .process(MarketSignalProcessor()).to("signal")
              .build("phase-or-level-to-signal"))

        return p


def _passthrough(channel: str) -> Callable[[ChannelInputs], List[Exchange]]:
    """Processor function: copy every Exchange of `channel` as a new child."""
    return lambda inputs: [Exchange.derive(e.body, e) for e in inputs[channel]]


def _bars(*closes: float) -> List[Exchange]:
    return [
        Exchange(body={"instrument": "XAU", "timeframe": "D1", "close": c})
        for c in closes
    ]


def _make_market() -> Tuple[ExecutionPlan, ChannelRegistry]:
    plan = ExecutionPlan.from_orchestrators(MarketOrchestrator())
    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)
    return plan, registry


# ============================================================
# Example 1: a batch through the whole pipeline
# ============================================================

def example_batch_pipeline() -> None:
    """
    Four bars are queued on `bar`. One execute() processes all of them:
    every route sees the full batch, phase is shared by zone/trade/signal
    (fan-out), and only bars closing above 3805 become trades.
    """
    plan, registry = _make_market()

    print("order:   ", plan.order)
    print("external:", plan.external_channels)   # caller fills
    print("internal:", plan.internal_channels)   # plan-private
    print("terminal:", plan.terminal_channels)   # caller drains

    registry.publish_all("bar", _bars(3800.0, 3812.5, 3790.0, 3820.0))
    result = plan.execute(registry)

    for r in result.results:
        print(f"  {r.route_id:26} {r.status.value:10} "
              f"in={r.input_count} out={len(r.exchanges)}")

    assert result.ok and result.clean
    by = result.by_route()
    assert by["bar-to-phase"].input_count == 4          # whole batch
    assert len(by["bar-to-phase"].exchanges) == 4
    assert len(by["phase-level-to-zone"].exchanges) == 4
    # phase feeds zone, trade and signal; all saw the full phase batch
    assert by["phase-zone-to-trade"].input_count == 4 + 4  # phase + zone

    trades = registry.drain("trade")                     # caller drains outputs
    assert [t.body["close"] for t in trades] == [3812.5, 3820.0]

    # Lineage: a trade descends from its phase and its zone.
    zone_ids = {ex.id for ex in by["phase-level-to-zone"].exchanges}
    assert any(pid in zone_ids for pid in trades[0].parent_ids)
    assert trades[0].route_id == "phase-zone-to-trade"

    # ANY route ran once, after its producers, and saw both channels.
    signal = registry.drain("signal")
    assert len(signal) == 1
    assert signal[0].body["counts"] == {"phase": 4, "level": 4}

    # The plan consumed its inputs and cleaned up its private channels.
    assert registry.size("bar") == 0
    assert all(registry.size(c) == 0 for c in plan.internal_channels)


# ============================================================
# Example 2: reusing a registry (queue lifecycle)
# ============================================================

def example_reuse_and_lifecycle() -> None:
    """
    Run the same plan repeatedly on one registry. Inputs are consumed and
    internal channels reset, so nothing leaks between runs. Terminal
    channels belong to the caller: undrained outputs accumulate.
    """
    plan, registry = _make_market()

    # -- run 1, caller drains the output --------------------------------
    registry.publish_all("bar", _bars(3800.0, 3812.5))
    plan.execute(registry)
    assert [t.body["close"] for t in registry.drain("trade")] == [3812.5]

    # -- run 2: no leakage from run 1 -----------------------------------
    registry.publish_all("bar", _bars(3830.0, 3700.0))
    plan.execute(registry)
    assert [t.body["close"] for t in registry.drain("trade")] == [3830.0]

    # -- runs 3 and 4 WITHOUT draining: outputs accumulate --------------
    registry.publish_all("bar", _bars(3900.0))
    plan.execute(registry)
    registry.publish_all("bar", _bars(3910.0))
    plan.execute(registry)
    assert [t.body["close"] for t in registry.peek_all("trade")] == [3900.0, 3910.0]
    registry.clear(["trade", "signal"])

    # -- a run with no input is an error, raised before anything runs ---
    try:
        plan.execute(registry)
    except MissingInputError as exc:
        print("  empty input rejected:", exc)
    else:
        raise AssertionError("expected MissingInputError")


# ============================================================
# Example 3: fan-out, fan-in and one-to-many processors
# ============================================================

def example_fan_out_and_splitter() -> None:
    """
    `numbers` feeds two independent routes (fan-out; both see every item).
    One processor emits several Exchanges per input (a splitter), the other
    aggregates the whole batch into one (a fold). A third route joins both.
    """
    def split_digits(inputs: ChannelInputs):
        for ex in inputs["numbers"]:
            for digit in str(ex.body):
                yield Exchange.derive(int(digit), ex)        # generator result

    def total(inputs: ChannelInputs):
        return Exchange.derive(sum(inputs.bodies("numbers")), *inputs["numbers"])

    def report(inputs: ChannelInputs):
        return Exchange.derive(
            {"digits": inputs.bodies("digits"), "total": inputs.single("total").body},
            *inputs.exchanges(),
        )

    plan = ExecutionPlan([
        RouteBuilder().from_("numbers")
            .process(FunctionProcessor("splitter", split_digits))
            .to("digits").build("split"),
        RouteBuilder().from_("numbers")
            .process(FunctionProcessor("folder", total))
            .to("total").build("fold"),
        RouteBuilder().from_all("digits", "total")
            .process(FunctionProcessor("reporter", report))
            .to("report").build("join"),
    ])
    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)

    registry.publish_all("numbers", [Exchange(12), Exchange(345)])
    result = plan.execute(registry)

    assert result.ok
    assert result.by_route()["split"].input_count == 2   # fan-out: full batch
    assert result.by_route()["fold"].input_count == 2    # ...to both consumers
    (report_ex,) = registry.drain("report")
    assert report_ex.body == {"digits": [1, 2, 3, 4, 5], "total": 357}
    assert len(report_ex.parent_ids) == 5 + 1      # 5 digits + the total


# ============================================================
# Example 4: filtering
# ============================================================

def example_filtering() -> None:
    """
    Two ways for a route to decline: a batch-level predicate (.when) and a
    processor that returns nothing. Both give FILTERED (not an error) and
    their dependents are SKIPPED with `upstream_filtered` set.
    """
    drop_all = FunctionProcessor("drop", lambda i: [])

    plan = ExecutionPlan([
        # predicate gate: needs at least 3 items in the batch
        RouteBuilder().from_("a").when(lambda i: i.count("a") >= 3)
            .process(FunctionProcessor("keep", _passthrough("a")))
            .to("b").build("gated"),
        RouteBuilder().from_("b")
            .process(FunctionProcessor("next", _passthrough("b")))
            .to("c").build("after-gated"),
        # processor-level decline
        RouteBuilder().from_("a").process(drop_all).to("d").build("declines"),
        RouteBuilder().from_("d")
            .process(FunctionProcessor("next2", _passthrough("d")))
            .to("e").build("after-declines"),
    ])
    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)

    registry.publish_all("a", [Exchange(1), Exchange(2)])   # only 2 < 3
    result = plan.execute(registry)

    by = result.by_route()
    assert by["gated"].status is RouteStatus.FILTERED
    assert by["declines"].status is RouteStatus.FILTERED
    assert by["after-gated"].status is RouteStatus.SKIPPED
    assert by["after-gated"].upstream_filtered == ["b"]
    assert by["after-declines"].upstream_filtered == ["d"]
    assert result.ok and result.clean                # filtering is not failure

    registry.publish_all("a", [Exchange(1), Exchange(2), Exchange(3)])
    result = plan.execute(registry)
    assert result.by_route()["gated"].status is RouteStatus.SUCCEEDED
    assert [e.body for e in registry.drain("c")] == [1, 2, 3]


# ============================================================
# Example 5: failures and error policies
# ============================================================

def example_failures() -> None:
    """
    SKIP_DEPENDENTS records the failure and continues; HALT raises and
    leaves external inputs queued for a retry.
    """
    def boom(inputs: ChannelInputs):
        raise RuntimeError("upstream exploded")

    def build() -> Tuple[ExecutionPlan, ChannelRegistry]:
        plan = ExecutionPlan([
            RouteBuilder().from_("a").process(FunctionProcessor("boom", boom))
                .to("b").build("r-fails"),
            RouteBuilder().from_("b").process(FunctionProcessor("p1", _passthrough("b")))
                .to("c").build("r-needs-b"),
            RouteBuilder().from_("c").process(FunctionProcessor("p2", _passthrough("c")))
                .to("c2").build("r-needs-c"),                       # transitive
            RouteBuilder().from_any("b", "a2")
                .process(FunctionProcessor("p3", lambda i: [
                    Exchange.derive(e.body, e) for e in i.exchanges()]))
                .to("d").build("r-any"),
        ])
        registry = ChannelRegistry()
        registry.ensure_all(plan.channels)
        registry.publish("a", Exchange(1))
        registry.publish("a2", Exchange(2))
        return plan, registry

    # ---- SKIP_DEPENDENTS ------------------------------------------------
    plan, registry = build()
    result = plan.execute(registry, on_error=ErrorPolicy.SKIP_DEPENDENTS)
    by = result.by_route()

    assert by["r-fails"].status is RouteStatus.FAILED
    assert by["r-needs-b"].status is RouteStatus.SKIPPED
    assert by["r-needs-b"].upstream_failed == ["b"]
    assert by["r-needs-c"].upstream_failed == ["c"]          # cascade propagates
    assert by["r-any"].status is RouteStatus.SUCCEEDED       # ran on a2 alone...
    assert by["r-any"].upstream_failed == ["b"]              # ...but degraded

    assert not result.ok and not result.clean
    assert [r.route_id for r in result.cascade_skipped] == ["r-needs-b", "r-needs-c"]
    assert [r.route_id for r in result.degraded] == ["r-any"]
    assert registry.size("a") == 0                           # inputs consumed

    # ---- HALT -----------------------------------------------------------
    plan, registry = build()
    try:
        plan.execute(registry)                               # default: HALT
    except RouteExecutionError as exc:
        assert exc.route_id == "r-fails"
        assert exc.partial.failed[0].error is exc.__cause__
        assert registry.size("a") == 1                       # retained for retry
        assert all(registry.size(c) == 0 for c in plan.internal_channels)
    else:
        raise AssertionError("expected RouteExecutionError")


# ============================================================
# Example 6: ANY mode over batches
# ============================================================

def example_any_mode() -> None:
    """
    ANY routes run once, when at least one source holds Exchanges, and the
    processor receives only the channels that do.
    """
    def summarise(inputs: ChannelInputs):
        return Exchange.derive(
            {c: len(batch) for c, batch in inputs.items()}, *inputs.exchanges()
        )

    plan = ExecutionPlan([
        RouteBuilder().from_any("x", "y")
            .process(FunctionProcessor("summary", summarise))
            .to("out").build("any-route"),
    ])
    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)

    registry.publish_all("x", [Exchange(1), Exchange(2)])    # y stays empty
    plan.execute(registry)
    assert registry.drain("out")[0].body == {"x": 2}

    registry.publish_all("x", [Exchange(1)])
    registry.publish_all("y", [Exchange(2), Exchange(3), Exchange(4)])
    plan.execute(registry)
    assert registry.drain("out")[0].body == {"x": 1, "y": 3}

    try:                                                     # neither has input
        plan.execute(registry)
    except MissingInputError:
        pass
    else:
        raise AssertionError("expected MissingInputError")


# ============================================================
# Example 7: graph validation and contract checks
# ============================================================

def example_validation() -> None:
    """Structural mistakes are rejected when the plan is built."""
    noop = FunctionProcessor("noop", lambda i: None)

    def route(rid: str, src: str, dst: str) -> Route:
        return RouteBuilder().from_(src).process(noop).to(dst).build(rid)

    # cycle
    try:
        ExecutionPlan([route("r1", "a", "b"), route("r2", "b", "a")])
    except PlanValidationError as exc:
        print("  cycle:          ", exc)
    else:
        raise AssertionError

    # two routes writing one channel
    try:
        ExecutionPlan([route("r1", "a", "out"), route("r2", "b", "out")])
    except PlanValidationError as exc:
        print("  double writer:  ", exc)
    else:
        raise AssertionError

    # duplicate route id
    try:
        ExecutionPlan([route("r1", "a", "b"), route("r1", "c", "d")])
    except PlanValidationError as exc:
        print("  duplicate id:   ", exc)
    else:
        raise AssertionError

    # mixing input modes on one builder
    try:
        RouteBuilder().from_all("a").from_any("b")
    except ValueError as exc:
        print("  mixed modes:    ", exc)
    else:
        raise AssertionError

    # processor contract: returning an input Exchange is rejected
    echo = FunctionProcessor("echo", lambda i: i.exchanges())
    plan = ExecutionPlan([RouteBuilder().from_("a").process(echo).to("b").build("echo")])
    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)
    registry.publish("a", Exchange(1))
    try:
        plan.execute(registry)
    except RouteExecutionError as exc:
        print("  input returned: ", exc.partial.failed[0].reason)
    else:
        raise AssertionError

    # unregistered channels are caught before anything runs
    try:
        plan.execute(ChannelRegistry())
    except LookupError as exc:
        print("  unregistered:   ", exc)
    else:
        raise AssertionError


# ============================================================
# Run everything
# ============================================================

def run_all() -> None:
    examples: List[Callable[[], None]] = [
        example_batch_pipeline,
        example_reuse_and_lifecycle,
        example_fan_out_and_splitter,
        example_filtering,
        example_failures,
        example_any_mode,
        example_validation,
    ]
    for fn in examples:
        print(f"\n== {fn.__name__}")
        fn()
        print("   ok")
    print("\nAll examples passed.")


if __name__ == "__main__":
    run_all()
