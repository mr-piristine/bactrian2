# Bactrian

**A small, Camel-inspired routing and orchestration engine for Python.**

Bactrian lets you describe a data-processing topology declaratively, as routes that read from named channels, run a processor, and write to another channel, and then executes that topology as a dependency-ordered batch. Processors stay small and ignorant of the graph. The engine owns ordering, readiness, queue lifecycles, error containment and diagnostics.

```python
RouteBuilder().from_("bar").process(PhaseProcessor()).to("phase").build("bar-to-phase")
```

> **About the name.** The Bactrian camel is the two-humped camel of Central Asia, the pack animal of the Silk Road, the network of trade routes along which goods travelled in stages. The name nods to both ideas: **routes** that carry things from one stop to the next, and the **Camel** family of integration engines (Apache Camel) whose `from → process → to` model Bactrian's DSL follows.

---

## Contents

1. [Overview](#1-overview)
2. [Installation and layout](#2-installation-and-layout)
3. [Quick start](#3-quick-start)
4. [Core concepts](#4-core-concepts)
5. [Execution model](#5-execution-model)
6. [API reference](#6-api-reference)
7. [Error handling and delivery guarantees](#7-error-handling-and-delivery-guarantees)
8. [Validation rules](#8-validation-rules)
9. [Cookbook](#9-cookbook)
10. [Testing](#10-testing)
11. [Design decisions](#11-design-decisions)
12. [Limits and extension points](#12-limits-and-extension-points)
13. [Cheat sheet](#13-cheat-sheet)

---

## 1. Overview

### What Bactrian is

Bactrian is an **in-process, synchronous, batch-oriented** routing engine. You give it:

- **Channels**: named FIFO queues that hold `Exchange` envelopes.
- **Processors**: units of business logic that turn a batch of input Exchanges into zero or more output Exchanges.
- **Routes**: declarative `from(channels) → process → to(channel)` relationships.

An `ExecutionPlan` validates the resulting graph, orders it, and executes it: each route runs exactly once per `execute()` call, over everything currently waiting in its input channels.

### Properties

| Property | What it means |
| --- | --- |
| **Declarative topology** | Wiring lives in routes, not in processor code. Processors never know their neighbours. |
| **Batch semantics** | A processor receives *all* Exchanges queued on each input channel, not one at a time. |
| **Dependency-driven order** | Execution order is derived from channel dependencies, never hand-written. |
| **Validated up front** | Cycles, duplicate writers and duplicate route ids are rejected when the plan is built. |
| **Contained failures** | A failing route is recorded, and its dependents are skipped with a precise cause. |
| **Full lineage** | Every Exchange records the Exchanges it was derived from and the route/processor that made it. |
| **No dependencies** | One module, standard library only. |

### When to use it

Bactrian suits pipelines that process data in cycles and fan out and join across stages, for example:

- bar-by-bar or tick-batch market analysis (bars → phases/levels → zones → trades)
- ETL stages where each stage consumes the previous stage's whole output
- rule or enrichment pipelines with branching and filtering
- anything where "what runs, in what order, on what" is easier to reason about as a graph

### When not to use it

Bactrian is deliberately small. It is **not** the right tool when you need:

- per-event streaming, with a handler triggered on every arrival
- concurrency, parallel branches, or async I/O
- durable queues, persistence, replay or exactly-once delivery
- backpressure, rate limiting or per-processor timeouts

See [Limits and extension points](#12-limits-and-extension-points) for what can be bolted on and what needs a different executor.

---

## 2. Installation and layout

Bactrian is a single module with no third-party dependencies:

```text
bactrian.py      the engine, plus runnable examples at the bottom
BACTRIAN.md      this document
```

Copy `bactrian.py` into your project and import from it:

```python
from bactrian import (
    ChannelRegistry, Exchange, ExecutionPlan, FunctionProcessor,
    Pipeline, Processor, RouteBuilder,
)
```

A recent Python 3 (3.8 or newer is recommended) is all you need. The self-checking examples run with:

```bash
python bactrian.py
```

---

## 3. Quick start

A two-stage pipeline: Fahrenheit readings are converted to Celsius, then labelled.

```python
from bactrian import (
    ChannelRegistry, Exchange, ExecutionPlan, FunctionProcessor, RouteBuilder,
)

# 1. Processors: receive batches, return new Exchanges.
def to_celsius(inputs):
    return [
        Exchange.derive(round((ex.body - 32) * 5 / 9, 1), ex)
        for ex in inputs["fahrenheit"]
    ]

def label(inputs):
    return [
        Exchange.derive("hot" if ex.body > 25 else "ok", ex)
        for ex in inputs["celsius"]
    ]

# 2. Routes: declare the wiring.
plan = ExecutionPlan([
    RouteBuilder().from_("fahrenheit")
        .process(FunctionProcessor("to-celsius", to_celsius))
        .to("celsius").build("convert"),
    RouteBuilder().from_("celsius")
        .process(FunctionProcessor("labeller", label))
        .to("labels").build("label"),
])

# 3. Channels: one registry holds the queues.
registry = ChannelRegistry()
registry.ensure_all(plan.channels)          # creates a MemoryChannel for each

# 4. Feed inputs, run one batch cycle, read outputs.
registry.publish_all("fahrenheit", [Exchange(68), Exchange(95)])
result = plan.execute(registry)

print(result.ok)                                    # True
print([ex.body for ex in registry.drain("labels")]) # ['ok', 'hot']
```

What happened:

- `plan.external_channels` is `['fahrenheit']`: the plan's input, which the caller fills.
- `plan.internal_channels` is `['celsius']`: private to the plan, cleared around every run.
- `plan.terminal_channels` is `['labels']`: the plan's output, which the caller drains.
- Both routes ran once; each processor saw its channel's entire batch.

---

## 4. Core concepts

### 4.1 The layers

```text
Orchestrator            describes a topology
    │  .pipeline()
    ▼
Pipeline / RouteSet     groups routes (structure only)
    │  .routes
    ▼
ExecutionPlan           validates, orders, executes
    │
    ▼
Route                   from(channels) → processor → to(channel)
    │
    ▼
Processor               batch in → zero or more Exchanges out

Channel                 FIFO queue of Exchanges, shared via a ChannelRegistry
Exchange                the envelope: body, headers, lineage
```

Responsibilities are strictly separated:

| Component | Answers the question |
| --- | --- |
| **Exchange** | What data and lineage is being carried? |
| **Channel** | Where is an Exchange waiting? |
| **Processor** | What semantic operation is performed? |
| **Route** | What happens when this dependency is satisfied? |
| **Pipeline / RouteSet** | Which routes belong together? |
| **Orchestrator** | What topology does this component contribute? |
| **ExecutionPlan** | Given what is queued, what runs, and in what order? |

A processor never becomes an orchestrator: it does not know which channel supplied its input, where its output goes, or what runs next.

### 4.2 Glossary

| Term | Definition |
| --- | --- |
| **Exchange** | The unit of data flowing through the engine: `body`, `headers`, `id`, `parent_ids`, `route_id`, `processor_id`, `exception`. |
| **Batch** | The list of Exchanges currently waiting on one channel. |
| **Channel** | A named FIFO queue. |
| **Registry** | The `ChannelRegistry` that maps channel names to channels. |
| **Processor** | Business logic: `process(inputs) → None \| Exchange \| iterable`. |
| **Route** | One `from → process → to` relationship, plus an input mode and an optional predicate. |
| **Input mode** | `ALL` (every source needs input) or `ANY` (at least one does). |
| **Predicate** | An optional batch-level gate on a route (`.when(...)`). |
| **Plan** | An `ExecutionPlan`: a validated, ordered set of routes. |
| **Run / cycle** | One call to `plan.execute(registry)`. |
| **External channel** | Consumed by some route, produced by none. The plan's inputs. |
| **Internal channel** | Produced by one route and consumed by another. Plan-private. |
| **Terminal channel** | Produced by a route, consumed by none. The plan's outputs. |
| **Lineage** | The chain of `parent_ids` linking an Exchange to its ancestors. |
| **Filtered** | A route ran its check and produced nothing. Not an error. |
| **Cascade skip** | A route skipped because an upstream route failed. |
| **Degraded** | An `ANY` route that ran while a failed upstream's input was missing. |

---

## 5. Execution model

### 5.1 One execution is one batch cycle

`plan.execute(registry)` processes **everything currently queued on the plan's input channels, exactly once**, in dependency order. Each route runs at most once and sees the *full* contents of its source channels. There is no per-event triggering: a route is activated by a run, and when activated it handles everything waiting.

### 5.2 Channel roles and lifecycle

Channel roles are not declared; they are **derived from the route graph**.

| Role | Definition | Who fills it | Who empties it | When |
| --- | --- | --- | --- | --- |
| **External** | consumed, never produced | the caller | the plan | after a *completed* run |
| **Internal** | produced and consumed | the plan | the plan | before and after every run |
| **Terminal** | produced, never consumed | the plan | **the caller** (`drain`) | whenever the caller reads |

Terminal channels are **not** cleared by the plan. Unread results therefore survive between runs, and undrained outputs accumulate. Treat terminal channels like an outbox: drain them, or read `ExecutionResult.exchanges` for what a single run produced.

```text
 caller                      plan.execute(registry)                       caller
   │                                                                        │
   │ publish_all("bar", …)                                                  │
   ├──────────────────────────►  1. validate registry                       │
   │                             2. clear internal channels                 │
   │                             3. for route in order:                     │
   │                                  peek inputs (non-destructive)         │
   │                                  processor.process(inputs)             │
   │                                  stamp + publish outputs               │
   │                             4. clear internal channels                 │
   │                                drain (consume) external channels       │
   │ ◄──────────────────────────  return ExecutionResult                    │
   │                                                                        │
   │ drain("trade")   ◄── terminal channels still hold this run's outputs   │
```

### 5.3 The run, step by step

1. **Validate the registry.** Every channel the plan references must be registered (`LookupError` otherwise), and every route's external inputs must be present (`MissingInputError` otherwise). This happens **before anything runs**.
2. **Clear internal channels.** Nothing from a previous run can leak in.
3. **Run routes in order.** For each route:
   1. Work out which source channels currently hold Exchanges.
   2. If the route is not ready, record `SKIPPED` with the cause and move on.
   3. Otherwise execute it: read the batches, evaluate the predicate, call the processor, validate and stamp the results, publish them.
   4. Record `SUCCEEDED`, `FILTERED` or `FAILED`.
4. **Finish.** Clear internal channels and drain external channels, then return an `ExecutionResult`. (Under `HALT`, a failure instead raises `RouteExecutionError` and leaves external inputs queued. See [section 7](#7-error-handling-and-delivery-guarantees).)

### 5.4 Ordering

The plan builds a dependency graph from the routes: route *B* depends on route *A* when one of *B*'s source channels is *A*'s target channel. The order is computed in **waves**:

- a wave contains every not-yet-ordered route whose dependencies are all in earlier waves
- within a wave, routes keep the order in which they were supplied

The result is deterministic. For the market example:

```text
wave 1:  bar-to-phase, bar-to-level
wave 2:  phase-level-to-zone, phase-or-level-to-signal
wave 3:  phase-zone-to-trade
```

Inspect it with `plan.order` (route ids, in execution order). Because execution is sequential, "wave" describes dependency depth, not parallelism.

### 5.5 Readiness: `ALL` and `ANY`

A route is ready when its source channels hold Exchanges according to its mode:

| Mode | Builder | Ready when | Processor receives |
| --- | --- | --- | --- |
| `ALL` | `from_(...)` / `from_all(...)` | **every** source channel is non-empty | every source's batch |
| `ANY` | `from_any(...)` | **at least one** source channel is non-empty | only the non-empty sources' batches |

Because routes run in dependency order, an `ANY` route runs after all of its upstream producers have finished. It sees whichever inputs exist at that point, so its input set is deterministic. It does **not** fire once per arrival.

### 5.6 Fan-out and fan-in

Several routes may read the same channel. Each reads **without removing**, so each sees the complete batch; channels are emptied only at run boundaries. This is what makes this work:

```text
            ┌──► phase-level-to-zone ──► zone ──┐
bar ► phase ┤                                   ├──► phase-zone-to-trade
            └───────────────────────────────────┘
```

`phase` is consumed by the zone route, the trade route and the signal route. All three see all of it.

Fan-in (joining several channels) is done **inside the processor**. The engine hands over complete batches; the processor decides how to pair them. See [Joining batches](#joining-batches).

### 5.7 Route outcomes

| Status | Meaning | Is it an error? |
| --- | --- | --- |
| `SUCCEEDED` | The route produced at least one Exchange. | No |
| `FILTERED` | The route ran its check and produced nothing: the predicate was false, or the processor returned `None` / an empty iterable. | No |
| `SKIPPED` | The route could not run because its inputs were unavailable. `upstream_failed` / `upstream_filtered` give the cause. | No (but see cascade below) |
| `FAILED` | The predicate, processor or publish raised an exception. | Yes |

**Cause propagation.** After each route the plan records which channels are "failed" or "filtered":

- a `FAILED` route marks its target channel failed
- a `FILTERED` route marks its target channel filtered
- a `SKIPPED` route with `upstream_failed` marks its target channel failed (the cause propagates transitively)
- a `SKIPPED` route with only `upstream_filtered` marks its target channel filtered

Dependents then report exactly why they could not run. An `ANY` route that still ran, but without a failed upstream's input, is `SUCCEEDED` with a non-empty `upstream_failed`, which the engine calls **degraded**.

`upstream_filtered` on a succeeded `ANY` route is informational only. A filtered input is a normal absence and does not make the run degraded.

### 5.8 Lineage

Every Exchange a processor returns should be created with `Exchange.derive(body, *parents)`, which records `parent_ids` (one id per distinct parent). The Route then stamps `route_id` and `processor_id` on each result. Lineage is multi-parent, so a trade can point at both the phase and the zone it came from, and it can be used to pair batch items (see [Join by lineage](#join-by-lineage)).

---

## 6. API reference

### 6.1 `Exchange`

```python
@dataclass
class Exchange:
    body: Any
    headers: Dict[str, Any] = {}
    exception: Optional[BaseException] = None
    id: str                      # auto: "EXCH-1", "EXCH-2", …
    parent_ids: Tuple[str, ...] = ()
    route_id: Optional[str] = None
    processor_id: Optional[str] = None
```

| Member | Description |
| --- | --- |
| `body` | Semantic payload. **Treat as read-only once on a channel**; see [read-only bodies](#read-only-bodies). |
| `headers` | Application metadata. Routing identity does *not* belong here. |
| `exception` | Optional exception attached to this Exchange (for example, a per-item failure a processor wants to carry forward). The engine never sets it. |
| `id` | Unique, process-wide (`ExchangeIdGenerator.next_id()`). |
| `parent_ids` | Ids of the Exchanges this one was derived from. |
| `route_id`, `processor_id` | Stamped by the Route on every output. |
| `set_header(key, value)` / `get_header(key, default=None)` | Header helpers. |

#### `Exchange.derive(body, *parents, headers=None, inherit_headers=True)`

Creates a new Exchange from zero or more parents.

- `parent_ids` becomes the distinct ids of `parents`, in order.
- If `inherit_headers` is true, each parent's headers are **deep-copied** and merged in parent order, so **later parents win on key collisions** and branches never share mutable header values.
- `headers`, if given, is applied last and so resolves any collision explicitly.
- `route_id` and `processor_id` are **not** inherited; the Route stamps them.

```python
zone = Exchange.derive(
    {"phase": phase.body, "levels": level.body},
    phase, level,                    # two parents
    headers={"kind": "zone"},
)
assert zone.parent_ids == (phase.id, level.id)
```

Use `inherit_headers=False` to start with clean headers.

### 6.2 Channels

#### `Channel` (abstract)

A named FIFO queue. It transports Exchanges and never interprets them.

| Method | Description |
| --- | --- |
| `name` (property) | The channel's name. |
| `publish(exchange)` | Append one Exchange. |
| `peek_all() -> list` | Every queued Exchange, in order, **without removing** them. |
| `drain() -> list` | Remove and return every queued Exchange, in order. |
| `size() -> int` | Number of queued Exchanges. |
| `clear()` | Discard everything. |
| `publish_all(exchanges)` | *(provided)* Append many, in order. |
| `has_exchange() -> bool` | *(provided)* `size() > 0`. |

Implement the five abstract methods (plus `name`) for custom storage. See [Bounded channel](#bounded-channel).

#### `MemoryChannel(name)`

Unbounded in-memory FIFO queue.

#### `ChannelRegistry`

Maps names to channels and exposes the queue operations by name.

| Method | Description |
| --- | --- |
| `register(channel)` | Add a channel. `ValueError` if the name is taken. |
| `ensure(name)` | Return the channel, creating a `MemoryChannel` if absent. |
| `ensure_all(names)` | `ensure` for each name. Typical use: `registry.ensure_all(plan.channels)`. |
| `has_channel(name)` / `get(name)` / `names()` | Lookup. `get` raises `LookupError` for unknown names. |
| `publish(name, ex)` / `publish_all(name, exs)` | Append. |
| `peek_all(name)` / `drain(name)` / `size(name)` / `has_exchange(name)` | Read or empty. |
| `clear(names)` | Clear each named channel. |

To use a custom channel, **register it before** calling `ensure_all`; `ensure` never replaces an existing channel.

#### `ChannelInputs`

What a processor receives: `channel name → list of Exchanges`. Only channels that currently hold Exchanges appear, and each list is non-empty. (With `from_any`, some of the route's sources may be absent.)

| Member | Returns | Notes |
| --- | --- | --- |
| `inputs["bar"]` / `require("bar")` | `list[Exchange]` | `LookupError` if the channel supplied nothing. |
| `get("bar")` | `list[Exchange]` | Empty list if absent. |
| `single("bar")` | `Exchange` | `ValueError` unless exactly one; `LookupError` if absent. |
| `bodies("bar")` | `list` | `[ex.body for ex in batch]`. |
| `exchanges()` | `list[Exchange]` | Every input Exchange, flattened. Ideal as `derive` parents. |
| `items()` | `(channel, list)` pairs | |
| `channels()` | `list[str]` | Names that supplied a batch. |
| `count(channel=None)` | `int` | One batch's size, or the total. |
| `"bar" in inputs`, `iter(inputs)`, `len(inputs)` | | `len` is the number of supplying channels. |
| `add(channel, exchanges)` | | Used by the engine, and by tests. |

### 6.3 Processors

```python
class Processor(abc.ABC):
    @property
    @abc.abstractmethod
    def processor_id(self) -> str: ...

    @abc.abstractmethod
    def process(self, inputs: ChannelInputs) -> Any: ...
```

`process` may return:

| Return value | Meaning |
| --- | --- |
| `None` or `[]` | Nothing. The route is recorded as `FILTERED`. |
| one `Exchange` | A single result. |
| any iterable of `Exchange` | Many results (list, tuple, generator, …). |

**Contract.** Return **new** Exchanges (use `Exchange.derive`). Returning an input Exchange, returning the same Exchange twice, or including a non-Exchange raises inside the route and is recorded as `FAILED`. A return value that is neither `None`, an Exchange nor an iterable raises `TypeError`.

A processor does not know where its input came from, which channel supplied it, where its output goes, or what follows it.

```python
class PhaseProcessor(Processor):
    @property
    def processor_id(self) -> str:
        return "phase-processor"

    def process(self, inputs):
        return [
            Exchange.derive({"source_bar": bar.body, "phase": "example"}, bar)
            for bar in inputs["bar"]
        ]
```

#### `FunctionProcessor(processor_id, fn)`

Wraps a plain function `fn(inputs) -> None | Exchange | iterable`.

### 6.4 Routes

```python
@dataclass
class Route:
    route_id: str
    source_channels: List[str]
    processor: Processor
    target_channel: str
    input_mode: InputMode = InputMode.ALL
    predicate: Optional[Callable[[ChannelInputs], bool]] = None
```

| Member | Description |
| --- | --- |
| `is_ready(available)` | Pure check. `available` is the set of source channels that hold Exchanges. |
| `collect_inputs(registry)` | Reads, without removing, the full batch of each available source. `LookupError` if an `ALL` source is empty, or if nothing is available. |
| `execute(registry) -> list[Exchange]` | Runs the route once and returns the published Exchanges (`[]` when filtered). |

`execute` does the following, in order:

1. Verifies readiness (`RuntimeError` if not ready).
2. Collects the inputs.
3. Evaluates the predicate. If false, returns `[]` without calling the processor.
4. Calls the processor and normalises the result to a list.
5. Validates every output (an Exchange, not an input, not duplicated).
6. Stamps `route_id` and `processor_id` on each output.
7. Publishes all outputs to the target channel.

Input Exchanges are never modified. You normally let the `ExecutionPlan` call `execute`, because the plan also owns the queue lifecycle. If you call a route directly, channels are not emptied.

#### The predicate

`predicate(inputs) -> bool` is a **batch-level** gate. When it returns false, the processor is not called and the route is `FILTERED`. A predicate that raises makes the route `FAILED`. To keep or drop individual items, filter inside the processor.

### 6.5 `RouteBuilder`

A fluent DSL. Declare inputs, optionally a predicate, a processor and a target, then `build(route_id)`.

```python
route = (
    RouteBuilder()
    .from_all("phase", "level")                       # inputs
    .when(lambda inputs: inputs.count("phase") > 0)   # optional batch gate
    .process(ZoneProcessor())                         # processor
    .to("zone")                                       # output
    .build("phase-level-to-zone")                     # route id
)
```

| Method | Description |
| --- | --- |
| `from_(*channels)` | Alias for `from_all`. |
| `from_all(*channels)` | Mode `ALL`. May be called more than once; channels accumulate. |
| `from_any(*channels)` | Mode `ANY`. |
| `when(predicate)` | Batch-level gate. |
| `process(processor)` | The processor. |
| `to(channel)` | The single target channel. |
| `build(route_id)` | Returns the `Route`. |

`ValueError` is raised when: `from_*` is called with no channels; `from_all` and `from_any` are mixed on one builder; or the sources, processor or target are missing at `build`. Duplicate source channels are removed, preserving order.

### 6.6 `RouteSet`, `Pipeline`, `Orchestrator`

```python
pipeline = Pipeline(name="market-pipeline")
pipeline.add(route)               # ValueError on a duplicate route id
pipeline.add_all(more_routes)
pipeline.routes                   # a copy of the list
```

`RouteSet` is purely structural; it does not execute. `Pipeline` is a semantic alias for a `RouteSet` whose routes form a processing pipeline.

An `Orchestrator` describes a topology and does no processing:

```python
class MarketOrchestrator(Orchestrator):
    @property
    def orchestrator_id(self) -> str:
        return "market-orchestrator"

    def pipeline(self) -> Pipeline:
        p = Pipeline(name="market-pipeline")
        p.add(RouteBuilder().from_("bar").process(PhaseProcessor())
              .to("phase").build("bar-to-phase"))
        # …
        return p
```

### 6.7 `ExecutionPlan`

#### Construction

```python
ExecutionPlan(routes)
ExecutionPlan.from_route_set(route_set)
ExecutionPlan.from_pipelines(*pipelines)
ExecutionPlan.from_orchestrators(*orchestrators)
```

Construction validates the graph and computes the order. Invalid graphs raise `PlanValidationError` (see [section 8](#8-validation-rules)).

#### Inspection

| Property | Returns |
| --- | --- |
| `routes` | The routes, as supplied. |
| `order` | Route ids in execution order. |
| `channels` | Every channel the plan references. |
| `external_channels` | Inputs (consumed, never produced). |
| `internal_channels` | Plan-private (produced and consumed). |
| `terminal_channels` | Outputs (produced, never consumed). |
| `produced_channels` | Every channel some route writes. |

#### `validate_registry(registry)`

Checks that the registry can support the plan. Called automatically by `execute`; call it yourself for an early check. Raises `LookupError` for unregistered channels, and `MissingInputError` when:

- an `ALL` route has an external source with no Exchanges, or
- an `ANY` route whose sources are all external has none of them populated.

An `ANY` route that mixes internal and external sources is never rejected here, because internal availability is only known at run time.

#### `execute(registry, on_error=ErrorPolicy.HALT) -> ExecutionResult`

Runs one batch cycle. See [section 5](#5-execution-model) for the full lifecycle and [section 7](#7-error-handling-and-delivery-guarantees) for failure behaviour.

### 6.8 Results

#### `RouteStatus`

`SUCCEEDED`, `FAILED`, `SKIPPED`, `FILTERED`.

#### `RouteResult`

| Field | Description |
| --- | --- |
| `route_id` | The route. |
| `status` | A `RouteStatus`. |
| `exchanges` | Exchanges the route published (empty unless `SUCCEEDED`). |
| `error` | The exception, for `FAILED`. |
| `reason` | Human-readable explanation (for `FAILED`, `SKIPPED` and `FILTERED`). |
| `input_count` | Exchanges available on the route's source channels when it was considered. |
| `upstream_failed` | Source channels whose producer failed (or was cascade-skipped). |
| `upstream_filtered` | Source channels whose producer produced nothing. |

For `SKIPPED` routes the `upstream_*` lists are the cause. For `SUCCEEDED` `ANY` routes they list the inputs that were absent; `upstream_failed` marks the run degraded.

#### `ExecutionResult`

| Member | Description |
| --- | --- |
| `results` | Every `RouteResult`, in execution order. |
| `ok` | No route failed. Skipped and filtered routes are **not** errors. |
| `clean` | `ok`, and nothing ran degraded. Stricter than `ok`. |
| `exchanges` | Every Exchange published during this run, in execution order. |
| `succeeded`, `failed`, `skipped`, `filtered` | Results filtered by status. |
| `cascade_skipped` | Skipped routes with a non-empty `upstream_failed`. |
| `degraded` | Succeeded routes with a non-empty `upstream_failed`. |
| `by_route()` | `{route_id: RouteResult}`. |

#### Exceptions

| Exception | Base | Raised when |
| --- | --- | --- |
| `PlanValidationError` | `ValueError` | The route graph is structurally invalid. |
| `MissingInputError` | `LookupError` | An external input channel holds nothing. |
| `RouteExecutionError` | `RuntimeError` | A route failed under `ErrorPolicy.HALT`. Has `.route_id` and `.partial` (an `ExecutionResult`); the original exception is its `__cause__`. |

#### `ErrorPolicy`

`HALT` (default) and `SKIP_DEPENDENTS`. See below.

---

## 7. Error handling and delivery guarantees

### 7.1 Where errors can come from

A route is `FAILED` when **anything** in its execution raises: the predicate, the processor, a contract violation (returning an input Exchange, a duplicate, a non-Exchange), or the publish itself. The exception is captured as `RouteResult.error`.

Failures of the *engine's* preconditions (an unregistered channel, a missing external input) are not route failures. They raise from `execute` before anything runs.

### 7.2 Error policies

| Policy | On a route failure | External inputs after the run | Result |
| --- | --- | --- | --- |
| `HALT` (default) | Raise `RouteExecutionError` immediately. | **Left queued**, so the caller can retry. | Partial result on the exception (`exc.partial`). |
| `SKIP_DEPENDENTS` | Record `FAILED`, continue. | **Drained** (consumed). | Full `ExecutionResult`. |

Under both policies, internal channels are cleared.

Under `SKIP_DEPENDENTS`, every route that needs a failed route's output is `SKIPPED` with `upstream_failed` set (transitively), and `ANY` routes run degraded. `result.ok` is `False` whenever any route failed.

```python
result = plan.execute(registry, on_error=ErrorPolicy.SKIP_DEPENDENTS)

if not result.ok:
    for r in result.failed:
        log.error("route %s failed: %s", r.route_id, r.error)
    for r in result.cascade_skipped:
        log.warning("route %s skipped because of %s", r.route_id, r.upstream_failed)
    for r in result.degraded:
        log.warning("route %s ran without %s", r.route_id, r.upstream_failed)
```

### 7.3 Delivery guarantees

Outputs are published as each route completes. There are no transactions. The consequences:

| Situation | Guarantee |
| --- | --- |
| Run completes (any outcome, `SKIP_DEPENDENTS`) | Inputs are consumed: **at most once** for a failed batch. |
| Run aborts under `HALT` | Inputs stay queued. Terminal channels may already contain outputs from routes that finished before the failure, so a retry can **re-deliver** them: **at least once**. |
| Process crash mid-run | In-memory queues are lost. There is no persistence. |

If you need exactly-once semantics, make downstream consumers idempotent (for example, by keying on lineage ids), or add transactional publishing in a custom channel.

### 7.4 Per-item failures

A route failure loses that route's **whole batch**. For per-item failures, handle them *inside* the processor: skip the bad item, or emit a marked Exchange. See [Dead letters](#per-item-errors-and-dead-letters).

---

## 8. Validation rules

Checked when an `ExecutionPlan` is constructed (`PlanValidationError`):

| Rule | Example error message |
| --- | --- |
| Route ids are unique. | `Duplicate route id: r1` |
| Each channel has at most one producing route. | `Channel 'out' is written by both 'r1' and 'r2'.` |
| The dependency graph is acyclic. A route that consumes its own target is a cycle. | `Dependency cycle among routes: ['r1', 'r2']` |

Checked on each `execute` / `validate_registry`:

| Rule | Exception |
| --- | --- |
| Every referenced channel is registered. | `LookupError: Channels not registered: [...]` |
| `ALL` routes' external inputs are populated. | `MissingInputError` |
| `ANY` routes with only external sources have at least one populated. | `MissingInputError` |

Other builder and registry errors:

| Where | Exception |
| --- | --- |
| `ChannelRegistry.register` with a taken name | `ValueError` |
| `RouteSet.add` with a duplicate route id | `ValueError` |
| `RouteBuilder`: no channels, mixed modes, missing parts | `ValueError` |
| `ChannelInputs.single` with a count other than one | `ValueError` |
| `ChannelRegistry.get` for an unknown channel | `LookupError` |
| `ChannelInputs.require` for a channel that supplied nothing | `LookupError` |

Inside `plan.execute`, exceptions raised by a route (including `TypeError` / `ValueError` from the processor contract) become `FAILED` results, or `RouteExecutionError` under `HALT`.

---

## 9. Cookbook

All snippets assume `from bactrian import *` (or the specific names).

### Mapping, splitting and folding

```python
# Map (1 → 1): a new Exchange per input.
def double(inputs):
    return [Exchange.derive(ex.body * 2, ex) for ex in inputs["numbers"]]

# Splitter (1 → N): a generator is fine.
def split_digits(inputs):
    for ex in inputs["numbers"]:
        for digit in str(ex.body):
            yield Exchange.derive(int(digit), ex)

# Fold (N → 1): the whole batch becomes one Exchange.
def total(inputs):
    return Exchange.derive(sum(inputs.bodies("numbers")), *inputs["numbers"])
```

### Joining batches

The engine hands your processor complete batches. You decide how items pair up.

#### Join by lineage

Items derived from the same parent share `parent_ids`. This is how `ZoneProcessor` pairs a phase with its level:

```python
class ZoneProcessor(Processor):
    @property
    def processor_id(self): return "zone-processor"

    def process(self, inputs):
        levels = {lv.parent_ids: lv for lv in inputs["level"]}
        zones = []
        for phase in inputs["phase"]:
            level = levels.get(phase.parent_ids)
            if level is None:
                continue                       # unmatched items are dropped
            zones.append(Exchange.derive(
                {"phase": phase.body, "levels": level.body}, phase, level))
        return zones
```

#### Join by key header

```python
def join_on_seq(inputs):
    right = {ex.get_header("seq"): ex for ex in inputs["right"]}
    out = []
    for left in inputs["left"]:
        match = right.get(left.get_header("seq"))
        if match is not None:
            out.append(Exchange.derive((left.body, match.body), left, match))
    return out
```

### Filtering

**Per item**, inside the processor. This is the common case:

```python
def only_large(inputs):
    return [Exchange.derive(ex.body, ex) for ex in inputs["orders"] if ex.body > 100]
```

If nothing passes, the route is `FILTERED` and its dependents are skipped with `upstream_filtered`.

**Per batch**, with a predicate. The processor is not called at all:

```python
RouteBuilder().from_("ticks")
    .when(lambda inputs: inputs.count("ticks") >= 100)   # wait for a full window
    .process(WindowProcessor()).to("windows").build("windowing")
```

### Optional inputs with `ANY`

```python
def summarise(inputs):
    counts = {channel: len(batch) for channel, batch in inputs.items()}
    return Exchange.derive({"counts": counts}, *inputs.exchanges())

RouteBuilder().from_any("phase", "level")
    .process(FunctionProcessor("summary", summarise))
    .to("signal").build("signal")
```

The processor sees only the channels that hold Exchanges. Use `inputs.get("level")` (empty list if absent) rather than `inputs["level"]` when an input is optional.

### Per-item errors and dead letters

A route has one target channel, so split good and bad items downstream:

```python
def parse(inputs):
    out = []
    for ex in inputs["text"]:
        try:
            out.append(Exchange.derive(int(ex.body), ex))
        except ValueError as exc:
            bad = Exchange.derive(None, ex)
            bad.exception = exc                       # carry the failure forward
            out.append(bad)
    return out

plan = ExecutionPlan([
    RouteBuilder().from_("text")
        .process(FunctionProcessor("parse", parse)).to("parsed").build("parse"),

    RouteBuilder().from_("parsed")
        .process(FunctionProcessor("good", lambda i: [
            Exchange.derive(ex.body, ex) for ex in i["parsed"] if ex.exception is None
        ])).to("numbers").build("keep-good"),

    RouteBuilder().from_("parsed")
        .process(FunctionProcessor("bad", lambda i: [
            Exchange.derive(None, ex, headers={"error": str(ex.exception)})
            for ex in i["parsed"] if ex.exception is not None
        ])).to("dead-letter").build("keep-bad"),
])
```

A bad item no longer fails the batch: `result.ok` stays `True`, `numbers` gets the good items, and `dead-letter` gets the rest.

### Chaining plans

A terminal channel of one plan can be the external channel of another, because terminal channels are never cleared by their producer and external channels are drained by their consumer:

```python
registry = ChannelRegistry()
registry.ensure_all(plan_a.channels)
registry.ensure_all(plan_b.channels)      # shared names share one queue

registry.publish_all("raw", inputs)
plan_a.execute(registry)                   # fills "doubled" (terminal in A)
plan_b.execute(registry)                   # consumes "doubled" (external in B)
final = registry.drain("final")
```

### Combining orchestrators

```python
plan = ExecutionPlan.from_orchestrators(MarketOrchestrator(), RiskOrchestrator())
```

Route ids must be unique across all of them, and channels are shared by name. If both define a route writing the same channel, `PlanValidationError` is raised.

### Bounded channel

`MemoryChannel` is unbounded. A bounded variant is a small subclass; a full channel makes the producing route `FAILED`:

```python
class BoundedChannel(MemoryChannel):
    def __init__(self, name, maxlen):
        super().__init__(name)
        self.maxlen = maxlen

    def publish(self, exchange):
        if self.size() >= self.maxlen:
            raise OverflowError(f"channel '{self.name}' is full")
        super().publish(exchange)

registry = ChannelRegistry()
registry.register(BoundedChannel("out", maxlen=100))   # before ensure_all
registry.ensure_all(plan.channels)
```

Note that a mid-batch overflow leaves the already-published items on the channel. Reject or truncate whole batches in a custom `publish_all` if you need all-or-nothing behaviour.

### Reading results

```python
def report(result):
    for r in result.results:
        print(f"{r.route_id:28} {r.status.value:10} "
              f"in={r.input_count:<4} out={len(r.exchanges):<4} {r.reason or ''}")
```

Produces output like:

```text
bar-to-phase               succeeded  in=4    out=4
phase-level-to-zone        succeeded  in=8    out=4
phase-zone-to-trade        succeeded  in=8    out=2
```

### Inspecting lineage

```python
trade = registry.drain("trade")[0]
trade.parent_ids          # ('EXCH-12', 'EXCH-31')  phase and zone
trade.route_id            # 'phase-zone-to-trade'
trade.processor_id        # 'trade-processor'
```

### Read-only bodies

Bodies are shared **by reference** between every route that reads a channel, and routes run sequentially. If one processor mutates `inputs["bar"][0].body`, every route after it sees the change, and the bug is order-dependent and hard to trace.

- Treat input bodies as read-only. Build new bodies instead of editing.
- Prefer immutable bodies (frozen dataclasses, tuples, `MappingProxyType`).
- If you must mutate, `copy.deepcopy` the body first.

---

## 10. Testing

### Unit-test a processor in isolation

A processor depends only on `ChannelInputs`, so no plan or registry is needed:

```python
def test_phase_processor_maps_every_bar():
    inputs = ChannelInputs()
    inputs.add("bar", [Exchange({"close": 3900.0}), Exchange({"close": 3910.0})])

    out = PhaseProcessor().process(inputs)

    assert [ex.body["source_bar"]["close"] for ex in out] == [3900.0, 3910.0]
    assert all(len(ex.parent_ids) == 1 for ex in out)
```

### Test a topology end to end

```python
def test_trades_only_above_threshold():
    plan = ExecutionPlan.from_orchestrators(MarketOrchestrator())
    registry = ChannelRegistry()
    registry.ensure_all(plan.channels)

    registry.publish_all("bar", [Exchange({"close": c}) for c in (3800.0, 3812.5)])
    result = plan.execute(registry)

    assert result.ok and result.clean
    assert [t.body["close"] for t in registry.drain("trade")] == [3812.5]
    assert registry.size("bar") == 0                       # inputs consumed
```

### Test failure handling

```python
def test_failure_cascades():
    result = plan.execute(registry, on_error=ErrorPolicy.SKIP_DEPENDENTS)

    by = result.by_route()
    assert by["r-fails"].status is RouteStatus.FAILED
    assert by["r-needs-b"].upstream_failed == ["b"]
    assert not result.ok
```

### Test the graph itself

```python
def test_plan_shape():
    plan = ExecutionPlan.from_orchestrators(MarketOrchestrator())
    assert plan.external_channels == ["bar"]
    assert plan.terminal_channels == ["trade", "signal"]
    assert plan.order.index("bar-to-phase") < plan.order.index("phase-level-to-zone")
```

### Built-in examples

`python bactrian.py` runs seven self-checking examples (assertions included):

| Example | Demonstrates |
| --- | --- |
| `example_batch_pipeline` | A batch through the full market pipeline; fan-out; lineage; `ANY`. |
| `example_reuse_and_lifecycle` | Reusing a registry; input consumption; terminal accumulation. |
| `example_fan_out_and_splitter` | Splitters, folds and joins with generators. |
| `example_filtering` | Predicate and processor filtering; `upstream_filtered`. |
| `example_failures` | `SKIP_DEPENDENTS` cascade, degraded `ANY`, `HALT` and retry. |
| `example_any_mode` | `ANY` over batches, with and without inputs. |
| `example_validation` | Cycles, double writers, duplicate ids, contract violations. |

---

## 11. Design decisions

**Why does one `execute()` equal one batch cycle?**
It gives a precise meaning to "when a processor is activated, it handles everything waiting". Every route runs once over a complete, stable snapshot of its inputs, so ordering is deterministic and joins see consistent data. The cost is that Bactrian is not an event-at-a-time engine.

**Why do routes read channels without removing items?**
Fan-out. If the first consumer of `phase` drained it, the zone, trade and signal routes could not all see it. Rather than per-consumer cursors and subscriptions, the plan reads non-destructively and empties channels at run boundaries, which is simpler and sufficient because every consumer runs within the same cycle.

**Why aren't terminal channels cleared?**
They are the plan's outbox. Clearing them at the start of the next run would silently destroy unread results. The symmetry is intentional: the caller fills external channels and the plan drains them; the plan fills terminal channels and the caller drains them.

**Why does the processor do the join?**
There is no universal way to pair items across batches: by position, by key, by lineage, by time window. Giving the processor complete batches keeps the engine small and the join logic explicit and testable.

**Why must processors return new Exchanges?**
Channels and routes share Exchange objects. Stamping `route_id` on an input would rewrite another route's output; returning the same object twice would corrupt lineage. The engine rejects both.

**Why is `FILTERED` separate from `SKIPPED` and `FAILED`?**
They mean different things operationally. `FILTERED` is a normal "nothing to do". `SKIPPED` means a precondition was missing. `FAILED` is a fault. Keeping them apart is what lets `ok` and `clean` be meaningful and lets dependents report the true cause.

**Why one producer per channel?**
With multiple writers, the batch a consumer sees would depend on route order, and the dependency graph would be ambiguous. If you need to merge streams, give each producer its own channel and read both with `from_any` or `from_all`.

**Why validate up front?**
A cycle or a double writer is a design error, not a runtime condition. Rejecting it at plan construction means it can never appear mid-run, and it makes the execution order a pure function of the topology.

**Why is `HALT` the default policy?**
Failing fast is the safer default. Continuing past a failure is an explicit choice (`SKIP_DEPENDENTS`) made by a caller who will inspect the result.

---

## 12. Limits and extension points

### Current limits

- **Synchronous, single-threaded.** One batch cycle at a time; routes run sequentially.
- **Batch activation.** A route fires once per run, never per arrival.
- **In-memory, unbounded channels.** No capacity limits or backpressure by default.
- **No persistence or replay.** A crash loses queued Exchanges.
- **No transactions.** Outputs are visible as routes finish (see [delivery guarantees](#73-delivery-guarantees)).
- **No timeouts or cancellation** for processors.
- **No built-in metrics or tracing hooks.** `ExecutionResult` and lineage are the observability surface.
- **Whole-batch failure.** One exception loses that route's batch; handle per-item errors in the processor.
- **Process-wide Exchange ids.** `ExchangeIdGenerator` is a module-level counter.

### Extension points

| Want | Approach |
| --- | --- |
| Different storage (bounded, persistent, remote) | Subclass `Channel` and `register` it before `ensure_all`. |
| Multiple tenants or shards | One `ChannelRegistry` per tenant, reusing a single `ExecutionPlan`. |
| Metrics or logging | Wrap `plan.execute`, or iterate `ExecutionResult.results` after each run. |
| Retry | Catch `RouteExecutionError` under `HALT`; external inputs are still queued. |
| Different ID scheme | Replace `ExchangeIdGenerator.next_id`, or set `id` explicitly. |
| Event-at-a-time execution | Keep Channel, Route and Processor as they are, and write a different executor that activates routes per arrival. |

---

## 13. Cheat sheet

### Wiring

```python
RouteBuilder().from_("a", "b")            # ALL: needs a AND b
RouteBuilder().from_any("a", "b")         # ANY: needs a OR b
    .when(lambda inputs: ...)             # optional batch gate
    .process(processor)
    .to("out")
    .build("route-id")

plan = ExecutionPlan(routes)              # or .from_orchestrators(...) etc.
registry = ChannelRegistry(); registry.ensure_all(plan.channels)
```

### Running

```python
registry.publish_all("in", exchanges)
result = plan.execute(registry)                              # HALT on failure
result = plan.execute(registry, ErrorPolicy.SKIP_DEPENDENTS) # keep going
outputs = registry.drain("out")                              # caller drains terminals
```

### Writing a processor

```python
def process(self, inputs):
    inputs["x"]                 # list of all Exchanges on channel x
    inputs.single("x")          # exactly one, or ValueError
    inputs.bodies("x")          # their bodies
    inputs.exchanges()          # all inputs flattened (derive parents)
    inputs.get("x")             # [] if absent (use with ANY)

    return Exchange.derive(body, *parents)          # one result
    return [Exchange.derive(...), ...]              # many
    return None                                      # nothing → FILTERED
```

### Outcomes

| Status | Meaning | `ok` affected |
| --- | --- | --- |
| `SUCCEEDED` | produced ≥ 1 Exchange | no |
| `FILTERED` | produced nothing | no |
| `SKIPPED` | inputs unavailable (see `upstream_*`) | no |
| `FAILED` | raised | **yes** |

`clean` = `ok` and no `degraded` routes.

### Channel roles

| Role | Fill | Empty |
| --- | --- | --- |
| external | caller | plan, after a completed run |
| internal | plan | plan, around each run |
| terminal | plan | **caller**, via `drain` |

### Rules of thumb

- New Exchange per output; never return an input.
- Input bodies are read-only.
- One writer per channel.
- Joins live in processors.
- Drain terminal channels.
