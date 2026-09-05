# Agent orchestration

The agent service runs each user message as a LangGraph graph: an **orchestrator** plans the work, **subagents** execute it in parallel where dependencies allow, and a **synthesizer** writes the answer the user sees. Every model call still goes through the Claude API directly (`claude-opus-5`, adaptive thinking, streaming, server-side refusal fallbacks); LangGraph only provides state, the `Send` fan-out and the recursion for nested orchestrators.

## Graph

```
START -> orchestrate -> [Send subagent x N] -> collect -> [Send subagent x M] -> ... -> synthesize -> END
              \-> END   (simple_mode: trivial question answered directly)
```

| Node | What it does |
|---|---|
| `orchestrate` | One model call with structured output (`output_config.format`, JSON schema in `agent/plan.py`). Produces a `Plan`: `mode` (`plan` or `answer`) and up to `orchestrator_max_subagents` tasks, each with `name`, `objective`, `type` (`worker` / `orchestrator`), `tools`, `effort` and `depends_on`. `parse_plan` sanitises the result (unknown tools/dependencies dropped, duplicate names suffixed, cycles broken, list truncated). Emits a `plan` event. |
| `dispatch` (conditional edge) | Every task whose dependencies are all finished becomes a `Send("subagent", ...)`, so independent tasks run concurrently in one superstep. A dependent task receives the `SubagentResult` of each dependency in its brief. When nothing is left, routes to `synthesize`. |
| `subagent` | Runs one task and returns `{"results": {name: SubagentResult}}` (a dict-merge reducer collects parallel results). A `worker` is a bounded tool loop; an `orchestrator` re-enters the same compiled graph one level deeper. Emits `subagent.started` / `subagent.completed`. |
| `collect` | Join point after a wave; `dispatch` runs again for the next wave. |
| `synthesize` | One model call over the conversation, the plan and every result. Streams the final text; nested levels return their text as the result of their orchestrator task. |

Code: `services/agent/src/agent/graph.py` (nodes, `RunContext`, `run_worker`, `run_nested`), `plan.py` (schema, parsing, `ready_tasks`/`waves`), `llm.py` (`call_model`, `run_tool`, `UsageMeter`, `ScopedEmitter`), `prompts.py` (orchestrator/subagent/synthesizer prompts), `runner.py` (`RunExecutor`, `EventSink`, citation resolution). Runtime objects (Anthropic client, `ToolBox`, event sink, usage meter) travel in `config["configurable"]["ctx"]`; graph state is in-memory only (no checkpointer yet).

### Workers

A worker gets the stable system prompt (cached), the study context, a subagent prompt derived from its task, the conversation history and a final user turn with its objective plus the outputs of the tasks it depends on. It may only call the tools named in its task (filtered from the `ToolBox` definitions: `search_documents`, `update_concept_sheet`, ClinicalTrials.gov MCP tools); a task with no tools is a pure writing task. The loop runs at most `subagent_max_iterations` model calls; on the last one `tool_choice` is `none` so the model must answer. Stop reasons: `tool_use` executes the calls in parallel and continues, `pause_turn` continues, `max_tokens` marks the result truncated, `refusal` raises. The result carries the text, the `[[chunk:ID]]` ids it cited, and tool-call / iteration counts. A failing subagent produces `ok=false` with the error and the run continues; the synthesizer is told what is missing.

### Nested orchestrators

A task typed `orchestrator` runs `orchestrate -> subagents -> synthesize` again with the task objective (and dependency outputs) as its brief. Depth is bounded by `orchestrator_max_depth`: the root is depth 0, its orchestrator tasks run at depth 1, and so on; at the last allowed level the plan schema only offers `worker`, and any orchestrator task that would exceed the limit runs as a worker. `orchestrator_max_subagents` bounds each level's plan.

Example: *"research eligibility criteria for trials like mine, design the inclusion/exclusion section of my concept sheet, then write a draft proposal I can send to the team"*

```
root orchestrator (depth 0)
├── research      type=orchestrator  tools=[search_studies, search_documents]        ┐ parallel
│     ├── registry_trials   worker  tools=[search_studies]      ┐ parallel           │
│     └── internal_protocols worker tools=[search_documents]    ┘                    │
│     └── (nested synthesize -> research result)                                     │
├── ie_section    type=worker        tools=[update_concept_sheet]                    ┘
└── proposal      type=worker        tools=[]   depends_on=[research, ie_section]   (runs after both)
synthesize -> answer with [[chunk:ID]] citations and the new concept-sheet version
```

### Simple mode

With `simple_mode=true` (default) the plan schema allows `mode: "answer"`: for a trivial single-step question the orchestrator writes the reply itself in the same structured call, it is emitted as one `text.delta`, and no subagents or synthesizer run. With `simple_mode=false` the schema only allows `mode: "plan"`.

### Usage, citations, write-back

`UsageMeter` on the `RunContext` accumulates `usage` from **every** model call (orchestrators at every depth, every subagent, synthesizers) into one `Usage`, which is emitted as the `usage` event, patched onto the run and reported to financials exactly as before. Citations are resolved from the final text: `[[chunk:ID]]` ids are looked up in `ToolBox.seen_chunks`, which every subagent's `search_documents` call populates, so a chunk found by any subagent at any depth resolves if the synthesizer keeps the marker. Unknown ids are dropped. The assistant message write-back and run status patching are unchanged.

## Events

New `RunEventType` values (`libs/common/src/clinical_common/events.py`):

| Type | Payload |
|---|---|
| `plan` | `orchestrator_id` (null at the root, else the orchestrator task's `subagent_id`), `depth`, `rationale`, `tasks[]` (`subagent_id`, `name`, `objective`, `type`, `tools`, `effort`, `depends_on`). Ids are assigned at plan time so a client can pre-render tracks. |
| `subagent.started` | `subagent_id`, `subagent_name`, `parent_id`, `depth`, `type` (as actually run), `objective`, `tools`, `effort`, `depends_on` |
| `subagent.completed` | `subagent_id`, `subagent_name`, `depth`, `ok`, `error`, `summary` (first 500 chars), `chunk_ids`, `tool_calls`, `iterations`, `truncated` |

Existing events are reused rather than duplicated: every event emitted while a subagent works (`text.delta`, `thinking.delta`, `tool.call`, `tool.result`, `concept_sheet.updated`, and a nested level's `plan` / `subagent.*`) carries `subagent_id` and `subagent_name` in its payload. Events without `subagent_id` belong to the root orchestrator or the final synthesized answer, which is the "main" track. `EventSink` coalesces consecutive text/thinking deltas only within the same track.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `orchestrator_max_depth` | 2 | Orchestration levels (root + nested). |
| `orchestrator_max_subagents` | 5 | Max tasks per plan (per level). |
| `subagent_effort` | `low` | Effort for subagents whose plan entry sets none (plans set `low`/`medium`/`high` per task). |
| `subagent_max_iterations` | 8 | Model calls per worker loop. |
| `simple_mode` | `true` | Let the orchestrator answer trivial questions directly. |
| `agent_effort` | `medium` | Effort for orchestrator and synthesizer calls. |

Cost note: one request now makes at least three model calls (plan, one worker, synthesis); the system prompt is cached and shared by all of them.

## Testing

`services/agent/tests/test_graph.py` drives the real compiled graph with a scripted fake Anthropic client (routes each call by kind: plan / worker / synthesizer), a fake `ToolBox` and a recording sink: fan-out and dependency ordering (parallel overlap measured), nested orchestration and depth limits, usage aggregation across nested calls, citation resolution from subagent results, failure reporting, the iteration bound, refusal handling, event tagging and the exact Claude API call shape. `test_plan.py` covers plan parsing and wave ordering. Real model runs are not covered by tests.
