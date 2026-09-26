# SDK facts this project relies on

Every Temporal SDK or LangGraph behavior that Stepledger depends on, with the file and line
where it is defined. Paths are relative to `third_party/sdk-python/` (tag `1.33.0`, commit
`ab52fdde`), which is byte-identical to the installed `temporalio==1.33.0` wheel's Python sources
(checked with `diff -r`). LangGraph paths are in the installed `langgraph==1.2.12`.

If a fact below changes in a later SDK, the test named next to it fails.

## The LangGraph plugin

| Fact | Where |
|---|---|
| `ActivityInput(args: tuple, kwargs: dict, langgraph_config: dict)` is the single argument of every node Activity. For a Graph API node, `args == (state,)`: the whole accumulated state. | `temporalio/contrib/langgraph/_activity.py:37-43`, built at `:157-159` |
| `ActivityOutput(result=None, langgraph_command=None, langgraph_interrupts=None)` is every node Activity's result. A node returning `Command` sets `langgraph_command`; a `GraphInterrupt` sets `langgraph_interrupts`; anything else sets `result`. | `_activity.py:46-52`, `:88-96` |
| The node Activity runs the user function inside `wrap_activity.wrapper`; the activity function is typed `(input: ActivityInput) -> ActivityOutput`, so an activity inbound interceptor sees `input.args[0]` already decoded as `ActivityInput`, and `self.next.execute_activity()` returns an `ActivityOutput` instance before encoding. | `_activity.py:64`, `:316` |
| Graph API activity names are `"{graph_name}.{node_name}"`. | `_plugin.py:232-234` |
| Functional API activity names are `task_id(func)` = `"{module}.{qualname}"`. `task_id` rejects `__main__` functions and closures (`<locals>` in the qualname). | `_plugin.py:257`, `_task_cache.py:30-54` |
| Nodes marked `execute_in="workflow"` run in workflow code via `wrap_workflow`; no Activity is scheduled. | `_plugin.py:321-324`, `_workflow.py:19-73` |
| Node Activities are scheduled with `workflow.execute_activity(afunc, input, **opts)`, where `opts` come from `default_activity_options` merged with node metadata. | `_activity.py:168`, `_plugin.py:228` |
| **Task cache.** Key = `sha256(json.dumps([task_id, args, kwargs, context], sort_keys=True, default=str))[:32]`. It does not include the LangGraph step or path. | `_task_cache.py:57-68` |
| **A cache hit schedules no Activity**: `wrap_execute_activity` returns the cached result before `workflow.execute_activity` is called. | `_activity.py:146-155` |
| `graph(name, cache)` always installs a dict cache (`set_task_cache(cache or {})`), so within-run hits are possible whenever the same node function receives equal `(args, kwargs, context)`, and after continue-as-new when `cache()` is passed forward. | `_plugin.py:340`, `_activity.py:183-184` |
| `langgraph_config["metadata"]` carries LangGraph's per-task metadata: `langgraph_step`, `langgraph_node`, `langgraph_triggers`, `langgraph_path` (`task_path[:3]`), `langgraph_checkpoint_ns`. | `_langgraph_config.py:40-41, 76-93`; `langgraph/pregel/_algo.py:655-659` |
| The plugin's interceptor is a worker `Interceptor` that only registers graphs per run; it does not touch activity headers. | `_interceptor.py:27-77` |

## Interceptors and headers

| Fact | Where |
|---|---|
| `StartActivityInput` has `activity: str` (the activity type name), `activity_id: str \| None`, `headers: Mapping[str, Payload]`, `cancellation_type`. | `temporalio/worker/_interceptor.py:248-269` |
| `ExecuteActivityInput` has `fn`, `args`, `executor`, `headers: Mapping[str, Payload]`. | `worker/_interceptor.py:100-107` |
| Headers set in `start_activity` are copied onto the `ScheduleActivity` command, so they are recorded in `ActivityTaskScheduledEventAttributes.header` in history. | `worker/_workflow_instance.py:3393-3394` |
| **The default activity ID is assigned after the outbound interceptor runs**: `v.activity_id = self._input.activity_id or str(self._seq)` in `_apply_schedule_command`; `ActivityHandle` exposes no ID. This is why Stepledger uses its own `stepledger-seq` header. | `worker/_workflow_instance.py:3305`, `:3391` |
| `_ActivityHandle` is an `asyncio.Task` on the workflow's deterministic event loop. Its result future is resolved from `ResolveActivity` jobs in history order. Done callbacks run through the same loop, so `add_done_callback` registered in `start_activity` fires before the workflow code awaiting the handle resumes. Verified on replay by `tests/test_replay_determinism.py` (Decision Gate D4). | `worker/_workflow_instance.py:3296-3316`, `:875-922`, `:2017-2054` |
| Worker interceptors are chained so the first in the list is outermost (`for interceptor in reversed(...)`). | `worker/_activity.py:677-678`, `worker/_workflow.py:156` |

## Activity info

| Fact | Where |
|---|---|
| `activity.info()` returns `Info` with `activity_id`, `activity_type`, `attempt`, `current_attempt_scheduled_time`, `namespace`, `workflow_id`, `workflow_run_id`, `workflow_type`. | `temporalio/activity.py:95-130`, `:310` |
| `activity.payload_converter()` returns the worker's payload converter with `ActivitySerializationContext` set. | `activity.py:459-465` |

## Workflow exits and cancellation

| Fact | Where |
|---|---|
| The SDK classifies a workflow exit in `_run_top_level_workflow_function`: `_ContinueAsNewError` -> continue-as-new command; `asyncio.CancelledError` is converted to `temporalio.exceptions.CancelledError`; then `cancel_reason is not None and is_cancelled_exception(err)` -> cancel workflow; else `workflow_is_failure_exception(err)` -> fail workflow; else -> **workflow task failure** (retried from history, run stays open). | `worker/_workflow_instance.py:2748-2797` |
| `workflow_is_failure_exception`: `FailureError`, `asyncio.TimeoutError`, or a configured failure exception type. | `worker/_workflow_instance.py:1956-1971` |
| Public helpers: `workflow.cancellation_reason()`, `workflow.is_failure_exception()`, `workflow.patched()`. | `temporalio/workflow/_context.py:600`, `:635`, `:762` |
| `temporalio.exceptions.is_cancelled_exception(e)` is true for `asyncio.CancelledError`, `CancelledError`, and `ActivityError` / `ChildWorkflowError` / `NexusOperationError` whose cause is `CancelledError`. | `temporalio/exceptions.py:459-485` |

## External Storage

| Fact | Where |
|---|---|
| `StorageDriver` API: `name()`, `type()` (defaults to the class name), `async store(context, payloads) -> list[StorageDriverClaim]`, `async retrieve(context, claims) -> list[Payload]`. | `temporalio/converter/_extstore.py:212-263` |
| `StorageDriverClaim(claim_data: Mapping[str, str])`. | `_extstore.py:110-121` |
| `ExternalStorage(drivers, driver_selector=None, payload_size_threshold=256 KiB)`. | `_extstore.py:283-312` |
| **Codec runs before External Storage** on the way out, and after it on the way in. An encryption codec therefore hands the driver ciphertext. | `temporalio/converter/_data_converter.py:263-286` |
| The shipped S3 driver keys each object by `sha256(payload_bytes)` and verifies it on retrieve. One object per distinct payload. | `temporalio/contrib/aws/s3driver/_driver.py:176-198`, `:223-224` |
| **`StorageDriverStoreContext.target` per call site:** | |
| - workflow commands with no other target, including `ScheduleActivityTask` inputs and continue-as-new: the current workflow (`id`, `run_id`, `type`, `namespace`) | `worker/_workflow_instance.py:2485-2503`, `:2546` |
| - child workflow start: the child's `id` and `type`, **no run id** | `worker/_workflow_instance.py:2505-2518` |
| - signal to an external workflow: its `id` only | `worker/_workflow_instance.py:2520-2531` |
| - a child's completion result: the parent workflow | `worker/_workflow_instance.py:2533-2544` |
| - activity results (worker side), when started by a workflow: that workflow's `id`, `type`, `run_id` | `worker/_activity.py:325-347` |
| - activity heartbeat details: **the activity** (`StorageDriverActivityInfo`, no workflow id). Not in the spec; such refs get `workflow_id=''` and expire by age | `worker/_activity.py:262-271` |
| - client workflow start / signal-with-start: `id` and `type`, **no run id** | `temporalio/client/_impl.py:238-240` |

## Plugins and workers

| Fact | Where |
|---|---|
| `SimplePlugin` is both a `client.Plugin` and a `worker.Plugin`. `configure_client` sets `data_converter` (value or callable) and client interceptors. `configure_worker` adds activities, the workflow runner and worker interceptors; it never sets a data converter. | `temporalio/plugin.py:35`, `:101-126`, `:136-183` |
| **Client plugins are inherited by workers**: `Worker.__init__` takes every client plugin that is also a worker plugin and prepends it (`plugins_from_client + list(plugins)`), then calls `configure_worker` on each in order. So `StepledgerPlugin` passed to `Client.connect` configures the worker before a worker-level `LangGraphPlugin`, and its interceptor is outermost. | `temporalio/worker/_worker.py:397-411` |
| Workers use the client's data converter. | `worker/_worker.py:471`, `:497` |
| The replayer applies plugin data converter, workflow runner and worker interceptors. | `plugin.py:188-227` |
| `Worker(disable_payload_error_limit=False)` by default: the worker checks payload sizes before sending and fails the task (`WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE = 37`) instead of letting the server reject it. The check itself is in the Rust core. | `worker/_worker.py:153`, `:333-339`; `temporalio/api/enums/v1/failed_cause_pb2.py:85` |

## Metrics

| Fact | Where |
|---|---|
| `WorkflowHandle.describe()` returns `WorkflowExecutionDescription`; history size is `raw_info.history_size_bytes`, event count is `history_length`. | `temporalio/client/_workflow.py:1242`, `:1263`; `temporalio/api/workflow/v1/message_pb2.pyi:79,97` |

## LangGraph internals used by materialize

| Fact | Where |
|---|---|
| `task_path_str(path)` gives LangGraph's sortable path string (ints zero-padded to 10 digits, nested paths prefixed `~`). Private module. | `langgraph/pregel/_algo.py:1412-1420` |
| `MISSING` sentinel for `channel.from_checkpoint(MISSING)`. Private module. | `langgraph/_internal/_typing.py:45` |
| Channels: `from_checkpoint(checkpoint)`, `update(values)`, `get()`, `checkpoint()`. | `langgraph/channels/base.py:49-90` |

## Differences from the build spec

- Heartbeat payloads target the Activity, not the workflow (`worker/_activity.py:263-271`). The spec lists only workflow targets. Refs from heartbeats are stored with `workflow_id=''` and expire by `orphan_ref_days`.
- `temporalio` is MIT-licensed, not Apache-2.0. Stepledger stays Apache-2.0 as locked.
- Everything else in the spec's list of SDK facts matches the source.
