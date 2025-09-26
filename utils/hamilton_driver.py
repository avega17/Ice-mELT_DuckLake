"""
Hamilton Driver helpers built around driver.Builder(), per official docs.

Usage:
    from hamilton import driver as h_driver
    import my_flow
    from utils.hamilton_driver import build_driver, execute

    dr = build_driver([my_flow], config={"export": True}, enable_parallel=False)
    results = execute(dr, ["some_node"])  # {"some_node": value}

Notes:
- Follows https://hamilton.dagworks.io/en/latest/concepts/driver/ and /builder/
- Uses .enable_dynamic_execution(allow_experimental_mode=True) only when requested.
- No compatibility fallbacks or broad exception handling.
"""
from __future__ import annotations

from types import ModuleType
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from hamilton import driver as h_driver
from hamilton import driver


def build_driver(
    modules: Sequence[ModuleType],
    config: Optional[Mapping[str, Any]] = None,
    *,
    enable_parallel: bool = False,
    enable_cache: bool = True,
    adapters: Optional[Iterable[Any]] = None,
    local_executor: Any = None,
    remote_executor: Any = None,
) -> h_driver.Driver:
    """
    Construct a Hamilton Driver using driver.Builder().

    Args:
        modules: Python modules that define Hamilton nodes.
        config: Configuration dict passed via .with_config().
        enable_parallel: If True, enables the V2 driver via
            .enable_dynamic_execution(allow_experimental_mode=True).
            Note: If a local/remote executor is provided, the V2 driver will be
            enabled automatically even if enable_parallel=False.
        enable_cache: If True, call .with_cache().
        adapters: Optional lifecycle/graph adapters passed via .with_adapters().
        local_executor: Optional executor object for .with_local_executor().
        remote_executor: Optional executor object for .with_remote_executor().

    Returns:
        A built hamilton.driver.Driver instance.
    """
    b = driver.Builder().with_modules(*modules)
    if config is not None:
        b = b.with_config(dict(config))

    # Enable the V2 driver (dynamic execution) if:
    # - explicitly requested via enable_parallel, or
    # - an executor is provided (required by Hamilton for execution managers).
    need_dynamic = bool(enable_parallel or local_executor is not None or remote_executor is not None)
    if need_dynamic:
        b = b.enable_dynamic_execution(allow_experimental_mode=True)

    if enable_cache:
        b = b.with_cache()
    if adapters:
        b = b.with_adapters(*adapters)
    if local_executor is not None:
        b = b.with_local_executor(local_executor)
    if remote_executor is not None:
        b = b.with_remote_executor(remote_executor)
    return b.build()


def execute(
    dr: h_driver.Driver,
    outputs: Sequence[str],
    *,
    inputs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute the DAG for the requested outputs and return {node_name: value}."""
    return dr.execute(list(outputs), inputs=dict(inputs or {}))


def build_and_execute(
    modules: Sequence[ModuleType],
    outputs: Sequence[str],
    *,
    config: Optional[Mapping[str, Any]] = None,
    enable_parallel: bool = False,
    enable_cache: bool = True,
    adapters: Optional[Iterable[Any]] = None,
    local_executor: Any = None,
    remote_executor: Any = None,
    inputs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Convenience wrapper: build a Driver, then execute the requested outputs."""
    dr = build_driver(
        modules,
        config,
        enable_parallel=enable_parallel,
        enable_cache=enable_cache,
        adapters=adapters,
        local_executor=local_executor,
        remote_executor=remote_executor,
    )
    return execute(dr, outputs, inputs=inputs)

