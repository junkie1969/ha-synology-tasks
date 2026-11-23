"""Data update coordinator for Synology Tasks."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from time import monotonic

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .const import DEFAULT_SCAN_INTERVAL, DOMAIN
from .models import Task
from .synology import SynologyDSM, SynologyTaskRunError

_LOGGER = logging.getLogger(__name__)

# Initial local selection; not inferred from whether tasks have schedules.
MONITORED_TASK_IDS = frozenset({25, 26})

ACTIVE_SCAN_INTERVAL = 10
START_CONFIRM_TIMEOUT = 120

# Monitoring limit only: reaching this does not stop the DSM task.
RUN_MONITOR_TIMEOUT = 24 * 60 * 60


@dataclass
class TaskRunState:
    """Latest observed execution information and tracking state."""

    status: str = "unknown"
    latest_run: dict | None = None
    baseline_start: str | None = None
    tracked_start: str | None = None
    active: bool = False
    waiting_for_start: bool = False
    deadline: float = 0
    error: str | None = None


class SynologyTasksCoordinator(DataUpdateCoordinator[list[Task]]):
    """Fetch task configuration and track selected task executions."""

    def __init__(
        self,
        hass: HomeAssistant,
        dsm_entry: ConfigEntry,
    ) -> None:
        """Initialize the data updater."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )
        self.api = SynologyDSM(hass, dsm_entry)
        self.run_states = {
            task_id: TaskRunState()
            for task_id in MONITORED_TASK_IDS
        }
        self._initial_checks_done: set[int] = set()
        self._request_lock = asyncio.Lock()

    def _set_interval(self) -> None:
        """Use faster polling only while a run is being monitored."""
        active = any(state.active for state in self.run_states.values())
        seconds = ACTIVE_SCAN_INTERVAL if active else DEFAULT_SCAN_INTERVAL
        self.update_interval = timedelta(seconds=seconds)

    def _finish_from_result(
        self,
        task_id: int,
        state: TaskRunState,
        latest: dict,
    ) -> None:
        """Store a terminal DSM result."""
        exit_type = latest.get("exit_type")
        exit_code = latest.get("exit_code")

        if exit_type == "normal":
            state.status = "success" if exit_code == 0 else "failed"
        elif exit_type == "by_signal":
            state.status = "interrupted"
        else:
            state.status = "unknown"

        state.active = False
        state.waiting_for_start = False

        _LOGGER.debug(
            "Task %s result: status=%s, start=%s, stop=%s, exit_code=%s",
            task_id,
            state.status,
            latest.get("start_time"),
            latest.get("stop_time"),
            exit_code,
        )

    def _apply_run(
        self,
        task_id: int,
        latest: dict | None,
    ) -> None:
        """Update tracking using a successful history response."""
        state = self.run_states[task_id]
        state.latest_run = latest
        state.error = None

        if state.waiting_for_start:
            start = latest.get("start_time") if latest else None

            if not start or (
                state.baseline_start is not None
                and start <= state.baseline_start
            ):
                state.status = "starting"
                return

            state.tracked_start = start
            state.waiting_for_start = False
            state.deadline = monotonic() + RUN_MONITOR_TIMEOUT

            _LOGGER.debug(
                "Confirmed new run for task %s: start=%s",
                task_id,
                start,
            )

        if latest is None:
            if state.active:
                state.status = "monitoring_error"
                state.error = "The tracked run is missing from history"
            else:
                state.status = "no_history"
            return

        start = latest.get("start_time")

        if state.active and state.tracked_start != start:
            state.status = "monitoring_error"
            state.error = "Latest history entry no longer matches tracked run"
            return

        if latest.get("exit_type") == "running":
            state.status = "running"

            if not state.active:
                state.active = True
                state.tracked_start = start
                state.deadline = monotonic() + RUN_MONITOR_TIMEOUT

            return

        self._finish_from_result(task_id, state, latest)

    async def _async_update_data(self) -> list[Task]:
        """Refresh task configuration and selected active runs."""
        async with self._request_lock:
            try:
                tasks = await self.api.get_tasks()
            except Exception as err:
                raise UpdateFailed(
                    f"Error communicating with DSM: {err}"
                ) from err

            tasks_by_id = {task.id: task for task in tasks}

            for task_id, state in self.run_states.items():
                task = tasks_by_id.get(task_id)

                if task is None:
                    state.status = "task_missing"
                    state.error = "Task is not present in the task list"
                    state.active = False
                    state.waiting_for_start = False
                    continue

                if not task.enabled:
                    state.status = "disabled"
                    state.latest_run = None
                    state.active = False
                    state.waiting_for_start = False
                    state.error = None
                    self._initial_checks_done.discard(task_id)
                    continue

                if state.active and monotonic() >= state.deadline:
                    state.status = (
                        "start_not_confirmed"
                        if state.waiting_for_start
                        else "monitoring_timeout"
                    )
                    state.error = "Monitoring deadline reached"
                    state.active = False
                    state.waiting_for_start = False
                    _LOGGER.warning(
                        "Task %s: %s; DSM task has not been stopped",
                        task_id,
                        state.status,
                    )
                    continue

                if (
                    task_id in self._initial_checks_done
                    and not state.active
                ):
                    continue

                try:
                    latest = await self.api.get_latest_run(task_id)
                except Exception:
                    state.status = "monitoring_error"
                    state.error = "Unable to read DSM run history"
                    _LOGGER.warning(
                        "Unable to read history for task %s",
                        task_id,
                        exc_info=True,
                    )
                    continue

                self._initial_checks_done.add(task_id)
                self._apply_run(task_id, latest)

                _LOGGER.debug(
                    "Task %s monitoring: status=%s, active=%s",
                    task_id,
                    state.status,
                    state.active,
                )

            self._set_interval()
            return tasks

    async def async_run_task(self, task_id: int) -> None:
        """Launch a task, tracking selected enabled tasks."""
        async with self._request_lock:
            tasks = await self.api.get_tasks()
            task = next(
                (item for item in tasks if item.id == task_id),
                None,
            )

            if task is None:
                raise SynologyTaskRunError(
                    f"Task ID {task_id} was not found"
                )

            if task_id not in self.run_states:
                await self.api.run_task(task_id)
            else:
                if not task.enabled:
                    raise SynologyTaskRunError(
                        f"Task {task.name!r} is disabled"
                    )

                state = self.run_states[task_id]

                if state.active:
                    raise SynologyTaskRunError(
                        f"Task {task.name!r} is already being monitored"
                    )

                latest = await self.api.get_latest_run(task_id)
                self._initial_checks_done.add(task_id)
                self._apply_run(task_id, latest)

                if state.active:
                    _LOGGER.info(
                        "Task %s is already running; following existing run",
                        task_id,
                    )
                else:
                    state.baseline_start = (
                        latest.get("start_time") if latest else None
                    )
                    state.tracked_start = None
                    state.status = "starting"
                    state.active = True
                    state.waiting_for_start = True
                    state.error = None
                    state.deadline = monotonic() + START_CONFIRM_TIMEOUT

                    try:
                        await self.api.run_task(task_id)
                    except Exception:
                        state.status = "request_error"
                        state.error = (
                            "Launch request failed or its outcome is uncertain"
                        )
                        state.active = False
                        state.waiting_for_start = False
                        self._set_interval()
                        self.async_update_listeners()
                        raise

                    _LOGGER.debug(
                        "Task %s launch accepted; waiting for run newer than %s",
                        task_id,
                        state.baseline_start,
                    )

            self._set_interval()
            self.async_update_listeners()

        # Refresh outside the request lock to avoid deadlock.
        await self.async_request_refresh()
