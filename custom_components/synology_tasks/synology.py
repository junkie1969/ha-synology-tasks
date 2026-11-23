"""API client for Synology DSM."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import requests
from homeassistant.exceptions import HomeAssistantError

from .const import (
    API_ENABLE_SYNO_TOKEN,
    API_ERROR_CODE_INVALID_SESSION,
    API_EVENT_SCHEDULER,
    API_LOGIN,
    API_METHOD_EVENT_SCHEDULER,
    API_METHOD_LOGIN,
    API_METHOD_TASK_SCHEDULER,
    API_SORT_BY_NAME,
    API_SORT_DIRECTION_ASC,
    API_SYNO_TOKEN,
    API_TASK_LIMIT,
    API_TASK_OFFSET,
    API_TASK_SCHEDULER,
    API_UNKNOWN,
    API_VERSION_EVENT_SCHEDULER,
    API_VERSION_LOGIN,
    API_VERSION_TASK_SCHEDULER,
    API_WEBAPI_ENDPOINT,
    API_YES,
    DATA_CODE_KEY,
    DATA_ERROR_KEY,
    DATA_KEY,
    DATA_SID_KEY,
    DATA_SUCCESS_KEY,
    DATA_SYNOTOKEN_KEY,
    DATA_TASKS_KEY,
    PROTOCOL_HTTP,
    PROTOCOL_HTTPS,
    SERVICE_DATA_TASK_NAME,
)
from .models import (
    SynologyAuthData,
    SynologyResponse,
    SynologyTask,
    SynologyTaskData,
    Task,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)


class SynologyDSM:
    """Synology DSM API client."""

    def __init__(self, hass: HomeAssistant, dsm_entry: ConfigEntry) -> None:
        """Initialize the API client."""
        self.hass = hass

        host = dsm_entry.data.get("host")
        port = dsm_entry.data.get("port")
        ssl = dsm_entry.data.get("ssl", True)

        self._verify_ssl = dsm_entry.data.get("verify_ssl", True)
        self._username = dsm_entry.data.get("username")
        self._password = dsm_entry.data.get("password")
        self._url = f"{(ssl and PROTOCOL_HTTPS) or PROTOCOL_HTTP}://{host}:{port}{API_WEBAPI_ENDPOINT}"

        self._session = None
        self._sid = None
        self._synotoken = None

    async def get_tasks(self) -> list[Task]:
        """Get list of tasks."""
        params = {
            "api": API_TASK_SCHEDULER,
            "method": API_METHOD_TASK_SCHEDULER,
            "version": API_VERSION_TASK_SCHEDULER,
            "sort_by": API_SORT_BY_NAME,
            "sort_direction": API_SORT_DIRECTION_ASC,
            "limit": API_TASK_LIMIT,
            "offset": API_TASK_OFFSET,
        }

        try:
            response = await self.hass.async_add_executor_job(
                self._sync_request, params
            )
        except Exception as err:
            _LOGGER.exception("Error getting tasks")
            raise SynologyTaskRunError from err

        tasks_data: SynologyTaskData = response.get(DATA_KEY, {})
        tasks: list[SynologyTask] = tasks_data.get(DATA_TASKS_KEY, [])
        return [Task.from_api(task) for task in tasks]

    async def get_latest_run(self, task_id: int) -> dict | None:
        """Return the latest DSM run, or None if there is no history."""
        params = {
            "api": API_TASK_SCHEDULER,
            "method": "view",
            "version": "1",
            "id": int(task_id),
        }

        response = await self.hass.async_add_executor_job(
            self._sync_request, params
        )

        history = response.get(DATA_KEY)

        if not isinstance(history, list):
            raise SynologyDSMAPIError(
                f"Unexpected run-history response for task ID {task_id}"
            )

        if not history:
            return None

        for run in history:
            if (
                not isinstance(run, dict)
                or not isinstance(run.get("start_time"), str)
                or not run["start_time"]
            ):
                raise SynologyDSMAPIError(
                    f"Invalid run-history entry for task ID {task_id}"
                )

        latest = max(history, key=lambda run: run["start_time"])

        return {
            "start_time": latest["start_time"],
            "stop_time": latest.get("stop_time") or None,
            "exit_type": latest.get("exit_type"),
            "exit_code": latest.get("exit_code"),
        }
    
    async def run_task(self, task_name: str | int) -> None:
        """Run a task by ID, retaining support for callers using its name."""
        # Buttons pass the numeric ID. Existing service callers may
        # still pass a task name, so resolve those without breaking them.
        if isinstance(task_name, int):
            task_id = task_name
        else:
            tasks = await self.get_tasks()
            matches = [task for task in tasks if task.name == task_name]

            if len(matches) != 1:
                raise SynologyTaskRunError(
                    f"Expected one task named {task_name!r}; "
                    f"found {len(matches)}"
                )

            task_id = int(matches[0].id)

        params = {
            "api": "SYNO.Entry.Request",
            "method": "request",
            "version": "1",
            "stop_when_error": "false",
            "mode": json.dumps("sequential"),
            "compound": json.dumps(
                [
                    {
                        "api": API_TASK_SCHEDULER,
                        "method": "run",
                        "version": 1,
                        "task": [task_id],
                    }
                ]
            ),
        }

        _LOGGER.debug("Requesting DSM execution of task ID %s", task_id)

        try:
            response = await self.hass.async_add_executor_job(
                self._sync_request, params
            )
        except Exception as err:
            _LOGGER.exception("Error running task ID %s", task_id)
            raise SynologyTaskRunError(
                f"Unable to request execution of task ID {task_id}"
            ) from err

        _LOGGER.debug(
            "DSM task-run response for task ID %s: %s",
            task_id,
            response,
        )
        # Temporary diagnostic: remove when monitoring is implemented.
        if task_id in {25, 26}:
            try:
                latest = await self.get_latest_run(task_id)
                _LOGGER.debug(
                    "DSM latest run for task ID %s: %s",
                    task_id,
                    latest,
                )
            except Exception:
                _LOGGER.warning(
                    "Task run was requested, but reading history failed "
                    "for task ID %s",
                    task_id,
                    exc_info=True,
                )
    
    def _sync_login(self) -> None:
        """Login to the DSM."""
        if self._session is None:
            self._session = requests.Session()

        params = {
            "api": API_LOGIN,
            "version": API_VERSION_LOGIN,
            "method": API_METHOD_LOGIN,
            API_ENABLE_SYNO_TOKEN: API_YES,
            "account": self._username,
            "passwd": self._password,
        }

        response = self._sync_request(params, is_login=True)
        data: SynologyAuthData = response.get(DATA_KEY, {})
        self._sid = data.get(DATA_SID_KEY)
        self._synotoken = data.get(DATA_SYNOTOKEN_KEY)

    def _sync_request(
        self, params: dict | None = None, *, is_login: bool = False
    ) -> SynologyResponse:
        """Send a DSM request, retrying once after an expired session."""
        request_params = dict(params or {})

        if not is_login and (
            not self._sid or not self._synotoken or not self._session
        ):
            self._sync_login()

        if self._session is None:
            self._session = requests.Session()

        # Login uses auth.cgi; task and event requests use entry.cgi.
        request_url = (
            self._url.rsplit("/", 1)[0] + "/auth.cgi"
            if is_login
            else self._url
        )

        # Never retry login automatically. Other requests may retry once
        # after receiving the invalid-session error.
        attempts = 1 if is_login else 2

        for attempt in range(attempts):
            if not is_login:
                request_params[API_SYNO_TOKEN] = self._synotoken

            try:
                r = self._session.get(
                    request_url,
                    params=request_params,
                    verify=self._verify_ssl,
                    timeout=30,
                )
                response: SynologyResponse = r.json()
            except Exception:
                # Avoid logging the request URL: login parameters
                # contain the DSM credentials.
                raise SynologyDSMAPIError(
                    "Unable to read a valid response from DSM"
                ) from None

            if response.get(DATA_SUCCESS_KEY, False):
                return response

            error_code = response.get(DATA_ERROR_KEY, {}).get(
                DATA_CODE_KEY, API_UNKNOWN
            )

            if (
                not is_login
                and attempt == 0
                and error_code == API_ERROR_CODE_INVALID_SESSION
            ):
                _LOGGER.debug(
                    "DSM session expired (error %s); logging in again",
                    error_code,
                )
                self._sid = None
                self._synotoken = None
                self._sync_login()
                continue

            error_message = (
                f"DSM API {request_params.get('api')} "
                f"method {request_params.get('method')} "
                f"version {request_params.get('version')} "
                f"at {request_url} returned error code: {error_code}"
            )
            _LOGGER.error("%s", error_message)
            raise SynologyDSMAPIError(error_message)

        raise SynologyDSMAPIError("DSM request failed after retry")

class SynologyTaskRunError(HomeAssistantError):
    """Error to indicate the task run failed."""


class SynologyDSMAPIError(HomeAssistantError):
    """Error to indicate the DSM API error."""
