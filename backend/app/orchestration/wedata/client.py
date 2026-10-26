"""Real Tencent Cloud WeData adapter using API 3.0 (TC3-HMAC-SHA256 signing) with async httpx."""

import hashlib
import hmac
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

import httpx

from app.orchestration.wedata.base import (
    WeDataAdapter,
    WeDataAdapterConfig,
    WeDataAdapterError,
    WeDataConnectionStatus,
    WeDataConnectionTestResult,
    WeDataInstanceRun,
    WeDataTaskLog,
    WeDataTaskSummary,
    tail_text,
)

logger = logging.getLogger(__name__)


def build_tc3_headers(
    *,
    secret_id: str,
    secret_key: str,
    region: str,
    endpoint: str,
    action: str,
    payload_str: str,
    version: str = "2021-08-20",
    service: str = "wedata",
    timestamp: int | None = None,
) -> dict[str, str]:
    """Generate Tencent Cloud API 3.0 request headers with TC3-HMAC-SHA256 signature."""
    if timestamp is None:
        timestamp = int(time.time())
    date_str = datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%d")

    http_request_method = "POST"
    canonical_uri = "/"
    canonical_querystring = ""
    canonical_headers = (
        f"content-type:application/json; charset=utf-8\n"
        f"host:{endpoint}\n"
        f"x-tc-action:{action.lower()}\n"
    )
    signed_headers = "content-type;host;x-tc-action"
    hashed_request_payload = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

    canonical_request = (
        f"{http_request_method}\n"
        f"{canonical_uri}\n"
        f"{canonical_querystring}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{hashed_request_payload}"
    )

    algorithm = "TC3-HMAC-SHA256"
    credential_scope = f"{date_str}/{service}/tc3_request"
    hashed_canonical_request = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    string_to_sign = (
        f"{algorithm}\n"
        f"{timestamp}\n"
        f"{credential_scope}\n"
        f"{hashed_canonical_request}"
    )

    secret_date = hmac.new(
        ("TC3" + secret_key).encode("utf-8"), date_str.encode("utf-8"), hashlib.sha256
    ).digest()
    secret_service = hmac.new(secret_date, service.encode("utf-8"), hashlib.sha256).digest()
    secret_signing = hmac.new(secret_service, b"tc3_request", hashlib.sha256).digest()
    signature = hmac.new(secret_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"{algorithm} Credential={secret_id}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    return {
        "Authorization": authorization,
        "Content-Type": "application/json; charset=utf-8",
        "Host": endpoint,
        "X-TC-Action": action,
        "X-TC-Timestamp": str(timestamp),
        "X-TC-Version": version,
        "X-TC-Region": region,
    }


class LiveWeDataAdapter(WeDataAdapter):
    """Communicates with Tencent Cloud WeData API."""

    def __init__(self, config: WeDataAdapterConfig) -> None:
        self.config = config
        self._url = f"https://{config.endpoint}"

    async def _call_api(self, action: str, body: dict[str, Any]) -> dict[str, Any]:
        """Perform a signed POST request to Tencent Cloud WeData API."""
        if not self.config.secret_id or not self.config.secret_key:
            raise WeDataAdapterError(
                WeDataConnectionTestResult(
                    status=WeDataConnectionStatus.UNAUTHORIZED,
                    message="Missing SecretId or SecretKey for Tencent Cloud WeData.",
                )
            )

        payload_str = json.dumps(body)
        headers = build_tc3_headers(
            secret_id=self.config.secret_id,
            secret_key=self.config.secret_key,
            region=self.config.region,
            endpoint=self.config.endpoint,
            action=action,
            payload_str=payload_str,
        )

        timeout = httpx.Timeout(
            connect=self.config.connect_timeout,
            read=self.config.read_timeout,
            write=self.config.connect_timeout,
            pool=self.config.connect_timeout,
        )

        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(self._url, headers=headers, content=payload_str.encode("utf-8"))
            if resp.status_code != 200:
                raise WeDataAdapterError(
                    WeDataConnectionTestResult(
                        status=WeDataConnectionStatus.WEDATA_ERROR,
                        message=f"HTTP {resp.status_code} from Tencent Cloud: {resp.text[:300]}",
                    )
                )

            data = resp.json()
            response_payload = data.get("Response", {})
            if "Error" in response_payload:
                err = response_payload["Error"]
                code = err.get("Code", "UnknownError")
                msg = err.get("Message", "Unknown Tencent Cloud WeData Error")
                status = self._map_error_code(code)
                raise WeDataAdapterError(
                    WeDataConnectionTestResult(
                        status=status,
                        message=f"[{code}] {msg}",
                        project_id=self.config.project_id,
                    )
                )
            return response_payload

    def _map_error_code(self, code: str) -> WeDataConnectionStatus:
        if "AuthFailure" in code or "SignatureFailure" in code:
            return WeDataConnectionStatus.UNAUTHORIZED
        if "UnauthorizedOperation" in code or "Forbidden" in code:
            return WeDataConnectionStatus.FORBIDDEN
        if "ResourceNotFound" in code or "InvalidParameter" in code:
            return WeDataConnectionStatus.INVALID_ENDPOINT
        return WeDataConnectionStatus.WEDATA_ERROR

    async def probe(self) -> WeDataConnectionTestResult:
        t0 = time.perf_counter()
        try:
            # Probe with DescribeTasks for this ProjectId
            res = await self._call_api(
                "DescribeTasks",
                {"ProjectId": self.config.project_id, "PageNumber": 1, "PageSize": 1},
            )
            latency = int((time.perf_counter() - t0) * 1000)
            return WeDataConnectionTestResult(
                status=WeDataConnectionStatus.HEALTHY,
                message=f"Connected to WeData ({self.config.region}, project {self.config.project_id}).",
                latency_ms=latency,
                project_id=self.config.project_id,
            )
        except WeDataAdapterError as e:
            latency = int((time.perf_counter() - t0) * 1000)
            return WeDataConnectionTestResult(
                status=e.result.status,
                message=e.result.message,
                latency_ms=latency,
                project_id=self.config.project_id,
            )
        except httpx.ConnectError:
            return WeDataConnectionTestResult(
                status=WeDataConnectionStatus.UNREACHABLE,
                message=f"Cannot reach Tencent Cloud endpoint {self.config.endpoint}.",
                project_id=self.config.project_id,
            )
        except httpx.TimeoutException:
            return WeDataConnectionTestResult(
                status=WeDataConnectionStatus.UNREACHABLE,
                message="Timeout connecting to Tencent Cloud WeData API.",
                project_id=self.config.project_id,
            )
        except Exception as ex:
            return WeDataConnectionTestResult(
                status=WeDataConnectionStatus.WEDATA_ERROR,
                message=f"Unexpected error probing WeData: {ex}",
                project_id=self.config.project_id,
            )

    async def list_tasks(self) -> list[WeDataTaskSummary]:
        body = {
            "ProjectId": self.config.project_id,
            "PageNumber": 1,
            "PageSize": 100,
        }
        res = await self._call_api("DescribeTasks", body)
        data = res.get("Data", {})
        items = data.get("Items", []) or data.get("Rows", []) or []
        tasks: list[WeDataTaskSummary] = []
        for item in items:
            tid = str(item.get("TaskId") or item.get("Id") or "")
            tname = str(item.get("TaskName") or item.get("Name") or tid)
            tasks.append(
                WeDataTaskSummary(
                    task_id=tid,
                    task_name=tname,
                    workflow_id=item.get("WorkflowId"),
                    workflow_name=item.get("WorkflowName"),
                    task_type=item.get("TaskTypeDesc") or item.get("TaskType") or "SparkSQL",
                    schedule_cron=item.get("CrontabExpression") or item.get("Cron"),
                    is_paused=bool(item.get("Status") == "FROZEN" or item.get("IsPaused")),
                    owner=item.get("OwnerUin") or item.get("InCharge"),
                )
            )
        return tasks

    async def list_task_instances(
        self, task_id: str, *, limit: int = 100
    ) -> list[WeDataInstanceRun]:
        body = {
            "ProjectId": self.config.project_id,
            "TaskId": task_id,
            "PageNumber": 1,
            "PageSize": min(limit, 100),
        }
        res = await self._call_api("ListInstances", body)
        data = res.get("Data", {})
        items = data.get("Items", []) or []
        runs: list[WeDataInstanceRun] = []
        for item in items:
            run_id = str(item.get("InstanceKey") or item.get("RunId") or item.get("Id") or "")
            state = str(item.get("State") or item.get("Status") or "UNKNOWN")
            cur_run_date = item.get("CurRunDate") or item.get("ExecutionDate")
            runs.append(
                WeDataInstanceRun(
                    run_id=run_id,
                    task_id=task_id,
                    state=state,
                    cur_run_date=cur_run_date,
                )
            )
        return runs

    async def get_task_log(
        self, task_id: str, run_id: str, *, max_bytes: int = 65536
    ) -> WeDataTaskLog:
        body = {
            "ProjectId": self.config.project_id,
            "InstanceKey": run_id,
            "TaskId": task_id,
        }
        try:
            res = await self._call_api("DescribeInstanceLogDetail", body)
            log_content = res.get("Data", {}).get("Content", "") or ""
            text, truncated = tail_text(log_content, max_bytes)
            return WeDataTaskLog(content=text, truncated=truncated, available=True)
        except Exception as e:
            logger.warning("Could not fetch log for WeData instance %s: %s", run_id, e)
            return WeDataTaskLog(content="", truncated=False, available=False)

    async def rerun_instance(self, task_id: str, run_id: str) -> bool:
        body = {
            "ProjectId": self.config.project_id,
            "InstanceKeys": [run_id],
            "TaskId": task_id,
        }
        res = await self._call_api("RerunTaskInstances", body)
        return bool(res.get("Data", {}).get("Success", True))

    async def kill_instance(self, task_id: str, run_id: str) -> bool:
        body = {
            "ProjectId": self.config.project_id,
            "InstanceKeys": [run_id],
            "TaskId": task_id,
        }
        res = await self._call_api("KillTaskInstances", body)
        return bool(res.get("Data", {}).get("Success", True))

    async def set_task_paused(self, task_id: str, paused: bool) -> None:
        action = "FreezeOpsTasks" if paused else "UnfreezeOpsTasks"
        body = {
            "ProjectId": self.config.project_id,
            "TaskIds": [task_id],
        }
        await self._call_api(action, body)
