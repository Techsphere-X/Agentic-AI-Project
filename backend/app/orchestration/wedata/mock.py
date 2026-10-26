"""Mock Tencent Cloud WeData adapter for offline development, local demos, and CI tests.

Supports ?simulate=<STATUS> in the endpoint/project_id to force error statuses,
e.g. simulate=UNAUTHORIZED, simulate=FORBIDDEN, simulate=UNREACHABLE.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

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

MOCK_TASKS: tuple[WeDataTaskSummary, ...] = (
    WeDataTaskSummary(
        task_id="20251025_task_spark_orders",
        task_name="ods_to_dwd_orders_spark",
        workflow_id="wf_retail_etl_01",
        workflow_name="Retail_ETL_Daily",
        task_type="SparkSQL",
        schedule_cron="*/5 * * * *",
        is_paused=False,
        owner="100028471920",
        tags=["etl", "orders", "spark"],
    ),
    WeDataTaskSummary(
        task_id="20251025_task_dq_checks",
        task_name="wedata_dq_rule_orders",
        workflow_id="wf_retail_etl_01",
        workflow_name="Retail_ETL_Daily",
        task_type="DataQuality",
        schedule_cron="*/30 * * * *",
        is_paused=False,
        owner="100028471920",
        tags=["quality", "rules"],
    ),
    WeDataTaskSummary(
        task_id="20251025_task_customer_churn",
        task_name="ml_customer_churn_prediction",
        workflow_id="wf_analytics_ml_02",
        workflow_name="Customer_Analytics_ML",
        task_type="PySpark",
        schedule_cron="0 2 * * *",
        is_paused=False,
        owner="100039281744",
        tags=["ml", "analytics", "pyspark"],
    ),
    WeDataTaskSummary(
        task_id="20251025_task_legacy_sync",
        task_name="legacy_oracle_inventory_sync",
        workflow_id="wf_legacy_migration_03",
        workflow_name="Legacy_Inventory_Pipeline",
        task_type="Datax",
        schedule_cron="0 4 * * *",
        is_paused=True,
        owner="100010293847",
        tags=["legacy", "inventory"],
    ),
)

_SPARK_ERROR_LOG = """2026-10-25 20:20:00 INFO TaskSetManager: Starting task 0.0 in stage 3.0 (TID 12, 10.0.1.42, executor 1, partition 0, PROCESS_LOCAL, 7856 bytes)
2026-10-25 20:20:02 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 10.0.1.42:38491 (size: 45.2 KiB, free: 3.2 GiB)
2026-10-25 20:20:08 ERROR Executor: Exception in task 0.0 in stage 3.0 (TID 12)
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 3.0 failed 4 times, most recent failure: Lost task 0.3 in stage 3.0: java.lang.OutOfMemoryError: Java heap space
\tat org.apache.spark.sql.catalyst.expressions.UnsafeRow.copy(UnsafeRow.java:543)
\tat org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec.processHashAggregate(ObjectHashAggregateExec.scala:212)
\tat org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:180)
\tat org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$1(RDD.scala:863)
2026-10-25 20:20:09 ERROR WeDataExecutor: Task instance execution failed. Exit code 137. Container killed by YARN for exceeding memory limits.
"""

_SUCCESS_LOG = """2026-10-25 20:25:00 INFO WeDataDriver: Task started with configuration: spark.executor.memory=8g, spark.driver.memory=4g
2026-10-25 20:25:12 INFO SparkSession: Running query on database `dwd_retail`...
2026-10-25 20:25:35 INFO FileFormatWriter: Job job_20261025_001 committed successfully. Wrote 1,425,890 records.
2026-10-25 20:25:38 INFO WeDataDriver: Task completed with status: SUCCESS. Elapsed: 38s.
"""


class MockWeDataAdapter(WeDataAdapter):
    """In-memory mock WeData adapter for development and testing."""

    def __init__(self, config: WeDataAdapterConfig) -> None:
        self.config = config
        self._rerun_instances: set[str] = set()
        self._paused_overrides: dict[str, bool] = {}

    def _simulated_status(self) -> WeDataConnectionStatus | None:
        """Check if simulation query parameter is present in endpoint or project_id."""
        target = f"{self.config.endpoint}&{self.config.project_id}"
        if "simulate=" in target:
            val = target.split("simulate=")[1].split("&")[0].upper()
            try:
                return WeDataConnectionStatus(val)
            except ValueError:
                return WeDataConnectionStatus.WEDATA_ERROR
        return None

    async def probe(self) -> WeDataConnectionTestResult:
        sim = self._simulated_status()
        if sim is not None:
            if sim == WeDataConnectionStatus.UNAUTHORIZED:
                return WeDataConnectionTestResult(
                    status=sim,
                    message="AuthFailure.SecretIdNotFound: The specified SecretId does not exist.",
                    latency_ms=12,
                    project_id=self.config.project_id,
                )
            if sim == WeDataConnectionStatus.FORBIDDEN:
                return WeDataConnectionTestResult(
                    status=sim,
                    message="UnauthorizedOperation: The CAM user has no permission for WeData DescribeTasks.",
                    latency_ms=15,
                    project_id=self.config.project_id,
                )
            if sim == WeDataConnectionStatus.UNREACHABLE:
                return WeDataConnectionTestResult(
                    status=sim,
                    message=f"Cannot reach WeData endpoint {self.config.endpoint}.",
                    project_id=self.config.project_id,
                )
            return WeDataConnectionTestResult(
                status=sim,
                message=f"Simulated error: {sim}",
                project_id=self.config.project_id,
            )

        await asyncio.sleep(0.01)
        return WeDataConnectionTestResult(
            status=WeDataConnectionStatus.HEALTHY,
            message=f"Mock WeData connected successfully (Project: {self.config.project_id}, Region: {self.config.region}).",
            latency_ms=8,
            project_id=self.config.project_id,
            project_name=f"Mock-Project-{self.config.project_id}",
        )

    async def list_tasks(self) -> list[WeDataTaskSummary]:
        sim = self._simulated_status()
        if sim is not None and sim != WeDataConnectionStatus.HEALTHY:
            raise WeDataAdapterError(
                WeDataConnectionTestResult(status=sim, message=f"Mock failure: {sim}")
            )

        tasks: list[WeDataTaskSummary] = []
        for t in MOCK_TASKS:
            paused = self._paused_overrides.get(t.task_id, t.is_paused)
            tasks.append(
                WeDataTaskSummary(
                    task_id=t.task_id,
                    task_name=t.task_name,
                    workflow_id=t.workflow_id,
                    workflow_name=t.workflow_name,
                    task_type=t.task_type,
                    schedule_cron=t.schedule_cron,
                    is_paused=paused,
                    owner=t.owner,
                    tags=list(t.tags),
                )
            )
        return tasks

    async def list_task_instances(
        self, task_id: str, *, limit: int = 100
    ) -> list[WeDataInstanceRun]:
        now = datetime.now(UTC)
        runs: list[WeDataInstanceRun] = []
        for i in range(min(limit, 5)):
            run_date = (now - timedelta(minutes=i * 5)).strftime("%Y-%m-%d %H:%M:00")
            run_id = f"{task_id}_{run_date.replace(' ', '_').replace(':', '')}"

            if run_id in self._rerun_instances:
                state = "SUCCESS"
            elif task_id == "20251025_task_spark_orders" and i == 0:
                state = "FAILED"
            elif i == 1:
                state = "SUCCESS"
            else:
                state = "SUCCESS"

            start_t = now - timedelta(minutes=i * 5, seconds=45)
            end_t = now - timedelta(minutes=i * 5, seconds=5)
            runs.append(
                WeDataInstanceRun(
                    run_id=run_id,
                    task_id=task_id,
                    state=state,
                    cur_run_date=run_date,
                    start_date=start_t,
                    end_date=end_t,
                )
            )
        return runs

    async def get_task_log(
        self, task_id: str, run_id: str, *, max_bytes: int = 65536
    ) -> WeDataTaskLog:
        if "FAILED" in run_id or task_id == "20251025_task_spark_orders":
            text, truncated = tail_text(_SPARK_ERROR_LOG, max_bytes)
        else:
            text, truncated = tail_text(_SUCCESS_LOG, max_bytes)
        return WeDataTaskLog(content=text, truncated=truncated, available=True)

    async def rerun_instance(self, task_id: str, run_id: str) -> bool:
        self._rerun_instances.add(run_id)
        return True

    async def kill_instance(self, task_id: str, run_id: str) -> bool:
        return True

    async def set_task_paused(self, task_id: str, paused: bool) -> None:
        self._paused_overrides[task_id] = paused
