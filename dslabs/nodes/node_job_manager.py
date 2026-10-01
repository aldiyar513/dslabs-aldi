from dataclasses import dataclass, field
from typing import Any

from .node_cas import NodeCAS


@dataclass
class NodeJobManager(NodeCAS):
    responses: dict[str, dict[str, Any]] = field(default_factory=dict)
    executed_tasks: set[tuple[str, int]] = field(default_factory=set)
    started_jobs: set[str] = field(default_factory=set)

    def submit_job(
        self,
        job_id: str,
        job_action: str,
        job_data: list[Any],
    ) -> str:
        job = {
            "job_id": job_id,
            "job_action": job_action,
            "job_data": job_data,
        }

        return self._publish(
            "replicate",
            operation="cas",
            key=f"job:{job_id}",
            old=None,
            new=job,
            job_submission=True,
            job_id=job_id,
        )

    def query_job_status(self, job_id: str) -> str:
        return self._publish(
            "replicate",
            operation="get",
            key=f"job:{job_id}",
            job_status_query=True,
            job_id=job_id,
        )

    def get_job_results(self, job_id: str) -> str:
        return self._publish(
            "replicate",
            operation="get",
            key=f"job:{job_id}",
            job_results_query=True,
            job_id=job_id,
        )

    def claim_task(self, job_id: str, task_index: int) -> str:
        return self._publish(
            "replicate",
            operation="cas",
            key=f"task:{job_id}:{task_index}",
            old=None,
            new={
                "status": "claimed",
                "worker": self.node_id,
            },
            task_claim=True,
            job_id=job_id,
            task_index=task_index,
        )

    def _start_job(self, job_id: str) -> None:
        job = self.store[f"job:{job_id}"]
        members = sorted(set(self.peers) | {self.node_id})

        # Distribute inputs across workers in round-robin order.
        for index in range(len(job["job_data"])):
            owner = members[index % len(members)]

            if owner == self.node_id:
                self.claim_task(job_id, index)

    def _execute_task(self, job_id: str, task_index: int) -> None:
        job = self.store[f"job:{job_id}"]
        task_key = f"task:{job_id}:{task_index}"

        claimed_state = {
            "status": "claimed",
            "worker": self.node_id,
        }

        if self.store.get(task_key) != claimed_state:
            return

        # For this version, job_action is a trusted lambda string.
        action = eval(job["job_action"])
        result = action(job["job_data"][task_index])

        self.client_cas(
            task_key,
            claimed_state,
            {
                "status": "complete",
                "worker": self.node_id,
                "result": result,
            },
        )

    def _job_complete(self, job: dict[str, Any]) -> bool:
        job_id = job["job_id"]

        for index in range(len(job["job_data"])):
            task = self.store.get(f"task:{job_id}:{index}")

            if task is None or task["status"] != "complete":
                return False

        return True

    def deliver(self, msg: dict[str, Any]) -> None:
        super().deliver(msg)

        uid = msg["uid"]
        job_id = msg.get("job_id")

        # Every replica schedules its assigned tasks after learning
        # about an accepted job. Identical retries do not restart it.
        if msg.get("job_submission"):
            accepted = self.store.get(msg["key"]) == msg["new"]

            if accepted and job_id not in self.started_jobs:
                self.started_jobs.add(job_id)
                self.scheduler.call_later(
                    0,
                    lambda: self._start_job(job_id),
                )

        # Only the requesting node prepares the client response.
        if msg["sender"] != self.node_id:
            return

        if msg.get("job_submission"):
            self.responses[uid] = {
                "type": "submit_job_response",
                "job_id": job_id,
                "job_submitted": accepted,
            }

        elif msg.get("job_status_query"):
            job = self.operation_results[uid]

            if job is None:
                status = "not_found"
            elif self._job_complete(job):
                status = "complete"
            else:
                status = "in_progress"

            self.responses[uid] = {
                "type": "query_job_status_response",
                "job_id": job_id,
                "job_status": status,
            }

        elif msg.get("job_results_query"):
            job = self.operation_results[uid]
            results = []

            if job is not None and self._job_complete(job):
                results = [
                    self.store[f"task:{job_id}:{index}"]["result"]
                    for index in range(len(job["job_data"]))
                ]

            self.responses[uid] = {
                "type": "get_job_results_response",
                "job_id": job_id,
                "job_results": results,
            }

        elif msg.get("task_claim"):
            claimed = self.operation_results[uid]

            self.responses[uid] = {
                "type": "claim_task_response",
                "job_id": job_id,
                "task_index": msg["task_index"],
                "task_claimed": claimed,
            }

            if claimed:
                task_index = msg["task_index"]
                task_id = (job_id, task_index)

                if task_id not in self.executed_tasks:
                    self.executed_tasks.add(task_id)
                    self.scheduler.call_later(
                        0,
                        lambda: self._execute_task(job_id, task_index),
                    )