from dataclasses import dataclass, field
from typing import Any

from .node_cas import NodeCAS


@dataclass
class NodeJobManager(NodeCAS):
    # Request UID -> response available to the client.
    responses: dict[str, dict[str, Any]] = field(default_factory=dict)

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

        # Create the job only if this ID is not already stored.
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

    def claim_task(self, job_id: str, task_index: int) -> str:
        # Initially, the task key does not exist.
        # Only one worker can change it from None to claimed.
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

    def deliver(self, msg: dict[str, Any]) -> None:
        # Apply the operation after total-order delivery.
        super().deliver(msg)

        # Only the node that originated the request prepares its response.
        if msg["sender"] != self.node_id:
            return

        uid = msg["uid"]

        if msg.get("job_submission"):
            stored_job = self.store.get(msg["key"])
            submitted = stored_job == msg["new"]

            self.responses[uid] = {
                "type": "submit_job_response",
                "job_id": msg["job_id"],
                "job_submitted": submitted,
            }

        elif msg.get("job_status_query"):
            job = self.operation_results[uid]
            status = "not_found" if job is None else "in_progress"

            self.responses[uid] = {
                "type": "query_job_status_response",
                "job_id": msg["job_id"],
                "job_status": status,
            }
        elif msg.get("task_claim"):
            self.responses[uid] = {
                "type": "claim_task_response",
                "job_id": msg["job_id"],
                "task_index": msg["task_index"],
                "task_claimed": self.operation_results[uid],
            }