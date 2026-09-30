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

    def deliver(self, msg: dict[str, Any]) -> None:
        # Apply the ordered operation and record its result.
        super().deliver(msg)

        if msg.get("job_status_query") and msg["sender"] == self.node_id:
            uid = msg["uid"]
            job = self.operation_results[uid]

            status = "not_found" if job is None else "in_progress"

            self.responses[uid] = {
                "type": "query_job_status_response",
                "job_id": msg["job_id"],
                "job_status": status,
            }