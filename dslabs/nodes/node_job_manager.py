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

    def deliver(self, msg: dict[str, Any]) -> None:
        # Apply the ordered operation and record its result.
        super().deliver(msg)

        if msg.get("job_submission") and msg["sender"] == self.node_id:
            uid = msg["uid"]
            stored_job = self.store.get(msg["key"])

            # An identical retry is also considered submitted.
            submitted = stored_job == msg["new"]

            self.responses[uid] = {
                "type": "submit_job_response",
                "job_id": msg["job_id"],
                "job_submitted": submitted,
            }