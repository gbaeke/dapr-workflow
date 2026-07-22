import json
import logging
from contextlib import asynccontextmanager

from dapr.clients import DaprClient
from dapr.ext.fastapi import DaprApp
from dapr.ext.workflow import DaprWorkflowClient
from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from workflow import BINDING_NAME, doc_processing_wf, wfr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("process")

PUBSUB_NAME = "pubsub"
TOPIC_NAME = "process-topic"


@asynccontextmanager
async def lifespan(app: FastAPI):
    wfr.start()
    logger.info("workflow runtime started")
    yield
    wfr.shutdown()


app = FastAPI(title="process", lifespan=lifespan)

# local dev only: the test UI runs from the VS Code browser / file://
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

dapr_app = DaprApp(app)


@dapr_app.subscribe(pubsub=PUBSUB_NAME, topic=TOPIC_NAME)
def on_process_request(event_data=Body()):
    payload = event_data.get("data") if isinstance(event_data, dict) else None
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not payload or "job_id" not in payload:
        # malformed message: ack it so it is not redelivered forever
        logger.error("dropping malformed message: %s", event_data)
        return {"success": True}

    job_id = payload["job_id"]
    wf_client = DaprWorkflowClient()
    try:
        wf_client.schedule_new_workflow(
            workflow=doc_processing_wf, input=payload, instance_id=job_id
        )
        logger.info("job %s: workflow scheduled", job_id)
    except Exception as e:
        # pub/sub is at-least-once; a redelivery for an already-scheduled job is fine
        if "already exists" in str(e).lower():
            logger.info("job %s: workflow already scheduled, acking duplicate", job_id)
        else:
            raise
    return {"success": True}


@app.get("/status/{job_id}")
def status(job_id: str):
    wf_client = DaprWorkflowClient()
    state = wf_client.get_workflow_state(instance_id=job_id, fetch_payloads=True)
    if state is None:
        raise HTTPException(status_code=404, detail=f"no workflow for job {job_id}")
    output = None
    if state.serialized_output:
        output = json.loads(state.serialized_output)
    return {
        "job_id": job_id,
        "status": state.runtime_status.name,
        "created_at": state.created_at.isoformat() if state.created_at else None,
        "last_updated_at": state.last_updated_at.isoformat() if state.last_updated_at else None,
        "output": output,
        "failure": state.failure_details.message if state.failure_details else None,
    }


@app.get("/jobs")
def jobs():
    """List all jobs found in the documents container, with blob inventory and workflow status."""
    with DaprClient() as client:
        resp = client.invoke_binding(
            binding_name=BINDING_NAME,
            operation="list",
            data=json.dumps({"maxResults": 5000}),
        )
    blobs = json.loads(resp.data) or []

    jobs_map: dict[str, dict] = {}
    for blob in blobs:
        name = blob.get("Name", "")
        if "/" not in name:
            continue
        job_id, _, file_part = name.partition("/")
        props = blob.get("Properties") or {}
        job = jobs_map.setdefault(job_id, {"job_id": job_id, "files": {}, "last_modified": None})
        job["files"][file_part] = {
            "blob": name,
            "size": props.get("ContentLength"),
            "last_modified": props.get("LastModified"),
        }
        lm = props.get("LastModified")
        if lm and (job["last_modified"] is None or lm > job["last_modified"]):
            job["last_modified"] = lm

    wf_client = DaprWorkflowClient()
    for job in jobs_map.values():
        try:
            state = wf_client.get_workflow_state(instance_id=job["job_id"])
            job["status"] = state.runtime_status.name if state else "UNKNOWN"
        except Exception:
            job["status"] = "UNKNOWN"
        job["has_result"] = "extracted.json" in job["files"]

    result = sorted(jobs_map.values(), key=lambda j: j["last_modified"] or "", reverse=True)
    return {"count": len(result), "jobs": result}


def _get_blob(blob_name: str) -> bytes:
    with DaprClient() as client:
        resp = client.invoke_binding(
            binding_name=BINDING_NAME,
            operation="get",
            binding_metadata={"blobName": blob_name},
        )
    if not resp.data:
        raise HTTPException(status_code=404, detail=f"blob {blob_name} not found or empty")
    return resp.data


@app.get("/result/{job_id}")
def result(job_id: str):
    return json.loads(_get_blob(f"{job_id}/extracted.json"))


@app.get("/markdown/{job_id}", response_class=PlainTextResponse)
def markdown(job_id: str):
    return _get_blob(f"{job_id}/converted.md").decode()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
