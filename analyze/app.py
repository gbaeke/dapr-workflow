import json
import logging
from contextlib import asynccontextmanager

from dapr.ext.fastapi import DaprApp
from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from agent import _get_blob, runner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("analyze")

PUBSUB_NAME = "pubsub"
TOPIC_NAME = "analyze-topic"


@asynccontextmanager
async def lifespan(app: FastAPI):
    runner.start()
    logger.info("agent workflow runtime started")
    yield
    runner.shutdown()


app = FastAPI(title="analyze", lifespan=lifespan)

# local dev only: the test UI runs from the VS Code browser / file://
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

dapr_app = DaprApp(app)


@dapr_app.subscribe(pubsub=PUBSUB_NAME, topic=TOPIC_NAME)
async def on_analyze_request(event_data=Body()):
    payload = event_data.get("data") if isinstance(event_data, dict) else None
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not payload or "job_id" not in payload:
        # malformed message: ack it so it is not redelivered forever
        logger.error("dropping malformed message: %s", event_data)
        return {"success": True}

    job_id = payload["job_id"]
    prompt = (
        f"Analyze the uploaded document '{payload.get('file_name') or payload['doc_blob']}'.\n"
        f"job_id: {job_id}\n"
        f"doc_blob: {payload['doc_blob']}"
    )
    try:
        # run_async schedules the workflow durably, then only polls for status;
        # stop consuming after the first event -- the agent keeps running in the
        # workflow engine and /status/{job_id} tracks it.
        async for event in runner.run_async(
            input={"messages": [{"role": "user", "content": prompt}]},
            thread_id=job_id,
            workflow_id=job_id,
        ):
            if event["type"] == "workflow_started":
                logger.info("job %s: agent workflow scheduled", job_id)
                break
    except Exception as e:
        # pub/sub is at-least-once; a redelivery for an already-scheduled job is fine
        if "already exists" in str(e).lower():
            logger.info("job %s: workflow already scheduled, acking duplicate", job_id)
        else:
            raise
    return {"success": True}


@app.get("/status/{job_id}")
def status(job_id: str):
    state = runner.get_workflow_status(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"no workflow for job {job_id}")
    return {"job_id": job_id, **state}


@app.get("/analysis/{job_id}", response_class=PlainTextResponse)
def analysis(job_id: str):
    data = _get_blob(f"{job_id}/analysis.md")
    if data is None:
        raise HTTPException(status_code=404, detail=f"no analysis for job {job_id} (yet)")
    return data.decode()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
