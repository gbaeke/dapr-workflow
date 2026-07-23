import base64
import json
import logging
import pathlib
import uuid

from dapr.clients import DaprClient
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("upload")

BINDING_NAME = "blobstore"
PUBSUB_NAME = "pubsub"
# pipeline -> (topic, service port for status URLs)
PIPELINES = {
    "extract": ("process-topic", 8101),
    "analyze": ("analyze-topic", 8102),
}

app = FastAPI(title="upload")

# local dev only: the test UI runs from the VS Code browser / file://
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/upload")
async def upload(
    file: UploadFile = File(...),
    schema: str | None = Form(None),
    pipeline: str = Form("extract"),
):
    """Accept a document, store it in blob storage and publish a request for the
    chosen pipeline: 'extract' (structured extraction against a JSON schema, the
    default) or 'analyze' (deep-agent document analysis, no schema needed)."""
    if pipeline not in PIPELINES:
        raise HTTPException(status_code=400, detail=f"unknown pipeline '{pipeline}', use one of {sorted(PIPELINES)}")

    schema_obj = None
    if pipeline == "extract":
        if not schema:
            raise HTTPException(status_code=400, detail="the extract pipeline requires a schema")
        try:
            schema_obj = json.loads(schema)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"schema is not valid JSON: {e}")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="file is empty")

    job_id = uuid.uuid4().hex
    ext = pathlib.Path(file.filename or "document").suffix or ".bin"
    doc_blob = f"{job_id}/original{ext}"

    with DaprClient() as client:
        client.invoke_binding(
            binding_name=BINDING_NAME,
            operation="create",
            data=base64.b64encode(content),
            binding_metadata={"blobName": doc_blob},
        )

        topic_name, status_port = PIPELINES[pipeline]
        message = {
            "job_id": job_id,
            "doc_blob": doc_blob,
            "file_name": file.filename,
        }
        if schema_obj is not None:
            message["schema"] = schema_obj
        client.publish_event(
            pubsub_name=PUBSUB_NAME,
            topic_name=topic_name,
            data=json.dumps(message),
            data_content_type="application/json",
        )

    logger.info("job %s: stored %s (%d bytes) and published to %s", job_id, doc_blob, len(content), topic_name)
    return {
        "job_id": job_id,
        "doc_blob": doc_blob,
        "pipeline": pipeline,
        "status_url": f"http://localhost:{status_port}/status/{job_id}",
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
