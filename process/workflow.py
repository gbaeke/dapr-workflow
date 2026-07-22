"""Dapr workflow: retrieve document -> convert to markdown -> structured extraction -> write results."""

import base64
import json
import logging
import os
import pathlib
import tempfile
from datetime import timedelta

import openai
from dapr.clients import DaprClient
from dotenv import load_dotenv
from dapr.ext.workflow import (
    DaprWorkflowContext,
    RetryPolicy,
    WorkflowActivityContext,
    WorkflowRuntime,
    when_all,
)
from markitdown import MarkItDown

load_dotenv(pathlib.Path(__file__).parent / ".env")

logger = logging.getLogger("process.workflow")

BINDING_NAME = "blobstore"

# token budget per extraction call; markdown beyond this is split into chunks.
# conservative estimate: verse/prose tokenizes at ~2.5-2.8 chars per token.
EXTRACT_MAX_TOKENS = int(os.getenv("EXTRACT_MAX_TOKENS", "200000"))
CHARS_PER_TOKEN = 2.5

wfr = WorkflowRuntime()

retry_policy = RetryPolicy(
    first_retry_interval=timedelta(seconds=2),
    max_number_of_attempts=3,
    backoff_coefficient=2,
    max_retry_interval=timedelta(seconds=30),
    retry_timeout=timedelta(minutes=5),
)


@wfr.workflow(name="doc_processing_wf")
def doc_processing_wf(ctx: DaprWorkflowContext, wf_input: dict):
    """Activities exchange BLOB REFERENCES, never document content: workflow state
    (and every gRPC work item built from it) stays small regardless of document size."""
    job_id = wf_input["job_id"]
    if not ctx.is_replaying:
        logger.info("job %s: workflow started for %s", job_id, wf_input.get("file_name"))

    meta = yield ctx.call_activity(
        retrieve_document,
        input={"doc_blob": wf_input["doc_blob"]},
        retry_policy=retry_policy,
    )

    markdown_blob = yield ctx.call_activity(
        convert_to_markdown,
        input={
            "job_id": job_id,
            "doc_blob": wf_input["doc_blob"],
            "file_name": wf_input.get("file_name") or wf_input["doc_blob"],
        },
        retry_policy=retry_policy,
    )

    chunk_blobs = yield ctx.call_activity(
        plan_chunks,
        input={"job_id": job_id, "markdown_blob": markdown_blob},
        retry_policy=retry_policy,
    )

    if len(chunk_blobs) == 1:
        extraction = yield ctx.call_activity(
            extract_structured,
            input={"markdown_blob": chunk_blobs[0], "schema": wf_input["schema"]},
            retry_policy=retry_policy,
        )
    else:
        # fan out: one extraction per chunk, all running in parallel
        if not ctx.is_replaying:
            logger.info("job %s: fanning out extraction over %d chunks", job_id, len(chunk_blobs))
        tasks = [
            ctx.call_activity(
                extract_structured,
                input={"markdown_blob": chunk, "schema": wf_input["schema"]},
                retry_policy=retry_policy,
            )
            for chunk in chunk_blobs
        ]
        partials = yield when_all(tasks)
        extraction = yield ctx.call_activity(
            merge_extractions,
            input={"partials": partials, "schema": wf_input["schema"]},
            retry_policy=retry_policy,
        )

    blobs = yield ctx.call_activity(
        write_results,
        input={"job_id": job_id, "extraction": extraction},
        retry_policy=retry_policy,
    )

    return {
        "job_id": job_id,
        "doc_size": meta["size"],
        "chunks": len(chunk_blobs),
        "markdown_blob": markdown_blob,
        **blobs,
    }


def _get_blob(blob_name: str) -> bytes:
    with DaprClient() as client:
        resp = client.invoke_binding(
            binding_name=BINDING_NAME,
            operation="get",
            binding_metadata={"blobName": blob_name},
        )
    if not resp.data:
        raise ValueError(f"blob {blob_name} is empty or missing")
    return resp.data


def _create_blob(blob_name: str, data: bytes, content_type: str) -> None:
    with DaprClient() as client:
        client.invoke_binding(
            binding_name=BINDING_NAME,
            operation="create",
            data=base64.b64encode(data),
            binding_metadata={"blobName": blob_name, "contentType": content_type},
        )


@wfr.activity(name="retrieve_document")
def retrieve_document(ctx: WorkflowActivityContext, act_input: dict) -> dict:
    """Verify the document is retrievable; return only its metadata, never the bytes."""
    data = _get_blob(act_input["doc_blob"])
    logger.info("retrieved %s (%d bytes)", act_input["doc_blob"], len(data))
    return {"size": len(data)}


@wfr.activity(name="convert_to_markdown")
def convert_to_markdown(ctx: WorkflowActivityContext, act_input: dict) -> str:
    """Convert the document to markdown locally with MarkItDown; write the result
    straight back to blob storage and return only the blob name."""
    content = _get_blob(act_input["doc_blob"])
    suffix = pathlib.Path(act_input["file_name"]).suffix or ".bin"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    try:
        result = MarkItDown().convert(tmp_path)
    finally:
        os.unlink(tmp_path)
    markdown = result.text_content
    if not markdown or not markdown.strip():
        raise ValueError("document conversion produced no text")

    markdown_blob = f"{act_input['job_id']}/converted.md"
    _create_blob(markdown_blob, markdown.encode(), "text/markdown")
    logger.info("converted %s to markdown (%d chars) -> %s", act_input["file_name"], len(markdown), markdown_blob)
    return markdown_blob


def _call_openai(system: str, user: str, schema: dict) -> dict:
    """Structured-outputs call with strict-mode fallback and refusal handling."""
    client = openai.OpenAI()
    model = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    response_format = {
        "type": "json_schema",
        "json_schema": {"name": "extraction", "schema": schema, "strict": True},
    }
    try:
        resp = client.chat.completions.create(model=model, messages=messages, response_format=response_format)
    except openai.BadRequestError as e:
        if "context_length" in str(e) or "maximum context" in str(e):
            raise  # no point retrying non-strict; chunking should prevent this
        # user schemas often don't satisfy strict-mode rules (additionalProperties: false,
        # every property required) -- retry without strict validation
        logger.warning("strict schema rejected (%s), retrying non-strict", e)
        response_format["json_schema"]["strict"] = False
        resp = client.chat.completions.create(model=model, messages=messages, response_format=response_format)

    message = resp.choices[0].message
    if message.refusal:
        raise ValueError(f"model refused extraction: {message.refusal}")
    if not message.content:
        raise ValueError("model returned no content")
    return json.loads(message.content)


@wfr.activity(name="plan_chunks")
def plan_chunks(ctx: WorkflowActivityContext, act_input: dict) -> list:
    """Split the markdown into chunks that each fit the extraction token budget.
    Returns a list of blob names -- just [markdown_blob] if no split is needed."""
    markdown = _get_blob(act_input["markdown_blob"]).decode()
    budget_chars = int(EXTRACT_MAX_TOKENS * CHARS_PER_TOKEN)
    if len(markdown) <= budget_chars:
        return [act_input["markdown_blob"]]

    n_chunks = -(-len(markdown) // budget_chars)  # ceil
    target = -(-len(markdown) // n_chunks)
    chunk_blobs = []
    pos = 0
    for i in range(n_chunks):
        end = min(pos + target, len(markdown))
        # cut on a paragraph boundary where possible
        if end < len(markdown):
            nl = markdown.rfind("\n\n", pos + target // 2, end)
            if nl != -1:
                end = nl
        chunk = markdown[pos:end]
        blob_name = f"{act_input['job_id']}/chunks/chunk-{i:03d}.md"
        _create_blob(blob_name, chunk.encode(), "text/markdown")
        chunk_blobs.append(blob_name)
        pos = end
    logger.info("split %d chars into %d chunks", len(markdown), len(chunk_blobs))
    return chunk_blobs


@wfr.activity(name="extract_structured")
def extract_structured(ctx: WorkflowActivityContext, act_input: dict) -> dict:
    """Use OpenAI structured outputs to extract content according to the user-supplied JSON schema."""
    markdown = _get_blob(act_input["markdown_blob"]).decode()
    return _call_openai(
        "Extract information from the user's document according to the provided JSON schema. "
        "The document may be one segment of a larger work: fill in what this segment supports "
        "and leave fields you cannot ground in the text empty.",
        markdown,
        act_input["schema"],
    )


@wfr.activity(name="merge_extractions")
def merge_extractions(ctx: WorkflowActivityContext, act_input: dict) -> dict:
    """Combine partial extractions from consecutive document segments into one result."""
    partials = act_input["partials"]
    user = "\n\n".join(
        f"### Extraction from segment {i + 1} of {len(partials)}\n{json.dumps(p, indent=2)}"
        for i, p in enumerate(partials)
    )
    return _call_openai(
        "The following JSON objects are partial extractions from consecutive segments of ONE document, "
        "all produced with the same JSON schema. Merge them into a single, complete, deduplicated "
        "extraction conforming to that schema. Prefer information from earlier segments for metadata; "
        "union list fields, keeping order of appearance in the document.",
        user,
        act_input["schema"],
    )


@wfr.activity(name="write_results")
def write_results(ctx: WorkflowActivityContext, act_input: dict) -> dict:
    """Write the extraction result back to blob storage (the markdown was already
    written by convert_to_markdown)."""
    job_id = act_input["job_id"]
    result_blob = f"{job_id}/extracted.json"
    _create_blob(result_blob, json.dumps(act_input["extraction"], indent=2).encode(), "application/json")
    logger.info("job %s: wrote %s", job_id, result_blob)
    return {"result_blob": result_blob}
