"""LangChain deep agent for document analysis, hosted as a durable Dapr Workflow.

The agent itself is a stock deepagents agent; DaprWorkflowDeepAgentRunner wraps its
compiled graph so every LLM turn and tool call executes as a Dapr Workflow activity
(retried, checkpointed, resumable after a crash). No LangGraph checkpointer is used --
durability comes from the workflow engine.
"""

import base64
import logging
import os
import pathlib
import tempfile

from dapr.clients import DaprClient
from deepagents import create_deep_agent
from diagrid.agent.deepagents import DaprWorkflowDeepAgentRunner
from dotenv import load_dotenv
from markitdown import MarkItDown

load_dotenv(pathlib.Path(__file__).parent / ".env")

logger = logging.getLogger("analyze.agent")

BINDING_NAME = "blobstore"

# max characters returned per read_document call; tool results travel through
# workflow activity payloads (4 MiB sidecar limit) and the agent's context window
READ_MAX_CHARS = int(os.getenv("READ_MAX_CHARS", "80000"))


def _get_blob(blob_name: str) -> bytes | None:
    """Fetch a blob; returns None if it does not exist (the binding raises on missing blobs)."""
    try:
        with DaprClient() as client:
            resp = client.invoke_binding(
                binding_name=BINDING_NAME,
                operation="get",
                binding_metadata={"blobName": blob_name},
            )
    except Exception as e:
        if "blob not found" in str(e).lower() or "blobnotfound" in str(e).lower():
            return None
        raise
    return resp.data or None


def _create_blob(blob_name: str, data: bytes, content_type: str) -> None:
    with DaprClient() as client:
        client.invoke_binding(
            binding_name=BINDING_NAME,
            operation="create",
            data=base64.b64encode(data),
            binding_metadata={"blobName": blob_name, "contentType": content_type},
        )


def read_document(doc_blob: str, offset: int = 0) -> str:
    """Read the uploaded document as markdown text.

    Args:
        doc_blob: blob name of the original document, e.g. "<job_id>/original.pdf".
        offset: character position to start reading from (use for documents longer
            than one read; the response header states the total length).

    Returns the markdown content from `offset`, at most 80000 characters per call,
    prefixed with a header line stating the total length and the range returned.
    """
    job_id = doc_blob.partition("/")[0]
    converted_blob = f"{job_id}/converted.md"

    cached = _get_blob(converted_blob)
    if cached is not None:
        markdown = cached.decode()
    else:
        content = _get_blob(doc_blob)
        if content is None:
            return f"ERROR: blob {doc_blob} is empty or missing"
        suffix = pathlib.Path(doc_blob).suffix or ".bin"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        try:
            markdown = MarkItDown().convert(tmp_path).text_content
        finally:
            os.unlink(tmp_path)
        if not markdown or not markdown.strip():
            return f"ERROR: document {doc_blob} converted to empty text"
        _create_blob(converted_blob, markdown.encode(), "text/markdown")
        logger.info("converted %s to markdown (%d chars)", doc_blob, len(markdown))

    chunk = markdown[offset : offset + READ_MAX_CHARS]
    end = offset + len(chunk)
    header = (
        f"[document: {len(markdown)} chars total, returning chars {offset}-{end}"
        f"{'' if end >= len(markdown) else f'; call again with offset={end} for more'}]\n\n"
    )
    return header + chunk


def save_analysis(job_id: str, analysis_markdown: str) -> str:
    """Save the final analysis report for a job.

    Args:
        job_id: the job identifier the analysis belongs to.
        analysis_markdown: the complete analysis report as markdown.

    Returns the blob name the report was written to.
    """
    blob_name = f"{job_id}/analysis.md"
    _create_blob(blob_name, analysis_markdown.encode(), "text/markdown")
    logger.info("job %s: wrote %s (%d chars)", job_id, blob_name, len(analysis_markdown))
    return blob_name


SYSTEM_PROMPT = """You are a document analyst. Each request names a job_id and a doc_blob.

Work through these steps, tracking them with your todo list:
1. Read the document with read_document (page through it with offset if it is longer
   than one read).
2. Analyze it and write a markdown report with these sections:
   - Summary: 2-5 sentences on what the document is and says.
   - Document type: invoice, contract, report, article, ...
   - Key entities: people, organizations, dates, amounts, identifiers found.
   - Notable findings: anything unusual, important, inconsistent, or worth flagging.
3. Save the report with save_analysis(job_id, ...).

Finish by replying with a one-paragraph summary of the analysis. Ground every statement
in the document text; never invent details."""

_model = os.getenv("OPENAI_MODEL", "gpt-5-mini")

agent = create_deep_agent(
    model=f"openai:{_model}",
    tools=[read_document, save_analysis],
    system_prompt=SYSTEM_PROMPT,
    name="doc-analysis-agent",
)

runner = DaprWorkflowDeepAgentRunner(
    agent=agent,
    name="doc-analysis-agent",
    role="Document Analyst",
    goal="Read an uploaded document and produce a structured analysis report",
    max_steps=int(os.getenv("AGENT_MAX_STEPS", "25")),
)
