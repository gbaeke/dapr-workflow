# Dapr document extraction

Two Dapr services that extract structured content from documents using OpenAI structured outputs:

- **upload** (port 8100) — accepts a document + a JSON schema via `POST /upload`, stores the document in blob storage (Azurite locally) through the Dapr `blobstore` binding, and publishes a message on the `process-topic` topic.
- **process** (port 8101) — subscribes to `process-topic` and runs a **Dapr Workflow** (instance id = job id) with four retryable activities:
  1. `retrieve_document` — get the original document via the blob binding
  2. `convert_to_markdown` — local conversion with [MarkItDown](https://github.com/microsoft/markitdown) (pdf, docx, pptx, xlsx, html, ...)
  3. `extract_structured` — OpenAI structured outputs (`json_schema` response format) using the uploaded schema
  4. `write_results` — write `converted.md` + `extracted.json` back to blob storage

Blob layout per job: `documents/{job_id}/original.{ext}`, `documents/{job_id}/converted.md`, `documents/{job_id}/extracted.json`.

## Prerequisites

- `dapr init` has been run (Redis is used for pub/sub and as the workflow actor state store)
- Docker (for Azurite), [uv](https://docs.astral.sh/uv/) (Python envs)
- A `process/.env` file with your OpenAI credentials:

  ```dotenv
  OPENAI_API_KEY=sk-...
  # OPENAI_MODEL=gpt-5-mini   # optional, this is the default
  ```

## Run

```bash
docker compose up -d          # start Azurite
dapr run -f dapr.yaml         # start both services + sidecars
```

## Try it

```bash
curl -F "file=@sample/invoice.html" -F "schema=<sample/schema.json" http://localhost:8100/upload
# -> {"job_id": "...", "doc_blob": ".../original.html", "status_url": "http://localhost:8101/status/..."}

curl http://localhost:8101/status/<job_id>
# -> {"status": "COMPLETED", "output": {"markdown_blob": ..., "result_blob": ...}, ...}
```

## Test UI

`ui/index.html` is a single-file stress-test UI: pick a document, edit or load a schema (prefilled with the sample invoice schema), choose how many parallel uploads to fire (1–500), and watch jobs complete in the documents table (auto-refresh, view extracted JSON / converted markdown per job). Open it with the VS Code built-in browser or any static server — both services allow all CORS origins for local dev. The service base URLs are editable in the header (persisted in localStorage) in case VS Code server forwards the ports under different addresses.

Supporting endpoints on the process service: `GET /jobs` (blob inventory + workflow status per job), `GET /result/{job_id}`, `GET /markdown/{job_id}`.

Fetch a result through the Dapr binding (any sidecar works):

```bash
curl -s -X POST http://localhost:3500/v1.0/bindings/blobstore \
  -H 'Content-Type: application/json' \
  -d '{"operation": "get", "metadata": {"blobName": "<job_id>/extracted.json"}}'
```

## Notes

- **Schemas and strict mode**: the extraction first tries OpenAI strict structured outputs. Strict mode requires `additionalProperties: false` on every object and every property listed in `required` (see `sample/schema.json`). If the schema doesn't satisfy those rules, the service automatically falls back to non-strict `json_schema` mode.
- **Big documents**: if the converted markdown exceeds the extraction token budget (`EXTRACT_MAX_TOKENS`, default 200k), a `plan_chunks` activity splits it into chunk blobs, the workflow fans out one `extract_structured` per chunk in parallel (`when_all`), and a `merge_extractions` activity combines the partial JSONs into one schema-conforming result. The full Odyssey (305k tokens) processes as 2 chunks + merge in ~2.5 minutes.
- **Reliability**: every workflow step runs with a retry policy (3 attempts, exponential backoff). If the process service crashes mid-job, restarting `dapr run -f dapr.yaml` resumes the workflow from the last completed activity. Duplicate pub/sub deliveries are acked because the workflow instance id equals the job id.
- **Document size**: activities pass **blob references** between steps, never document content — `convert_to_markdown` writes `converted.md` to storage itself and returns only the blob name. Workflow state (and the gRPC work items built from it) stays tiny regardless of document size. (Earlier versions passed base64 content through workflow state; a 1.7 MB PDF pushed the replay history past the 4 MB gRPC message limit and wedged the worker — hence this design.)
- **Azurite**: the `blobstore` component points at `http://127.0.0.1:10000` with the well-known `devstoreaccount1` dev credentials; the `documents` container is created automatically.
