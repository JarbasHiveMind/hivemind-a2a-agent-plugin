"""Minimal FastAPI A2A server used by e2e tests and the README example.

Start manually:
    uvicorn tests.e2e.mock_a2a_server:app --port 9999

The server handles:
- GET  /.well-known/agent.json  — agent card
- POST /                        — tasks/send  (blocking)
- POST /                        — tasks/sendSubscribe  (SSE streaming)
"""
from __future__ import annotations

import json
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Mock A2A Server")

AGENT_CARD = {
    "name": "MockAgent",
    "description": "A mock A2A agent for testing",
    "url": "http://localhost:9999",
    "version": "1.0",
    "capabilities": {"streaming": True},
    "skills": [
        {
            "id": "echo",
            "name": "Echo",
            "description": "Echoes the user message",
            "tags": ["echo"],
            "examples": ["hello"],
        }
    ],
}


@app.get("/.well-known/agent.json")
async def agent_card():
    return JSONResponse(AGENT_CARD)


@app.post("/")
async def handle_rpc(request: Request):
    body = await request.json()
    method = body.get("method", "")
    rpc_id = body.get("id", str(uuid.uuid4()))
    params = body.get("params", {})

    # Extract user text
    message = params.get("message", {})
    parts = message.get("parts", [])
    user_text = next(
        (p["text"] for p in parts if p.get("type") == "text"), ""
    )

    if method == "tasks/send":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {
                "artifacts": [
                    {"parts": [{"type": "text", "text": f"Echo: {user_text}"}]}
                ]
            },
        })

    if method == "tasks/sendSubscribe":
        async def _stream():
            words = f"Echo: {user_text}".split()
            for word in words:
                event = {
                    "result": {
                        "delta": {"parts": [{"type": "text", "text": word + " "}]}
                    }
                }
                yield f"data: {json.dumps(event)}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(_stream(), media_type="text/event-stream")

    return JSONResponse(
        {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"},
        },
        status_code=200,
    )
