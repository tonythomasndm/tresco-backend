from __future__ import annotations

import traceback

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes.score import router as score_router
from app.core.config import get_settings
from app.utils.helpers import build_error_payload


settings = get_settings()
app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(HTTPException)
async def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, dict):
        error_code = detail.get("code") or "request_failed"
        message = detail.get("message") or "Request failed."
        details = detail.get("details")
    else:
        error_code = "request_failed"
        message = str(detail)
        details = None

    return JSONResponse(
        status_code=exc.status_code,
        content=build_error_payload(error_code, message, details),
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(
    _request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=build_error_payload("invalid_request", "Request validation failed.", exc.errors()),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    traceback.print_exception(type(exc), exc, exc.__traceback__)
    return JSONResponse(
        status_code=500,
        content=build_error_payload(
            "internal_server_error",
            "An unexpected error occurred while processing the request.",
        ),
    )


app.include_router(score_router)
