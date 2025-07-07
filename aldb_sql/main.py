import uvicorn

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from webhook_logger import WebhookMessager
from api import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    webhook_messager = WebhookMessager(message_target="feishu", machine_name="")
    webhook_messager.post_data(
        msg="Sql service is starting...",
        error_type=None,
        at_user=None,
        is_success=False,
        log_mode=False,
    )
    yield
    webhook_messager.post_data(
        msg="Sql service is shutting down...",
        error_type=3,
        at_user="xingjian",
        is_success=False,
        log_mode=False,
    )

app = FastAPI(
    title="Sql SERVICE API",
    description="REST API for Sql service",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/")
async def test():
    return {
        "service": "Sql SERVICE API",
        "version": "1.0.0",
        "status": "running",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=20060)