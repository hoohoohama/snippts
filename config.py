import os


class Settings:
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "4318"))

    INGEST_KEY: str = os.getenv("INGEST_KEY", "change-me")

    DYNAMODB_TABLE_NAME: str = os.getenv("DYNAMODB_TABLE_NAME", "MLflowTraceTable")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "mlflow-trace-json")

    MAX_BODY_BYTES: int = int(os.getenv("MAX_BODY_BYTES", str(10 * 1024 * 1024)))
    MAX_EVENTS_PER_SPAN: int = int(os.getenv("MAX_EVENTS_PER_SPAN", "100"))
    MAX_LINKS_PER_SPAN: int = int(os.getenv("MAX_LINKS_PER_SPAN", "20"))
    MAX_ATTRS_PER_SPAN: int = int(os.getenv("MAX_ATTRS_PER_SPAN", "100"))
    MAX_RESOURCE_ATTRS_PER_SPAN: int = int(os.getenv("MAX_RESOURCE_ATTRS_PER_SPAN", "50"))

    TRACE_JSON_FORMAT: str = os.getenv("TRACE_JSON_FORMAT", "mlflow_3_x")


settings = Settings()