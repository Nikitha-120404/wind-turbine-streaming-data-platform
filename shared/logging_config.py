import logging
import sys
import structlog


def setup_logging(service_name: str | None = None, json_logs: bool = True):
    timestamper = structlog.processors.TimeStamper(fmt="iso")

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        timestamper,
        structlog.stdlib.add_logger_name,
        structlog.processors.dict_tracebacks,
    ]

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )

    if json_logs:
        processors = shared_processors + [structlog.processors.JSONRenderer()]
    else:
        processors = shared_processors + [structlog.dev.ConsoleRenderer()]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    if service_name:
        return structlog.get_logger(service_name)
    return structlog.get_logger()


def get_logger(name: str):
    return setup_logging(service_name=name, json_logs=True)