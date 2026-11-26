"""One logging format for every script: timestamp level logger message (key=value pairs)."""
import logging


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    # these libraries are chatty at INFO
    for noisy in ("urllib3", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
