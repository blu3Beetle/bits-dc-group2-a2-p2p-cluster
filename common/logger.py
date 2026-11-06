"""Per-node logging to the terminal and to logs/<node_id>.log."""

import logging
import os
import sys

from common.config import REPO_ROOT

_FORMAT = "%(asctime)s.%(msecs)03d [%(name)s] %(levelname)-7s %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(node_id, level=logging.INFO):
    """Return the logger for a node. Safe to call more than once."""
    logger = logging.getLogger(node_id)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False
    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    log_dir = os.environ.get("DC_LOG_DIR") or os.path.join(REPO_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, "{}.log".format(node_id))
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
