from typing import Protocol

import os
import logging

from redis import Redis
from redis.exceptions import ConnectionError

from . import settings

from .urls import get_redis_url

logger = logging.getLogger(__name__)


class Checkpoint(Protocol):
    def set_value(self, value: int | None) -> None: ...

    def get_value(self) -> int | None: ...

    def teardown(self) -> None: ...

    def validate(self) -> None: ...


class CheckPointFile:
    def __init__(self, name: str) -> None:
        self._checkpoint: int | None = None
        self._checkpoint_file: str = os.path.join(settings.CHECKPOINT_PATH, f".{name}")

    def validate(self) -> None:
        if not os.path.exists(settings.CHECKPOINT_PATH):
            raise RuntimeError(
                f"Ensure the checkpoint directory exists "
                f'"{settings.CHECKPOINT_PATH}" and is readable.'
            )

        if not os.access(settings.CHECKPOINT_PATH, os.W_OK | os.R_OK):
            raise RuntimeError(
                f'Ensure the checkpoint directory "{settings.CHECKPOINT_PATH}"'
                f" is read/writable"
            )

    def set_value(self, value: int | None = None) -> None:
        if value is None:
            raise ValueError("Cannot assign a None value to checkpoint")
        with open(self._checkpoint_file, "w+") as fp:
            fp.write(f"{value}\n")
        self._checkpoint = value

    def get_value(self) -> int | None:
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, "r") as fp:
                self._checkpoint = int(fp.read().split()[0])
        return self._checkpoint

    def teardown(self) -> None:
        try:
            os.unlink(self._checkpoint_file)
        except (OSError, FileNotFoundError):
            logger.warning(f"Checkpoint file not found: {self._checkpoint_file}")


class CheckPointRedis:
    def __init__(self, name: str, namespace: str = "checkpoint") -> None:
        url: str = get_redis_url()
        self._key = f"{namespace}:{name}"
        self._redis = Redis.from_url(
            url=url, socket_timeout=settings.REDIS_SOCKET_TIMEOUT, decode_responses=True
        )

    def validate(self) -> None:
        try:
            self._redis.ping()
        except ConnectionError:
            raise RuntimeError("Ensure redis is reachable to be used for checkpoints.")

    def set_value(self, value: int | None = None) -> None:
        if value is None:
            raise ValueError("Cannot assign a None value to checkpoint")
        self._redis.set(self._key, value)

    def get_value(self) -> int | None:
        value = self._redis.get(self._key)
        return int(value) if value is not None else None

    def teardown(self) -> None:
        self._redis.delete(self._key)


def get_checkpoint(name: str) -> Checkpoint:
    match settings.CHECKPOINT_IMPL:
        case "CHECKPOINT_REDIS":
            return CheckPointRedis(name=name)
        case "CHECKPOINT_FILE":
            return CheckPointFile(name=name)
        case _:
            raise RuntimeError(
                f"Unknown checkpoint implementation: {settings.CHECKPOINT_IMPL}"
            )
