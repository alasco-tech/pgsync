import enum
import logging
import os
from typing import Protocol

from redis import Redis
from redis.exceptions import ConnectionError

from . import settings
from .urls import get_redis_url

logger = logging.getLogger(__name__)


@enum.unique
class CheckpointImpl(str, enum.Enum):
    FILE = "FILE"
    REDIS = "REDIS"


class Checkpoint(Protocol):
    def set_value(self, value: int | None) -> None: ...

    def get_value(self) -> int | None: ...

    def teardown(self) -> None: ...

    def validate(self) -> None: ...


class FileCheckpoint:
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
        with open(self._checkpoint_file, "w") as fp:
            fp.write(str(value))
        self._checkpoint = value

    def get_value(self) -> int | None:
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, "r") as fp:
                self._checkpoint = int(fp.read())
        return self._checkpoint

    def teardown(self) -> None:
        try:
            os.unlink(self._checkpoint_file)
        except (OSError, FileNotFoundError):
            logger.warning(f"Checkpoint file not found: {self._checkpoint_file}")


class RedisCheckpoint:
    def __init__(self, name: str) -> None:
        url: str = get_redis_url()
        namespace = settings.CHECKPOINT_REDIS_NAMESPACE
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
    if settings.CHECKPOINT_IMPL == CheckpointImpl.REDIS:
        return RedisCheckpoint(name=name)
    elif settings.CHECKPOINT_IMPL == CheckpointImpl.FILE:
        return FileCheckpoint(name=name)

    raise TypeError(f"Unknown checkpoint implementation: {settings.CHECKPOINT_IMPL}")
