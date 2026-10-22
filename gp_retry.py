"""
gp_retry: перезапуск обработки по известным ошибкам Greenplum.

- Если текст ошибки совпал с одним из 28 известных сообщений -> обработка
  перезапускается (до max_attempts раз, с экспоненциальной паузой).
- Любая другая ошибка -> сразу падает (исключение пробрасывается как есть).
- Если попытки закончились -> RetryExhaustedError (из последней ошибки).
"""
from __future__ import annotations

import functools
import logging
import random
import re
import time
from typing import Callable, Optional, TypeVar

log = logging.getLogger(__name__)
T = TypeVar("T")

# ВАЖНО: замените на реальные 28 сообщений. Сравнение: без учёта регистра,
# по вхождению подстроки (если нужен regex - см. is_regex ниже).
RETRYABLE_PATTERNS: tuple[str, ...] = (
    "server closed the connection unexpectedly",
    "connection to server was lost",
    "could not connect to server",
    "could not receive data from server",
    "could not send data to server",
    "ssl syscall error: eof detected",
    "connection timed out",
    "connection reset by peer",
    "terminating connection due to administrator command",
    "the database system is starting up",
    "the database system is shutting down",
    "the database system is in recovery mode",
    "too many clients already",
    "remaining connection slots are reserved",
    "deadlock detected",
    "could not serialize access due to concurrent update",
    "tuple concurrently updated",
    "canceling statement due to lock timeout",
    "canceling statement due to statement timeout",
    "canceling query because of high vmem usage",
    "out of memory",
    "interconnect error",
    "interconnect encountered a network error",
    "failed to acquire resource from resource manager",
    "failed to acquire resources on one or more segments",
    "gang was lost due to cluster reconfiguration",
    "error on receive from seg",
    "transaction aborted due to segment failure",
)

# Если часть сообщений - это regex, добавляйте сюда (компилируются один раз).
_COMPILED = tuple(re.compile(re.escape(p), re.IGNORECASE) for p in RETRYABLE_PATTERNS)


class RetryExhaustedError(RuntimeError):
    """Все попытки перезапуска исчерпаны."""


def is_retryable(exc: BaseException) -> bool:
    """True, если сообщение ошибки (или её причины) входит в список перезапускаемых."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        text = str(exc)
        if any(rx.search(text) for rx in _COMPILED):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def run_with_retry(
    func: Callable[[], T],
    *,
    max_attempts: int = 5,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: float = 0.2,
    on_retry: Optional[Callable[[int, BaseException], None]] = None,
) -> T:
    """Выполняет func(); перезапускает при известных ошибках, иначе падает.

    Пауза перед n-й повторной попыткой:
        min(base_delay * backoff_factor ** (n - 1), max_delay)
    плюс/минус случайный разброс jitter (доля от задержки, 0.2 = +-20%).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return func()
        except Exception as exc:
            if not is_retryable(exc):
                log.error("Неперезапускаемая ошибка: %s", exc)
                raise  # остальные ошибки - падаем сразу
            if attempt >= max_attempts:
                raise RetryExhaustedError(
                    f"Исчерпано {max_attempts} попыток: {exc}"
                ) from exc
            delay = min(base_delay * backoff_factor ** (attempt - 1), max_delay)
            if jitter:
                delay *= random.uniform(1 - jitter, 1 + jitter)
            log.warning(
                "Ошибка Greenplum (попытка %d/%d): %s. Повтор через %.1f c",
                attempt, max_attempts, exc, delay,
            )
            if on_retry:
                on_retry(attempt, exc)
            time.sleep(delay)


def retry_on_greenplum_errors(**kwargs):
    """Декоратор-обёртка над run_with_retry."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kw):
            return run_with_retry(lambda: fn(*args, **kw), **kwargs)
        return wrapper
    return decorator


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    calls = {"n": 0}

    @retry_on_greenplum_errors(max_attempts=3, base_delay=0.1)
    def process():
        calls["n"] += 1
        if calls["n"] < 3:
            raise Exception("ERROR: deadlock detected")  # перезапустится
        return "ok"

    print(process())  # -> ok после 2 перезапусков
