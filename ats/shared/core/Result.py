from __future__ import annotations

from typing import TypeVar, NamedTuple, Any, Optional, Generic

T = TypeVar('T')


class ResultError(NamedTuple):
    code: str
    message: str
    meta: Any = None


class Result(Generic[T]):

    def __init__(self, isSuccess: bool, error: Optional[ResultError] = None, value: Optional[T] = None):
        if isSuccess and error:
            raise Exception("InvalidOperation: A result cannot be successful and contain an error")

        if not isSuccess and not error:
            raise Exception("InvalidOperation: A failing result needs to contain an error message")

        self.isSuccess: bool = isSuccess
        self.isFailure: bool = not isSuccess
        self.error: ResultError = error
        self._value: T = value

    def getValue(self) -> T:
        if not self.isSuccess:
            raise Exception(f'${self.error.code}: ${self.error.message}')
        return self._value

    @classmethod
    def ok(cls, value: Optional[T]) -> Result[T]:
        return Result(True, None, value)

    @classmethod
    def fail(cls, error: ResultError) -> 'Result'[T]:
        return Result(False, error)
