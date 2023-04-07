from typing import NamedTuple, Optional, Any, List, Dict

from ats.shared.core.Result import ResultError
from ats.shared.domain.ErrorCodes import ErrorCodes


class GuardResult(NamedTuple):
    succeeded: bool
    error: Optional[ResultError] = None


class GuardArgument(NamedTuple):
    argument: Any
    argumentName: str


class Guard:

    @classmethod
    def againstNull(cls, argument: Any, argumentName: str) -> GuardResult:
        if argument is None:
            return GuardResult(succeeded=False, error=ResultError(
                code=ErrorCodes.VALIDATION_FAILURE,
                message=f'`${argumentName} is null`'
            ))

        return GuardResult(succeeded=True)

    @classmethod
    def againstNullBulk(cls, args: List[GuardArgument]) -> GuardResult:
        for arg in args:
            result = Guard.againstNull(argument=arg.argument, argumentName=arg.argumentName)
            if not result.succeeded:
                return result

        return GuardResult(succeeded=True)

    @classmethod
    def againstNull(cls, argument: Any, argumentName: str) -> GuardResult:
        if argument is None:
            return GuardResult(succeeded=False, error=ResultError(
                code=ErrorCodes.VALIDATION_FAILURE.value,
                message=f'${argumentName} is null'
            ))

        return GuardResult(succeeded=True)

    @classmethod
    def againstNullKeys(cls, keys: List[str], dictionary: Dict[str, Any]) -> GuardResult:
        for key in keys:
            if key not in dictionary:
                return GuardResult(succeeded=False, error=ResultError(
                    code=ErrorCodes.VALIDATION_FAILURE.value,
                    message=f'${key} is null',
                    meta=dictionary
                ))

        return GuardResult(succeeded=True)

    @classmethod
    def setNoneIfKeyNotExists(cls, keys: List[str], dictionary: Dict[str, Any]) -> GuardResult:
        for key in keys:
            if key not in dictionary:
                dictionary[key] = None
        return GuardResult(succeeded=True)
