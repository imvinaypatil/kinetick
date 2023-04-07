import enum


class ErrorCodes(enum.Enum):
    VALIDATION_FAILURE = 'VALIDATION_FAILURE'

    def __str__(self):
        return self.value
