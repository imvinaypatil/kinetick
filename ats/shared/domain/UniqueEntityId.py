import uuid
from typing import Union, Optional

from ats.shared.domain.Indentifier import Identifier


class UniqueEntityId(Identifier[Union[str, int]]):
    def __init__(self, id: Optional[Union[str, int]] = None):
        value = id if id is not None else uuid.uuid4()
        super().__init__(value)


if __name__ == '__main__':
    id = UniqueEntityId()
    print(id.toString())
    id = UniqueEntityId("id")
    print(id.toString())
    id = UniqueEntityId(123456789012345678)
    print(id.toString())
