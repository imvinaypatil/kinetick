import abc
import asyncio

from ats.modules.position.aggregates.Position import Position


class ABCPositionAction(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    @asyncio.Task
    def execute(self, position: Position) -> None:
        pass
