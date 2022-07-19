from ats.modules.position.actions.ABCPositionAction import ABCPositionAction
from ats.modules.position.aggregates.Position import Position, PositionStatusEnum, PositionDirectionEnum
from ats.modules.position.services.PositionService import PositionService
from ats.shared.types.enums import TransactionTypeEnum


class BuyAction(ABCPositionAction):

    def __init__(self, positionService: PositionService):
        self.positionService = positionService

    def execute(self, position: Position) -> None:
        self.positionService.openPosition(position, TransactionTypeEnum.BUY)

