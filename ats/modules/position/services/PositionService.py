from ats.modules.broker.services.BrokerService import BrokerService
from ats.modules.position.aggregates.Position import Position, PositionStatusEnum, PositionDirectionEnum
from ats.shared.types.enums import TransactionTypeEnum


class PositionService:

    def __init__(self, brokerService: BrokerService):
        self.brokerService = brokerService

    def isPositionFilled(self, position: Position) -> bool:
        if position.getStatus() == PositionStatusEnum.FILLED:
            return True
        else:
            status = self.brokerService.orderStatus(position.getBrokerOrder())
            position.setStatus(status)
            if status is PositionStatusEnum.FILLED:
                return True
        return False

    def fetchPositionStatus(self, position: Position) -> PositionStatusEnum:
        if position.getStatus() is PositionStatusEnum.EXITED:
            return PositionStatusEnum.EXITED
        position.setStatus(self.brokerService.orderStatus(position.getBrokerOrder()))
        return position.getStatus()

    def openPosition(self, position: Position, txnType: TransactionTypeEnum) -> Position:
        if position.getStatus() is not PositionStatusEnum.CREATED:
            raise Exception('PositionService: Unable to open position as the state is not in CREATED')

        position.setDirection(PositionDirectionEnum.LONG
                              if txnType == TransactionTypeEnum.BUY
                              else PositionDirectionEnum.SHORT)
        if position.getBrokerOrder() is None:
            position.setBrokerOrder(self.brokerService.createOrderDraft(position))

        order = self.brokerService.placeOrder(position.rawBrokerOrder)

        position.setBrokerOrder(order)
        self.fetchPositionStatus(position)

        return position

    def closePosition(self, position: Position) -> Position:
        if position.getStatus() in [PositionStatusEnum.EXITED]:
            raise Exception('PositionService: Unable to close position. Position state {EXITED} violation')
        if position.getBrokerOrder() is None:
            raise Exception(f'PositionService: Unable to close position. '
                            f'Position state {position.getStatus()} violation')
        order = self.brokerService.exitOrder(position.getBrokerOrder())
        position.setBrokerOrder(order)
        position.setStatus(PositionStatusEnum.EXITED)
        return position


