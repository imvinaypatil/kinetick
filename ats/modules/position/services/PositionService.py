from ats.modules.broker.services.BrokerService import BrokerService
from ats.modules.position.aggregates.Position import Position, PositionStateEnum, PositionDirectionEnum
from ats.shared.types.enums import TransactionTypeEnum


class PositionService:

    def __init__(self, brokerService: BrokerService):
        self.brokerService = brokerService

    def isPositionFilled(self, position: Position) -> bool:
        if position.getState() == PositionStateEnum.FILLED:
            return True
        else:
            status = self.brokerService.orderStatus(position.getBrokerOrder())
            position.setState(status)
            if status is PositionStateEnum.FILLED:
                return True
        return False

    def fetchPositionStatus(self, position: Position) -> PositionStateEnum:
        if position.getState() is PositionStateEnum.EXITED:
            return PositionStateEnum.EXITED
        position.setState(self.brokerService.orderStatus(position.getBrokerOrder()))
        return position.getState()

    def openPosition(self, position: Position, txnType: TransactionTypeEnum) -> Position:
        if position.getState() is not PositionStateEnum.CREATED:
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
        if position.getState() in [PositionStateEnum.EXITED]:
            raise Exception('PositionService: Unable to close position. Position state {EXITED} violation')
        if position.getBrokerOrder() is None:
            raise Exception(f'PositionService: Unable to close position. '
                            f'Position state {position.getState()} violation')
        order = self.brokerService.exitOrder(position.getBrokerOrder())
        position.setBrokerOrder(order)
        position.setState(PositionStateEnum.EXITED)
        return position


