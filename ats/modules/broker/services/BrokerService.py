from ats.modules.broker.clients.BrokerClient import BrokerClient
from ats.modules.position.aggregates.Position import Position, RawBrokerOrder, PositionStatusEnum


class BrokerService:

    def __init__(self, client: BrokerClient):
        self.client = client

    def createOrderDraft(self, position: Position) -> RawBrokerOrder:
        orderDraft = self.client.createOrderDraft()
        return orderDraft

    def placeOrder(self, rawBrokerPosition: RawBrokerOrder) -> RawBrokerOrder:
        return self.client.placeOrder(rawBrokerPosition)

    def exitOrder(self, order: RawBrokerOrder) -> RawBrokerOrder:
        """if order is not filled yet then cancel the order otherwise place a reverse order against the open position
        to close it."""
        if self.client.orderStatus(order) is PositionStatusEnum.OPEN:
            return self.client.cancelOrder(order)

        # TODO if there's enough holdings with same qty then place reverse order

    def orderStatus(self, order: RawBrokerOrder) -> PositionStatusEnum:
        return self.client.orderStatus(order)