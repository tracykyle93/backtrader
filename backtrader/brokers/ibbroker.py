#!/usr/bin/env python
"""Interactive Brokers Broker Module - IB trading implementation.

This module provides the IBBroker for trading through Interactive
Brokers TWS or IB Gateway using the modern ibapi (TWS API).

Classes:
    IBOrderState: Wraps IB OrderState object.
    IBOrder: IB-specific order implementation.
    IBBroker: Broker implementation for IB trading.

Example:
    >>> store = bt.stores.IBStore(port=7497)
    >>> cerebro.setbroker(store.getbroker())
"""

import collections
import threading
import uuid
from datetime import date, datetime, timedelta

from ibapi.order import Order as IBApiOrder

from ..broker import BrokerBase
from ..comminfo import CommInfoBase
from ..order import Order, OrderBase
from ..stores import ibstore
from ..utils import date2num, num2date
from ..utils.py3 import queue


class IBOrderState:
    """Wraps Interactive Brokers OrderState object.

    This class wraps the IB OrderState object to provide convenient
    access to order state fields and string representation.

    Attributes:
        status: Order status.
        initMarginBefore: Initial margin before order.
        maintMarginBefore: Maintenance margin before order.
        equityWithLoanBefore: Equity with loan before order.
        initMarginChange: Change in initial margin.
        maintMarginChange: Change in maintenance margin.
        equityWithLoanChange: Change in equity with loan.
        initMarginAfter: Initial margin after order.
        maintMarginAfter: Maintenance margin after order.
        equityWithLoanAfter: Equity with loan after order.
        commission: Commission charged.
        minCommission: Minimum commission.
        maxCommission: Maximum commission.
        commissionCurrency: Currency of commission.
        warningText: Warning message text.
        completedTime: Time the order completed.
        completedStatus: Completed status string.
    """

    _fields = [
        "status",
        "initMarginBefore",
        "maintMarginBefore",
        "equityWithLoanBefore",
        "initMarginChange",
        "maintMarginChange",
        "equityWithLoanChange",
        "initMarginAfter",
        "maintMarginAfter",
        "equityWithLoanAfter",
        "commission",
        "minCommission",
        "maxCommission",
        "commissionCurrency",
        "warningText",
        "completedTime",
        "completedStatus",
    ]

    def __init__(self, orderstate):
        """Initialize the IBOrderState from an IB OrderState object.

        Args:
            orderstate: IB OrderState object containing order state information.
        """
        for field in self._fields:
            setattr(self, field, getattr(orderstate, field, None))

    def __str__(self):
        txt = []
        txt.append("--- ORDERSTATE BEGIN")
        for field in self._fields:
            txt.append(f"{field.capitalize()}: {getattr(self, field)}")
        txt.append("--- ORDERSTATE END")
        return "\n".join(txt)


class IBOrder(OrderBase, IBApiOrder):
    """Subclasses the ibapi Order to provide the minimum extra functionality
    needed to be compatible with the internally defined orders

    Once ``OrderBase`` has processed the parameters, the __init__ method takes
    over to use the parameter values and set the appropriate values in the
    ibapi.order.Order object

    Any extra parameters supplied with kwargs are applied directly to the
    ibapi.order.Order object, which could be used as follows::

      Example: if the four order execution types directly supported by
      ``backtrader`` are not enough, in the case of, for example
      *Interactive Brokers* the following could be passed as *kwargs*:

        orderType='LIT', lmtPrice=10.0, auxPrice=9.8

      This would override the settings created by ``backtrader`` and
      generate a ``LIMIT IF TOUCHED`` order with a *touched* price of 9.8
      and a *limit* price of 10.0.

    This would be done almost always from the ``Buy`` and ``Sell`` methods of
    the ``Strategy`` subclass being used in ``Cerebro``
    """

    def __str__(self):
        """Get the printout from the base class and add some ibapi Order
        specific fields"""
        basetxt = super().__str__()
        tojoin = [basetxt]
        tojoin.append(f"Ref: {self.ref}")
        tojoin.append(f"orderId: {self.orderId}")
        tojoin.append(f"Action: {self.action}")
        tojoin.append(f"Size (ib): {self.totalQuantity}")
        tojoin.append(f"Lmt Price: {self.lmtPrice}")
        tojoin.append(f"Aux Price: {self.auxPrice}")
        tojoin.append(f"OrderType: {self.orderType}")
        tojoin.append(f"Tif (Time in Force): {self.tif}")
        tojoin.append(f"GoodTillDate: {self.goodTillDate}")
        return "\n".join(tojoin)

    _IBOrdTypes = {
        None: "MKT",
        Order.Market: "MKT",
        Order.Limit: "LMT",
        Order.Close: "MOC",
        Order.Stop: "STP",
        Order.StopLimit: "STPLMT",
        Order.StopTrail: "TRAIL",
        Order.StopTrailLimit: "TRAIL LIMIT",
    }

    def __init__(self, action, **kwargs):
        """Initialize the IBOrder with action and order parameters.

        Args:
            action (str): Order direction, either 'BUY' or 'SELL'.
            **kwargs: Additional order parameters including price, size,
                exectype, valid, and other IB-specific parameters.

        Raises:
            KeyError: If invalid order type is specified.
        """
        self._willexpire = False
        self.ordtype = self.Buy if action == "BUY" else self.Sell

        super().__init__()
        IBApiOrder.__init__(self)

        self.orderType = self._IBOrdTypes[self.exectype]
        self.permId = 0

        self.action = action

        self.lmtPrice = 0.0
        self.auxPrice = 0.0

        if self.exectype == self.Market:
            pass
        elif self.exectype == self.Close:
            pass
        elif self.exectype == self.Limit:
            self.lmtPrice = self.price
        elif self.exectype == self.Stop:
            self.auxPrice = self.price
        elif self.exectype == self.StopLimit:
            self.lmtPrice = self.pricelimit
            self.auxPrice = self.price
        elif self.exectype == self.StopTrail:
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                self.trailingPercent = self.trailpercent * 100.0
        elif self.exectype == self.StopTrailLimit:
            self.trailStopPrice = self.lmtPrice = self.price
            self.lmtPrice = self.pricelimit
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                self.trailingPercent = self.trailpercent * 100.0

        self.totalQuantity = abs(self.size)
        self.transmit = self.transmit
        if self.parent is not None:
            self.parentId = self.parent.orderId

        if self.valid is None:
            tif = "GTC"
        elif isinstance(self.valid, (datetime, date)):
            tif = "GTD"
            self.goodTillDate = self.valid.strftime("%Y%m%d %H:%M:%S")
        elif isinstance(self.valid, (timedelta,)):
            if self.valid == self.DAY:
                tif = "DAY"
            else:
                tif = "GTD"
                valid = datetime.now() + self.valid
                self.goodTillDate = valid.strftime("%Y%m%d %H:%M:%S")
        elif self.valid == 0:
            tif = "DAY"
        else:
            tif = "GTD"
            valid = num2date(self.valid)
            self.goodTillDate = valid.strftime("%Y%m%d %H:%M:%S")

        self.tif = tif

        self.ocaType = 1

        for k in kwargs:
            setattr(self, k, kwargs[k])


class IBCommInfo(CommInfoBase):
    """
    Commissions are calculated by ib, but the trade calculations in the
    ```Strategy`` rely on the order carrying a CommInfo object attached for the
    calculation of the operation cost and value.

    These are non-critical information, but removing them from the trade could
    break existing usage, and it is better to provide a CommInfo objet which
    enables those calculations even if with approvimate values.

    The margin calculation is not known in advance information with IB
    (margin impact can be gotten from OrderState objects, and therefore it is
    left as a future exercise to get it"""

    def getvaluesize(self, size, price):
        """Calculate the value size for margin calculation.

        Args:
            size (float): Position size.
            price (float): Price of the instrument.

        Returns:
            float: The value size calculated as absolute size times price.
        """
        return abs(size) * price

    def getoperationcost(self, size, price):
        """Returns the necessary amount of cash an operation would cost"""
        return abs(size) * price


def _register_broker_class(broker_cls):
    """Register broker class with the store when module is loaded"""
    from backtrader.stores import ibstore

    ibstore.IBStore.BrokerCls = broker_cls
    return broker_cls


@_register_broker_class
class IBBroker(BrokerBase):
    """Broker implementation for Interactive Brokers.

    This class maps the orders/positions from Interactive Brokers to the
    internal API of `backtrader`.

    Notes:

      - ``tradeid`` is not really supported, because the profit and loss are
        taken directly from IB. Because (as expected) calculates it in FIFO
        manner, the pnl is not accurate for the tradeid.

      - Position

        If there is an open position for an asset at the beginning of
        operaitons or orders given by other means, change a position, the trades
        calculated in the `Strategy` in cerebro will not reflect the reality.

        To avoid this, this broker would have to do its own position
        management which would also allow tradeid with multiple ids (profit and
        loss would also be calculated locally), but could be considered to be
        defeating the purpose of working with a live broker
    """

    def __init__(self, **kwargs):
        """Initialize the IBBroker with IBStore connection.

        Args:
            **kwargs: Arguments passed to IBStore for connection setup
                (e.g., host, port, clientId).
        """
        super().__init__()
        self.ib = ibstore.IBStore(**kwargs)
        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0
        self._lock_orders = threading.Lock()
        self.orderbyid = dict()
        self.executions = dict()
        self.ordstatus = collections.defaultdict(dict)
        self.notifs = queue.Queue()
        self.tonotify = collections.deque()

    def start(self):
        """Start the broker and IBStore connection.

        Requests account updates and initializes cash and value from IB.
        Sets starting cash and value to 0.0 if connection fails.
        """
        super().start()
        self.ib.start(broker=self)
        if self.ib.connected():
            self.ib.reqAccountUpdates()
            self.startingcash = self.cash = self.ib.get_acc_cash()
            self.startingvalue = self.value = self.ib.get_acc_value()
        else:
            self.startingcash = self.cash = 0.0
            self.startingvalue = self.value = 0.0

    def stop(self):
        """Stop the broker and IBStore connection.

        Stops the IB connection and cleans up resources.
        """
        super().stop()
        self.ib.stop()

    def getcash(self):
        """Get the current cash balance from IB.

        Returns:
            float: Current cash balance in the account.
        """
        self.cash = self.ib.get_acc_cash()
        return self.cash

    def getvalue(self, datas=None):
        """Get the current account value from IB.

        Args:
            datas: Not used, kept for API compatibility.

        Returns:
            float: Current account value including cash and positions.
        """
        self.value = self.ib.get_acc_value()
        return self.value

    def getposition(self, data, clone=True):
        """Get the current position for a data feed from IB.

        Args:
            data: Data feed object.
            clone (bool): Whether to return a cloned position object.
                Defaults to True.

        Returns:
            Position object containing size, price, and other position details.
        """
        return self.ib.getposition(data.tradecontract, clone=clone)

    def cancel(self, order):
        """Cancel an active order.

        Args:
            order: IBOrder object to cancel.

        Note:
            If order is already cancelled or not found, this method
            does nothing.
        """
        try:
            _order = self.orderbyid[order.orderId]
        except (ValueError, KeyError):
            return

        if order.status == Order.Cancelled:
            return

        self.ib.cancelOrder(order.orderId)

    def orderstatus(self, order):
        """Get the current status of an order.

        Args:
            order: IBOrder object to query.

        Returns:
            Order.Status: The current status of the order.
        """
        try:
            o = self.orderbyid[order.orderId]
        except (ValueError, KeyError):
            o = order

        return o.status

    def submit(self, order):
        """Submit an order to Interactive Brokers.

        Args:
            order: IBOrder object to submit.

        Returns:
            IBOrder: The submitted order object.
        """
        order.submit(self)

        if order.oco is None:
            order.ocaGroup = str(uuid.uuid4())
        else:
            order.ocaGroup = self.orderbyid[order.oco.orderId].ocaGroup

        self.orderbyid[order.orderId] = order
        self.ib.placeOrder(order.orderId, order.data.tradecontract, order)
        self.notify(order)

        return order

    def getcommissioninfo(self, data):
        """Get commission information for a data feed.

        Args:
            data: Data feed object.

        Returns:
            IBCommInfo: Commission info object with multiplier and
                stocklike settings based on the contract type.
        """
        contract = data.tradecontract
        try:
            mult = float(contract.multiplier)
        except (ValueError, TypeError):
            mult = 1.0

        stocklike = contract.secType not in (
            "FUT",
            "OPT",
            "FOP",
        )

        return IBCommInfo(mult=mult, stocklike=stocklike)

    def _makeorder(
        self,
        action,
        owner,
        data,
        size,
        price=None,
        plimit=None,
        exectype=None,
        valid=None,
        tradeid=0,
        **kwargs,
    ):
        """Create an IBOrder with the specified parameters.

        Args:
            action (str): Order direction ('BUY' or 'SELL').
            owner: Owner object (typically a strategy).
            data: Data feed object.
            size (int): Order size (positive for buy, negative for sell).
            price (float, optional): Limit or stop price.
            plimit (float, optional): Limit price for stop-limit orders.
            exectype (Order.ExecType, optional): Order execution type.
            valid (datetime/timedelta, optional): Order validity period.
            tradeid (int, optional): Trade identifier. Defaults to 0.
            **kwargs: Additional IB-specific order parameters.

        Returns:
            IBOrder: Configured order object with commission info attached.
        """
        order = IBOrder(
            action,
            owner=owner,
            data=data,
            size=size,
            price=price,
            pricelimit=plimit,
            exectype=exectype,
            valid=valid,
            tradeid=tradeid,
            clientId=self.ib.clientId,
            orderId=self.ib.nextOrderId(),
            **kwargs,
        )

        order.addcomminfo(self.getcommissioninfo(data))
        return order

    def buy(
        self,
        owner,
        data,
        size,
        price=None,
        plimit=None,
        exectype=None,
        valid=None,
        tradeid=0,
        **kwargs,
    ):
        """Create and submit a buy order.

        Args:
            owner: Owner object (typically a strategy).
            data: Data feed object.
            size (int): Order size (negative for buy orders).
            price (float, optional): Limit or stop price.
            plimit (float, optional): Limit price for stop-limit orders.
            exectype (Order.ExecType, optional): Order execution type.
            valid (datetime/timedelta, optional): Order validity period.
            tradeid (int, optional): Trade identifier. Defaults to 0.
            **kwargs: Additional IB-specific order parameters.

        Returns:
            IBOrder: The submitted buy order.
        """
        order = self._makeorder(
            "BUY", owner, data, size, price, plimit, exectype, valid, tradeid, **kwargs
        )

        return self.submit(order)

    def sell(
        self,
        owner,
        data,
        size,
        price=None,
        plimit=None,
        exectype=None,
        valid=None,
        tradeid=0,
        **kwargs,
    ):
        """Create and submit a sell order.

        Args:
            owner: Owner object (typically a strategy).
            data: Data feed object.
            size (int): Order size (positive for sell orders).
            price (float, optional): Limit or stop price.
            plimit (float, optional): Limit price for stop-limit orders.
            exectype (Order.ExecType, optional): Order execution type.
            valid (datetime/timedelta, optional): Order validity period.
            tradeid (int, optional): Trade identifier. Defaults to 0.
            **kwargs: Additional IB-specific order parameters.

        Returns:
            IBOrder: The submitted sell order.
        """
        order = self._makeorder(
            "SELL", owner, data, size, price, plimit, exectype, valid, tradeid, **kwargs
        )

        return self.submit(order)

    def notify(self, order):
        """Store a cloned order notification in the notification queue.

        Args:
            order: IBOrder object to notify.
        """
        self.notifs.put(order.clone())

    def get_notification(self):
        """Get the next notification from the queue.

        Returns:
            IBOrder or None: The next order notification, or None if queue is empty.
        """
        try:
            return self.notifs.get(False)
        except queue.Empty:
            pass

        return None

    def next(self):
        """Mark a notification boundary.

        Places None in the notification queue to signal the end of
        current notifications.
        """
        self.notifs.put(None)

    SUBMITTED, FILLED, CANCELLED, INACTIVE, PENDINGSUBMIT, PENDINGCANCEL, PRESUBMITTED = (
        "Submitted",
        "Filled",
        "Cancelled",
        "Inactive",
        "PendingSubmit",
        "PendingCancel",
        "PreSubmitted",
    )

    def push_orderstatus(self, msg):
        """Process and update order status from IB message.

        Args:
            msg: Order status message from IB containing orderId,
                status, and filled quantity.

        Note:
            Handles various order states including Submitted, Cancelled,
            Inactive, Filled, PendingSubmit, PreSubmitted, and PendingCancel.
        """
        try:
            order = self.orderbyid[msg.orderId]
        except KeyError:
            return

        if msg.status == self.SUBMITTED and msg.filled == 0:
            if order.status == order.Accepted:
                return

            order.accept(self)
            self.notify(order)

        elif msg.status == self.CANCELLED:
            if order.status in [order.Cancelled, order.Expired]:
                return

            if order._willexpire:
                order.expire()
            else:
                order.cancel()
            self.notify(order)

        elif msg.status == self.PENDINGCANCEL:
            if order.status == order.Cancelled:
                return

        elif msg.status == self.INACTIVE:
            if order.status == order.Rejected:
                return

            order.reject(self)
            self.notify(order)

        elif msg.status in [self.SUBMITTED, self.FILLED]:
            self.ordstatus[msg.orderId][msg.filled] = msg

        elif msg.status in [self.PENDINGSUBMIT, self.PRESUBMITTED]:
            if msg.filled:
                self.ordstatus[msg.orderId][msg.filled] = msg
        else:
            pass

    def push_execution(self, ex):
        """Store an execution report from IB.

        Args:
            ex: Execution object from ibapi containing execution details
                including execId, orderId, shares, price, and time.
        """
        self.executions[ex.execId] = ex

    def push_commissionreport(self, cr):
        """Process commission report and update order execution details.

        Args:
            cr: Commission report object from ibapi containing execId,
                commission amount, and realizedPNL.

        Note:
            This method updates the order with execution details,
            calculates closed/opened positions and commissions, and
            triggers order notifications.
        """
        with self._lock_orders:
            ex = self.executions.pop(cr.execId)
            oid = ex.orderId
            order = self.orderbyid[oid]
            ostatus = self.ordstatus[oid].pop(ex.cumQty)

            position = self.getposition(order.data, clone=False)
            pprice_orig = position.price
            size = ex.shares if ex.side[0] == "B" else -ex.shares
            price = ex.price
            psize, pprice, opened, closed = position.update(size, price)

            comm = cr.commission
            closedcomm = comm * closed / size
            openedcomm = comm - closedcomm

            comminfo = order.comminfo
            closedvalue = comminfo.getoperationcost(closed, pprice_orig)
            openedvalue = comminfo.getoperationcost(opened, price)

            pnl = cr.realizedPNL if closed else 0.0

            dt = date2num(datetime.strptime(ex.time, "%Y%m%d  %H:%M:%S"))

            margin = order.data.close[0]

            order.execute(
                dt,
                size,
                price,
                closed,
                closedvalue,
                closedcomm,
                opened,
                openedvalue,
                openedcomm,
                margin,
                pnl,
                psize,
                pprice,
            )

            if ostatus.status == self.FILLED:
                order.completed()
                self.ordstatus.pop(oid)
            else:
                order.partial()

            if oid not in self.tonotify:
                self.tonotify.append(oid)

    def push_portupdate(self):
        """Process portfolio update and notify pending orders.

        Note:
            Called when IBStore receives a portfolio update. Notifies
            all orders pending notification. Portfolio updates intermixed
            with split executions signal that the strategy can be notified.
        """
        with self._lock_orders:
            while self.tonotify:
                oid = self.tonotify.popleft()
                order = self.orderbyid[oid]
                self.notify(order)

    def push_ordererror(self, msg):
        """Process order error messages from IB.

        Args:
            msg: Error message object containing error code and order ID.

        Note:
            Handles error code 202 (order cancellation) and 201 (order rejection).
            All other error codes result in order rejection.
        """
        with self._lock_orders:
            try:
                order = self.orderbyid[msg.id]
            except (KeyError, AttributeError):
                return

            if msg.errorCode == 202:
                if not order.alive():
                    return
                order.cancel()

            elif msg.errorCode == 201:
                if order.status == order.Rejected:
                    return
                order.reject()

            else:
                order.reject()

            self.notify(order)

    def push_orderstate(self, msg):
        """Process order state messages from IB.

        Args:
            msg: Order state message containing orderId and orderState
                with status information.

        Note:
            Detects when orders are about to expire by checking for
            PendingCancel/Cancelled status in openOrder messages.
        """
        with self._lock_orders:
            try:
                order = self.orderbyid[msg.orderId]
            except (KeyError, AttributeError):
                return

            if msg.orderState.status in ["PendingCancel", "Cancelled", "Canceled"]:
                order._willexpire = True
