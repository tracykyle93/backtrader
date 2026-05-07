#!/usr/bin/env python
"""Interactive Brokers Store Module - IB TWS API connection.

This module provides the IBStore for connecting to Interactive Brokers
TWS or IB Gateway for trading and data, using the official ibapi library
(EClient + EWrapper pattern).

Classes:
    IBStore: Singleton store for IB connections.

Functions:
    _ts2dt: Converts IB timestamp to datetime.

Example:
    >>> store = bt.stores.IBStore(port=7497, clientId=1)
    >>> cerebro.setbroker(store.getbroker())
"""

import bisect
import collections
import itertools
import random
import threading
import time
from copy import copy
from datetime import datetime, timedelta

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order_cancel import OrderCancel

from backtrader.mixins.singleton import ParameterizedSingletonMixin

from ..dataseries import TimeFrame
from ..position import Position
from ..utils import UTC, AutoDict
from ..utils.py3 import long, queue


def _ts2dt(tstamp=None):
    """Transforms a RTVolume timestamp to a datetime object.

    Args:
        tstamp: Optional timestamp value. If None, empty, or False,
            returns current UTC time. Otherwise, converts the timestamp
            to a datetime object.

    Returns:
        datetime: A datetime object in UTC timezone.
    """
    if not tstamp:
        return datetime.now(UTC)
    sec, msec = divmod(long(tstamp), 1)
    usec = msec * 1000
    return datetime.fromtimestamp(sec, UTC).replace(microsecond=usec)


class RTVolume:
    """Parses a tickString tickType 48 (RTVolume) event from the IB API into its
    constituent fields.
    Supports using a "price" to simulate an RTVolume from a tickPrice event.
    """

    _fields = [
        ("price", float),
        ("size", int),
        ("datetime", _ts2dt),
        ("volume", int),
        ("vwap", float),
        ("single", bool),
    ]

    def __init__(self, rtvol="", price=None, tmoffset=None):
        tokens = iter(rtvol.split(";"))
        for name, func in self._fields:
            setattr(self, name, func(next(tokens)) if rtvol else func())
        if price is not None:
            self.price = price
        if tmoffset is not None:
            self.datetime += tmoffset


class _MsgNamespace:
    """Simple namespace to emulate old IBPy msg objects for broker compatibility.

    Provides attribute access plus values()/items() methods that the
    notification system expects.
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()


class _OrderStateAdapter:
    """Adapter normalizing ibapi OrderState for broker compatibility.

    The broker code accesses msg.orderState.status (plain attribute).
    """

    def __init__(self, order_state):
        self.status = order_state.status
        self.initMarginBefore = getattr(order_state, "initMarginBefore", None)
        self.maintMarginBefore = getattr(order_state, "maintMarginBefore", None)
        self.equityWithLoanBefore = getattr(order_state, "equityWithLoanBefore", None)
        self.commissionAndFees = getattr(order_state, "commissionAndFees", None)
        self.minCommissionAndFees = getattr(order_state, "minCommissionAndFees", None)
        self.maxCommissionAndFees = getattr(order_state, "maxCommissionAndFees", None)
        self.commissionAndFeesCurrency = getattr(order_state, "commissionAndFeesCurrency", None)
        self.warningText = getattr(order_state, "warningText", None)


class _ExecutionAdapter:
    """Adapter normalizing ibapi Execution fields for broker compatibility.

    The broker code accesses execId, orderId, cumQty, shares, side, price, time.
    The ibapi Execution object already uses these names, but we also expose
    'commission' from the report when available.
    """

    def __init__(self, execution):
        self.execId = execution.execId
        self.orderId = execution.orderId
        self.cumQty = execution.cumQty
        self.shares = execution.shares
        self.side = execution.side
        self.price = execution.price
        self.time = execution.time
        self.acctNumber = execution.acctNumber
        self.exchange = execution.exchange
        self.permId = execution.permId
        self.clientId = execution.clientId
        self.liquidation = execution.liquidation
        self.avgPrice = execution.avgPrice
        self.orderRef = execution.orderRef


class _CommissionReportAdapter:
    """Adapter normalizing ibapi CommissionAndFeesReport for broker compatibility.

    The broker code accesses execId, commission, realizedPNL.
    """

    def __init__(self, report):
        self.execId = report.execId
        self.commission = report.commissionAndFees
        self.realizedPNL = report.realizedPNL
        self.currency = report.currency
        self.yield_ = report.yield_
        self.yieldRedemptionDate = report.yieldRedemptionDate


class IBStore(ParameterizedSingletonMixin, EWrapper, EClient):
    """Singleton class wrapping the official IB TWS API (EClient + EWrapper).

    This class inherits from both EWrapper (receives callbacks) and EClient
    (sends requests), using the standard combined pattern for the IB API.
    The ParameterizedSingletonMixin provides singleton behavior.

    The parameters can also be specified in the classes which use this store,
    like ``IBData`` and ``IBBroker``.

    Params:

      - ``host`` (default:``127.0.0.1``): where IB TWS or IB Gateway are
        actually running.

      - ``port`` (default: ``7496``): port to connect to. The demo system uses
        ``7497``

      - ``clientId`` (default: ``None``): which clientId to use to connect to
        TWS. ``None``: generates a random id between 1 and 65535.
        An ``integer``: will be passed as the value to use.

      - ``notifyall`` (default: ``False``): If ``False`` only ``error``
        messages will be sent to the ``notify_store`` methods of ``Cerebro``
        and ``Strategy``. If ``True``, each and every message received from
        TWS will be notified.

      - ``_debug`` (default: ``False``): Print all messages received from TWS
        to standard output.

      - ``reconnect`` (default: ``3``): Number of attempts to try to reconnect
        after the 1st connection attempt fails. Set it to ``-1`` to keep on
        reconnecting forever.

      - ``timeout`` (default: ``3.0``): Time in seconds between reconnection
        attempts.

      - ``timeoffset`` (default: ``True``): If True, the time obtained from
        ``reqCurrentTime`` (IB Server time) will be used to calculate the
        offset to localtime and this offset will be used for the price
        notifications.

      - ``timerefresh`` (default: ``60.0``): Time in seconds: how often the
        time offset has to be refreshed.

      - ``indcash`` (default: ``True``): Manage IND codes as if they were cash
        for price retrieval.
    """

    REQIDBASE = 0x01000000

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    _durations = dict(
        [
            ("60 S", ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min")),
            ("120 S", ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins")),
            (
                "180 S",
                ("1 secs", "5 secs", "10 secs", "15 secs", "30 secs", "1 min", "2 mins", "3 mins"),
            ),
            (
                "300 S",
                (
                    "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins",
                ),
            ),
            (
                "600 S",
                (
                    "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                ),
            ),
            (
                "900 S",
                (
                    "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins",
                ),
            ),
            (
                "1200 S",
                (
                    "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins",
                ),
            ),
            (
                "1800 S",
                (
                    "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins",
                ),
            ),
            (
                "3600 S",
                (
                    "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour",
                ),
            ),
            (
                "7200 S",
                (
                    "5 secs", "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                ),
            ),
            (
                "10800 S",
                (
                    "10 secs", "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours", "3 hours",
                ),
            ),
            (
                "14400 S",
                (
                    "15 secs", "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                    "3 hours", "4 hours",
                ),
            ),
            (
                "28800 S",
                (
                    "30 secs",
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                    "3 hours", "4 hours", "8 hours",
                ),
            ),
            (
                "1 D",
                (
                    "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                    "3 hours", "4 hours", "8 hours", "1 day",
                ),
            ),
            (
                "2 D",
                (
                    "2 mins", "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                    "3 hours", "4 hours", "8 hours", "1 day",
                ),
            ),
            (
                "1 W",
                (
                    "3 mins", "5 mins", "10 mins",
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                    "3 hours", "4 hours", "8 hours", "1 day", "1 W",
                ),
            ),
            (
                "2 W",
                (
                    "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
                    "3 hours", "4 hours", "8 hours", "1 day", "1 W",
                ),
            ),
            (
                "1 M",
                (
                    "30 mins", "1 hour", "2 hours", "3 hours", "4 hours",
                    "8 hours", "1 day", "1 W", "1 M",
                ),
            ),
            ("2 M", ("1 day", "1 W", "1 M")),
            ("3 M", ("1 day", "1 W", "1 M")),
            ("4 M", ("1 day", "1 W", "1 M")),
            ("5 M", ("1 day", "1 W", "1 M")),
            ("6 M", ("1 day", "1 W", "1 M")),
            ("7 M", ("1 day", "1 W", "1 M")),
            ("8 M", ("1 day", "1 W", "1 M")),
            ("9 M", ("1 day", "1 W", "1 M")),
            ("10 M", ("1 day", "1 W", "1 M")),
            ("11 M", ("1 day", "1 W", "1 M")),
            ("1 Y", ("1 day", "1 W", "1 M")),
        ]
    )

    _sizes = {
        "secs": (TimeFrame.Seconds, 1),
        "min": (TimeFrame.Minutes, 1),
        "mins": (TimeFrame.Minutes, 1),
        "hour": (TimeFrame.Minutes, 60),
        "hours": (TimeFrame.Minutes, 60),
        "day": (TimeFrame.Days, 1),
        "W": (TimeFrame.Weeks, 1),
        "M": (TimeFrame.Months, 1),
    }

    _dur2tf = {
        "S": TimeFrame.Seconds,
        "D": TimeFrame.Days,
        "W": TimeFrame.Weeks,
        "M": TimeFrame.Months,
        "Y": TimeFrame.Years,
    }

    params = (
        ("host", "127.0.0.1"),
        ("port", 7496),
        ("clientId", None),  # None generates a random clientid 1 -> 2^16
        ("notifyall", False),
        ("_debug", False),
        ("reconnect", 3),  # -1 forever, 0 No, > 0 number of retries
        ("timeout", 3.0),  # timeout between reconnections
        ("timeoffset", True),  # Use offset to server for timestamps if needed
        ("timerefresh", 60.0),  # How often to refresh the timeoffset
        ("indcash", True),  # Treat IND codes as CASH elements
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns ``DataCls`` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered ``BrokerCls``"""
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        ParameterizedSingletonMixin.__init__(self)
        EClient.__init__(self, wrapper=self)

        self._lock_q = threading.Lock()
        self._lock_accupd = threading.Lock()
        self._lock_pos = threading.Lock()
        self._lock_notif = threading.Lock()

        self._event_managed_accounts = threading.Event()
        self._event_accdownload = threading.Event()

        self.dontreconnect = False
        self._env = None
        self.broker = None
        self.datas = list()
        self.ccount = 0

        self._lock_tmoffset = threading.Lock()
        self.tmoffset = timedelta()

        # Structures to hold data requests
        self.qs = collections.OrderedDict()  # key: tickerId -> queues
        self.ts = collections.OrderedDict()  # key: queue -> tickerId
        self.iscash = dict()  # tickerIds from cash products
        self.histexreq = dict()  # holds segmented historical requests
        self.histfmt = dict()  # holds datetimeformat for request
        self.histsend = dict()  # holds sessionend (data time) for request
        self.histtz = dict()  # holds timezone for request

        self.acc_cash = AutoDict()
        self.acc_value = AutoDict()
        self.acc_upds = AutoDict()

        self.port_update = False

        self.positions = collections.defaultdict(Position)
        self._tickerId = itertools.count(self.REQIDBASE)
        self.orderid = None
        self.cdetails = collections.defaultdict(list)
        self.managed_accounts = list()
        self.notifs = queue.Queue()

        if self.p.clientId is None:
            self._bt_clientId = random.randint(1, pow(2, 16) - 1)
        else:
            self._bt_clientId = self.p.clientId

        self._ever_connected = False
        self._msg_thread = None

        # Build duration/size lookup tables
        def keyfn(x):
            n, t = x.split()
            tf, comp = self._sizes[t]
            return (tf, int(n) * comp)

        def key2fn(x):
            n, d = x.split()
            tf = self._dur2tf[d]
            return (tf, int(n))

        self.revdur = collections.defaultdict(list)
        for duration, barsizes in self._durations.items():
            for barsize in barsizes:
                self.revdur[keyfn(barsize)].append(duration)

        for barsize in self.revdur:
            self.revdur[barsize].sort(key=key2fn)

    # ---------------------------------------------------------------
    # Lifecycle management
    # ---------------------------------------------------------------

    def start(self, data=None, broker=None):
        """Start the IBStore connection and associated data feeds or broker."""
        self.reconnect(fromstart=True)

        if data is not None:
            self._env = data._env
            self.datas.append(data)
            return self.getTickerQueue(start=True)

        elif broker is not None:
            self.broker = broker

    def stop(self):
        """Stop the IBStore connection and cleanup resources."""
        try:
            self.disconnect()
        except AttributeError:
            pass

        self._event_managed_accounts.set()
        self._event_accdownload.set()

    def logmsg(self, *args):
        """Log messages to standard output when debug mode is enabled."""
        if self.p._debug:
            print(*args)

    def connected(self):
        """Check if currently connected to IB TWS or Gateway."""
        try:
            return self.isConnected()
        except AttributeError:
            pass
        return False

    def reconnect(self, fromstart=False, resub=False):
        """Attempt to connect or reconnect to IB TWS/Gateway with retry logic.

        Args:
            fromstart: If True, indicates this is the initial connection attempt.
            resub: If True, restarts data subscriptions when connection succeeds.

        Returns:
            bool: True if connection is successful, False if all retry attempts
                fail or if dontreconnect flag is set.
        """
        firstconnect = not self._ever_connected
        if not firstconnect:
            if self.isConnected():
                if resub:
                    self.startdatas()
                return True

        if self.dontreconnect:
            return False

        retries = self.p.reconnect
        if retries >= 0:
            retries += firstconnect

        while retries < 0 or retries:
            if not firstconnect:
                time.sleep(self.p.timeout)
            firstconnect = False

            try:
                EClient.connect(self, self.p.host, self.p.port, self._bt_clientId)
            except Exception:
                pass

            if self.isConnected():
                self._ever_connected = True

                self._msg_thread = threading.Thread(
                    target=self.run, daemon=True, name="IBStore-MsgLoop"
                )
                self._msg_thread.start()

                if not fromstart or resub:
                    self.startdatas()
                return True

            if retries > 0:
                retries -= 1

        self.dontreconnect = True
        return False

    def startdatas(self):
        """Start data subscriptions for all registered data feeds."""
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.reqdata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

    def stopdatas(self):
        """Stop data subscriptions for all registered data feeds."""
        qs = list(self.qs.values())
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.canceldata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

        for q in reversed(qs):  # datamaster the last one to get a None
            q.put(None)

    def get_notifications(self):
        """Return the pending "store" notifications."""
        self.notifs.put(None)
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:
                break
            notifs.append(notif)
        return notifs

    # ---------------------------------------------------------------
    # EWrapper callback overrides
    # ---------------------------------------------------------------

    def error(self, reqId, errorTime, errorCode, errorString,
              advancedOrderRejectJson=""):
        """Handle error messages from IB API.

        Maps the modern ibapi error callback (with direct parameters) into
        the existing queue-based notification and error-handling logic.
        """
        msg = _MsgNamespace(
            id=reqId,
            errorCode=errorCode,
            errorMsg=errorString,
            errorTime=errorTime,
        )

        self.logmsg(
            f"IB Error reqId={reqId} code={errorCode}: {errorString}"
        )

        if not self.p.notifyall:
            self.notifs.put((msg, tuple(msg.values()), dict(msg.items())))

        if errorCode is None:
            pass
        elif errorCode in [200, 203, 162, 320, 321, 322]:
            try:
                q = self.qs[reqId]
            except KeyError:
                pass
            else:
                self.cancelQueue(q, True)

        elif errorCode in [354, 420]:
            try:
                q = self.qs[reqId]
            except KeyError:
                pass
            else:
                q.put(-errorCode)
                self.cancelQueue(q)

        elif errorCode == 10225:
            try:
                q = self.qs[reqId]
            except KeyError:
                pass
            else:
                q.put(-errorCode)

        elif errorCode == 326:  # not recoverable, clientId in use
            self.dontreconnect = True
            self.disconnect()

        elif errorCode == 502:
            self.disconnect()

        elif errorCode == 504:  # Not Connected for data op
            pass

        elif errorCode == 1300:
            self.disconnect()

        elif errorCode == 1100:
            for q in self.ts:
                q.put(-errorCode)

        elif errorCode == 1101:
            for q in self.ts:
                q.put(-errorCode)

        elif errorCode == 1102:
            for q in self.ts:
                q.put(-errorCode)

        elif errorCode < 500:
            if reqId < self.REQIDBASE:
                if self.broker is not None:
                    self.broker.push_ordererror(msg)
            else:
                q = self.qs[reqId]
                self.cancelQueue(q, True)

    def connectionClosed(self):
        """Handle connection closed event from IB API.

        Called by EClient.disconnect() or when TWS closes the connection.
        We only stop datas here; disconnect cleanup is handled by EClient.
        """
        self.stopdatas()

    def managedAccounts(self, accountsList):
        """Handle managed accounts message from IB API."""
        self.managed_accounts = accountsList.split(",")
        self._event_managed_accounts.set()
        self.reqCurrentTime()

    def nextValidId(self, orderId):
        """Handle next valid order ID message from IB API."""
        self.orderid = itertools.count(orderId)

    def currentTime(self, time_val):
        """Handle current time message from IB API."""
        if not self.p.timeoffset:
            return
        curtime = datetime.fromtimestamp(float(time_val))
        with self._lock_tmoffset:
            self.tmoffset = curtime - datetime.now()

        threading.Timer(self.p.timerefresh, self.reqCurrentTime).start()

    def contractDetails(self, reqId, contractDetails):
        """Receive contract details and pass them to the queue."""
        self.qs[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        """Signal end of contract details."""
        self.cancelQueue(self.qs[reqId], True)

    def tickString(self, reqId, tickType, value):
        """Handle tickString messages from IB API.

        Processes tickType 48 (RTVolume) messages which contain real-time
        volume data.
        """
        if tickType == 48:  # RTVolume
            try:
                rtvol = RTVolume(value)
            except ValueError:
                pass
            else:
                self.qs[reqId].put(rtvol)

    def tickPrice(self, reqId, tickType, price, attrib):
        """Handle tick price for cash markets.

        Cash Markets have no notion of "last_price"/"last_size" and the
        tracking of the price is done following the BID price (industry
        de-facto standard with the IB API).
        """
        fieldcode = self.iscash.get(reqId, False)
        if fieldcode:
            if tickType == fieldcode:
                try:
                    if price == -1.0:
                        return
                except AttributeError:
                    pass

                try:
                    rtvol = RTVolume(price=price, tmoffset=self.tmoffset)
                except ValueError:
                    pass
                else:
                    self.qs[reqId].put(rtvol)

    def realtimeBar(self, reqId, time_val, open_, high, low, close,
                    volume, wap, count):
        """Receives x seconds Real Time Bars (5 seconds supported)."""
        msg = _MsgNamespace(
            reqId=reqId,
            time=datetime.fromtimestamp(float(time_val), UTC),
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            wap=wap,
            count=count,
        )
        self.qs[reqId].put(msg)

    def historicalData(self, reqId, bar):
        """Receives the events of a historical data request."""
        tickerId = reqId
        q = self.qs[tickerId]

        msg = _MsgNamespace(
            reqId=reqId,
            date=bar.date,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            barCount=bar.barCount,
            wap=bar.wap,
        )

        dtstr = msg.date
        if self.histfmt[tickerId]:
            sessionend = self.histsend[tickerId]
            dt = datetime.strptime(dtstr, "%Y%m%d")
            dteos = datetime.combine(dt, sessionend)
            tz = self.histtz[tickerId]
            if tz:
                dteostz = tz.localize(dteos)
                dteosutc = dteostz.astimezone(UTC).replace(tzinfo=None)
            else:
                dteosutc = dteos

            if dteosutc <= datetime.now(UTC):
                dt = dteosutc

            msg.date = dt
        else:
            msg.date = datetime.fromtimestamp(long(dtstr), UTC)

        q.put(msg)

    def historicalDataEnd(self, reqId, start, end):
        """Marks the ending of the historical bars reception.

        In the old IBPy API this was signaled by a bar with date starting
        with 'finished-'. The new API uses this separate callback instead.
        """
        tickerId = reqId
        q = self.qs[tickerId]

        self.histfmt.pop(tickerId, None)
        self.histsend.pop(tickerId, None)
        self.histtz.pop(tickerId, None)
        kargs = self.histexreq.pop(tickerId, None)
        if kargs is not None:
            self.reqHistoricalDataEx(tickerId=tickerId, **kargs)
            return

        msg = _MsgNamespace(reqId=reqId, date=None)
        self.cancelQueue(q)
        q.put(msg)

    def openOrder(self, orderId, contract, order, orderState):
        """Receive the event ``openOrder`` events."""
        msg = _MsgNamespace(
            orderId=orderId,
            contract=contract,
            order=order,
            orderState=_OrderStateAdapter(orderState),
        )
        self.broker.push_orderstate(msg)

    def execDetails(self, reqId, contract, execution):
        """Receive execDetails."""
        self.broker.push_execution(_ExecutionAdapter(execution))

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld,
                    mktCapPrice):
        """Receive the event ``orderStatus``."""
        msg = _MsgNamespace(
            orderId=orderId,
            status=status,
            filled=filled,
            remaining=remaining,
            avgFillPrice=avgFillPrice,
            permId=permId,
            parentId=parentId,
            lastFillPrice=lastFillPrice,
            clientId=clientId,
            whyHeld=whyHeld,
            mktCapPrice=mktCapPrice,
        )
        self.broker.push_orderstatus(msg)

    def commissionAndFeesReport(self, commissionAndFeesReport):
        """Receive the event commissionAndFeesReport."""
        self.broker.push_commissionreport(
            _CommissionReportAdapter(commissionAndFeesReport)
        )

    def updateAccountValue(self, key, val, currency, accountName):
        """Handle account value update message from IB API."""
        with self._lock_accupd:
            try:
                value = float(val)
            except ValueError:
                value = val

            self.acc_upds[accountName][key][currency] = value

            if key == "NetLiquidation":
                self.acc_value[accountName] = value
            elif key == "TotalCashBalance" and currency == "BASE":
                self.acc_cash[accountName] = value

    def updatePortfolio(self, contract, position, marketPrice, marketValue,
                        averageCost, unrealizedPNL, realizedPNL, accountName):
        """Handle portfolio update message from IB API."""
        with self._lock_pos:
            if not self._event_accdownload.is_set():
                pos = Position(position, averageCost)
                self.positions[contract.conId] = pos
            else:
                pos = self.positions[contract.conId]
                if not pos.fix(position, averageCost):
                    err = (
                        "The current calculated position and "
                        "the position reported by the broker do not match. "
                        "Operation can continue, but the trades "
                        "calculated in the strategy may be wrong"
                    )
                    self.notifs.put((err, (), {}))

                self.broker.push_portupdate()

    def accountDownloadEnd(self, accountName):
        """Handle account download end message from IB API."""
        self._event_accdownload.set()
        if False:
            if self.port_update:
                self.broker.push_portupdate()
                self.port_update = False

    def position(self, account, contract, pos, avgCost):
        """Receive event positions."""
        pass  # Not implemented yet

    # ---------------------------------------------------------------
    # Time offset
    # ---------------------------------------------------------------

    def timeoffset(self):
        """Get the current time offset between local clock and IB server."""
        with self._lock_tmoffset:
            return self.tmoffset

    # ---------------------------------------------------------------
    # Ticker / Queue management
    # ---------------------------------------------------------------

    def nextTickerId(self):
        """Generate the next unique ticker ID for data requests."""
        return next(self._tickerId)

    def nextOrderId(self):
        """Generate the next valid order ID for placing orders."""
        return next(self.orderid)

    def reuseQueue(self, tickerId):
        """Reuses queue for tickerId, returning the new tickerId and q."""
        with self._lock_q:
            q = self.qs.pop(tickerId, None)
            iscash = self.iscash.pop(tickerId, None)

            tickerId = self.nextTickerId()
            self.ts[q] = tickerId
            self.qs[tickerId] = q
            self.iscash[tickerId] = iscash

        return tickerId, q

    def getTickerQueue(self, start=False):
        """Creates ticker/Queue for data delivery to a data feed."""
        q = queue.Queue()
        if start:
            q.put(None)
            return q

        with self._lock_q:
            tickerId = self.nextTickerId()
            self.qs[tickerId] = q
            self.ts[q] = tickerId
            self.iscash[tickerId] = False

        return tickerId, q

    def cancelQueue(self, q, sendnone=False):
        """Cancels a Queue for data delivery."""
        tickerId = self.ts.pop(q, None)
        self.qs.pop(tickerId, None)
        self.iscash.pop(tickerId, None)

        if sendnone:
            q.put(None)

    def validQueue(self, q):
        """Returns (bool) if a queue is still valid."""
        return q in self.ts

    # ---------------------------------------------------------------
    # Contract details
    # ---------------------------------------------------------------

    def getContractDetails(self, contract, maxcount=None):
        """Get contract details from IB for a given contract."""
        cds = list()
        q = self.reqContractDetails(contract)
        while True:
            msg = q.get()
            if msg is None:
                break
            cds.append(msg)

        if not cds or (maxcount and len(cds) > maxcount):
            err = "Ambiguous contract: none/multiple answers received"
            self.notifs.put((err, cds, {}))
            return None

        return cds

    def reqContractDetails(self, contract):
        """Request contract details from IB API."""
        tickerId, q = self.getTickerQueue()
        EClient.reqContractDetails(self, tickerId, contract)
        return q

    # ---------------------------------------------------------------
    # Historical data requests
    # ---------------------------------------------------------------

    def reqHistoricalDataEx(
        self,
        contract,
        enddate,
        begindate,
        timeframe,
        compression,
        what=None,
        useRTH=False,
        tz="",
        sessionend=None,
        tickerId=None,
    ):
        """Extension of the raw reqHistoricalData proxy, which takes two dates
        rather than a duration, barsize and date.

        It uses the IB published valid duration/barsizes to make a mapping and
        spread a historical request over several historical requests if needed.
        """
        kwargs = locals().copy()
        kwargs.pop("self", None)

        if timeframe < TimeFrame.Seconds:
            return self.getTickerQueue(start=True)

        if enddate is None:
            enddate = datetime.now()

        if begindate is None:
            duration = self.getmaxduration(timeframe, compression)
            if duration is None:
                err = "No duration for historical data request for timeframe/compresison"
                self.notifs.put((err, (), kwargs))
                return self.getTickerQueue(start=True)

            barsize = self.tfcomp_to_size(timeframe, compression)
            if barsize is None:
                err = "No supported barsize for historical data request for timeframe/compresison"
                self.notifs.put((err, (), kwargs))
                return self.getTickerQueue(start=True)

            return self.reqHistoricalData(
                contract=contract,
                enddate=enddate,
                duration=duration,
                barsize=barsize,
                what=what,
                useRTH=useRTH,
                tz=tz,
                sessionend=sessionend,
            )

        durations = self.getdurations(timeframe, compression)
        if not durations:
            return self.getTickerQueue(start=True)

        if tickerId is None:
            tickerId, q = self.getTickerQueue()
        else:
            tickerId, q = self.reuseQueue(tickerId)

        duration = None
        for dur in durations:
            intdate = self.dt_plus_duration(begindate, dur)
            if intdate >= enddate:
                intdate = enddate
                duration = dur
                break

        if duration is None:
            duration = durations[-1]

            self.histexreq[tickerId] = dict(
                contract=contract,
                enddate=enddate,
                begindate=intdate,
                timeframe=timeframe,
                compression=compression,
                what=what,
                useRTH=useRTH,
                tz=tz,
                sessionend=sessionend,
            )

        barsize = self.tfcomp_to_size(timeframe, compression)
        self.histfmt[tickerId] = timeframe >= TimeFrame.Days
        self.histsend[tickerId] = sessionend
        self.histtz[tickerId] = tz

        if contract.secType in ["CASH", "CFD"]:
            self.iscash[tickerId] = 1
            if not what:
                what = "BID"

        elif contract.secType in ["IND"] and self.p.indcash:
            self.iscash[tickerId] = 4

        what = what or "TRADES"

        EClient.reqHistoricalData(
            self,
            tickerId,
            contract,
            intdate.strftime("%Y%m%d %H:%M:%S") + " GMT",
            duration,
            barsize,
            what,
            int(useRTH),
            2,  # formatDate: 2 for unix time in seconds
            False,  # keepUpToDate
            [],  # chartOptions
        )

        return q

    def reqHistoricalData(
        self, contract, enddate, duration, barsize, what=None,
        useRTH=False, tz="", sessionend=None
    ):
        """Proxy to reqHistorical Data."""
        tickerId, q = self.getTickerQueue()

        if contract.secType in ["CASH", "CFD"]:
            self.iscash[tickerId] = True
            if not what:
                what = "BID"
            elif what == "ASK":
                self.iscash[tickerId] = 2
        else:
            what = what or "TRADES"

        tframe = self._sizes[barsize.split()[1]][0]
        self.histfmt[tickerId] = tframe >= TimeFrame.Days
        self.histsend[tickerId] = sessionend
        self.histtz[tickerId] = tz

        EClient.reqHistoricalData(
            self,
            tickerId,
            contract,
            enddate.strftime("%Y%m%d %H:%M:%S") + " GMT",
            duration,
            barsize,
            what,
            int(useRTH),
            2,  # formatDate: 2 for unix time in seconds
            False,  # keepUpToDate
            [],  # chartOptions
        )

        return q

    def cancelHistoricalData(self, q):
        """Cancels an existing HistoricalData request.

        Params:
          - q: the Queue returned by reqHistoricalData
        """
        with self._lock_q:
            EClient.cancelHistoricalData(self, self.ts[q])
            self.cancelQueue(q, True)

    # ---------------------------------------------------------------
    # Real-time bars
    # ---------------------------------------------------------------

    def reqRealTimeBars(self, contract, useRTH=False, duration=5):
        """Creates a request for (5 seconds) Real Time Bars.

        Params:
          - contract: an ibapi.contract.Contract instance
          - useRTH: (default: False) passed to TWS
          - duration: (default: 5) passed to TWS

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        """
        tickerId, q = self.getTickerQueue()

        EClient.reqRealTimeBars(
            self, tickerId, contract, duration, "TRADES", int(useRTH), []
        )

        return q

    def cancelRealTimeBars(self, q):
        """Cancels an existing RealTimeBars subscription.

        Params:
          - q: the Queue returned by reqRealTimeBars
        """
        with self._lock_q:
            tickerId = self.ts.get(q, None)
            if tickerId is not None:
                EClient.cancelRealTimeBars(self, tickerId)

            self.cancelQueue(q, True)

    # ---------------------------------------------------------------
    # Market data
    # ---------------------------------------------------------------

    def reqMktData(self, contract, what=None):
        """Creates a MarketData subscription.

        Params:
          - contract: an ibapi.contract.Contract instance

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        """
        tickerId, q = self.getTickerQueue()
        ticks = "233"  # request RTVOLUME tick delivered over tickString

        if contract.secType in ["CASH", "CFD"]:
            self.iscash[tickerId] = True
            ticks = ""  # cash markets do not get RTVOLUME
            if what == "ASK":
                self.iscash[tickerId] = 2

        EClient.reqMktData(self, tickerId, contract, ticks, False, False, [])
        return q

    def cancelMktData(self, q):
        """Cancels an existing MarketData subscription.

        Params:
          - q: the Queue returned by reqMktData
        """
        with self._lock_q:
            tickerId = self.ts.get(q, None)
            if tickerId is not None:
                EClient.cancelMktData(self, tickerId)

            self.cancelQueue(q, True)

    # ---------------------------------------------------------------
    # Order management
    # ---------------------------------------------------------------

    def cancelOrder(self, orderid):
        """Proxy to cancelOrder."""
        EClient.cancelOrder(self, orderid, OrderCancel())

    def placeOrder(self, orderid, contract, order):
        """Proxy to placeOrder."""
        EClient.placeOrder(self, orderid, contract, order)

    def reqPositions(self):
        """Proxy to reqPositions."""
        EClient.reqPositions(self)

    # ---------------------------------------------------------------
    # Account management
    # ---------------------------------------------------------------

    def reqAccountUpdates(self, subscribe=True, account=None):
        """Proxy to reqAccountUpdates.

        If ``account`` is ``None``, wait for the ``managedAccounts`` message to
        set the account codes.
        """
        if account is None:
            self._event_managed_accounts.wait()
            account = self.managed_accounts[0]

        EClient.reqAccountUpdates(self, subscribe, account)

    def get_acc_values(self, account=None):
        """Returns all account value infos sent by TWS during regular updates.
        Waits for at least one successful download.
        """
        if self.connected():
            self._event_accdownload.wait()

        with self._lock_accupd:
            if account is None:
                if self.connected():
                    self._event_managed_accounts.wait()

                if not self.managed_accounts:
                    return self.acc_upds.copy()

                elif len(self.managed_accounts) > 1:
                    return self.acc_upds.copy()

                account = self.managed_accounts[0]

            try:
                return self.acc_upds[account].copy()
            except KeyError:
                pass

            return self.acc_upds.copy()

    def get_acc_value(self, account=None):
        """Returns the net liquidation value sent by TWS during regular updates.
        Waits for at least one successful download.
        """
        if self.connected():
            self._event_accdownload.wait()

        with self._lock_accupd:
            if account is None:
                if self.connected():
                    self._event_managed_accounts.wait()

                if not self.managed_accounts:
                    return float()

                elif len(self.managed_accounts) > 1:
                    return sum(self.acc_value.values())

                account = self.managed_accounts[0]

            try:
                return self.acc_value[account]
            except KeyError:
                pass

            return float()

    def get_acc_cash(self, account=None):
        """Returns the total cash value sent by TWS during regular updates.
        Waits for at least one successful download.
        """
        if self.connected():
            self._event_accdownload.wait()

        with self._lock_accupd:
            if account is None:
                if self.connected():
                    self._event_managed_accounts.wait()

                if not self.managed_accounts:
                    return float()

                elif len(self.managed_accounts) > 1:
                    return sum(self.acc_cash.values())

                account = self.managed_accounts[0]

            try:
                return self.acc_cash[account]
            except KeyError:
                pass

            return float()

    def getposition(self, contract, clone=False):
        """Get position information for a contract."""
        with self._lock_pos:
            position = self.positions[contract.conId]
            if clone:
                return copy(position)

            return position

    # ---------------------------------------------------------------
    # Contract creation
    # ---------------------------------------------------------------

    def makecontract(self, symbol, sectype, exch, curr,
                     expiry="", strike=0.0, right="", mult=1):
        """Create an IB Contract object from parameters without validation."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sectype
        contract.exchange = exch
        if curr:
            contract.currency = curr
        if sectype in ["FUT", "OPT", "FOP"]:
            contract.lastTradeDateOrContractMonth = expiry
        if sectype in ["OPT", "FOP"]:
            contract.strike = strike
            contract.right = right
        if mult:
            contract.multiplier = str(mult)
        return contract

    # ---------------------------------------------------------------
    # Duration / size helpers
    # ---------------------------------------------------------------

    def getdurations(self, timeframe, compression):
        """Get available durations for a given timeframe and compression."""
        key = (timeframe, compression)
        if key not in self.revdur:
            return []

        return self.revdur[key]

    def getmaxduration(self, timeframe, compression):
        """Get the maximum duration available for a given timeframe and compression."""
        key = (timeframe, compression)
        try:
            return self.revdur[key][-1]
        except (KeyError, IndexError):
            pass

        return None

    def tfcomp_to_size(self, timeframe, compression):
        """Convert timeframe and compression to IB bar size string."""
        if timeframe == TimeFrame.Months:
            return f"{compression} M"

        if timeframe == TimeFrame.Weeks:
            return f"{compression} W"

        if timeframe == TimeFrame.Days:
            if not compression % 7:
                return f"{compression // 7} W"

            return f"{compression} day"

        if timeframe == TimeFrame.Minutes:
            if not compression % 60:
                hours = compression // 60
                return (f"{hours} hour") + ("s" * (hours > 1))

            return (f"{compression} min") + ("s" * (compression > 1))

        if timeframe == TimeFrame.Seconds:
            return f"{compression} secs"

        return None

    def dt_plus_duration(self, dt, duration):
        """Add a duration string to a datetime."""
        size, dim = duration.split()
        size = int(size)
        if dim == "S":
            return dt + timedelta(seconds=size)

        if dim == "D":
            return dt + timedelta(days=size)

        if dim == "W":
            return dt + timedelta(days=size * 7)

        if dim == "M":
            month = dt.month - 1 + size
            years, month = divmod(month, 12)
            return dt.replace(year=dt.year + years, month=month + 1)

        if dim == "Y":
            return dt.replace(year=dt.year + size)

        return dt

    def calcdurations(self, dtbegin, dtend):
        """Calculate a duration in between 2 datetimes."""
        duration = self.histduration(dtbegin, dtend)

        if duration[-1] == "M":
            m = int(duration.split()[0])
            m1 = min(2, m)
            m2 = max(1, m1)
            checkdur = f"{m2} M"
        elif duration[-1] == "Y":
            checkdur = "1 Y"
        else:
            checkdur = duration
        sizes = self._durations[checkdur]
        return duration, sizes

    def calcduration(self, dtbegin, dtend):
        """Calculate a duration in between 2 datetimes. Returns single size."""
        duration, sizes = self.calcdurations(dtbegin, dtend)
        return duration, sizes[0]

    def histduration(self, dt1, dt2):
        """Calculate the smallest possible duration between two datetimes
        according to IB's historical data limitations.
        """
        td = dt2 - dt1

        tsecs = td.total_seconds()
        secs = [60, 120, 180, 300, 600, 900, 1200, 1800, 3600, 7200,
                10800, 14400, 28800]

        idxsec = bisect.bisect_left(secs, tsecs)
        if idxsec < len(secs):
            return f"{secs[idxsec]} S"

        tdextra = bool(td.seconds or td.microseconds)

        days = td.days + tdextra
        if td.days <= 2:
            return f"{days} D"

        weeks, d = divmod(td.days, 7)
        weeks += bool(d or tdextra)
        if weeks <= 2:
            return f"{weeks} W"

        y2, m2, d2 = dt2.year, dt2.month, dt2.day
        y1, m1, d1 = dt1.year, dt1.month, dt2.day

        H2, M2, S2, US2 = dt2.hour, dt2.minute, dt2.second, dt2.microsecond
        H1, M1, S1, US1 = dt1.hour, dt1.minute, dt1.second, dt1.microsecond

        months = (y2 * 12 + m2) - (y1 * 12 + m1) + (
            (d2, H2, M2, S2, US2) > (d1, H1, M1, S1, US1)
        )
        if months <= 1:
            return "1 M"
        elif months <= 11:
            return "2 M"

        return "1 Y"
