from dataclasses import dataclass, field
from itertools import count, chain
from typing import Dict, NamedTuple, Callable, Any, List, Iterable, Optional, Tuple

import networkx
from sortedcontainers import SortedDict

NewEvent = NamedTuple("NewEvent", (("event", Any), ("time_shift", int)))

NANOSECOND = 1
MICROSECOND = 1000 * NANOSECOND
MILLISECOND = 1000 * MICROSECOND
SECOND = 1000 * MILLISECOND
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR


class Timeline:
    def __init__(self):
        self._time_cells = SortedDict()

    def add(self, event: Any, occurring: int):
        if occurring in self._time_cells:
            self._time_cells[occurring].append(event)
        else:
            self._time_cells[occurring] = [event]

    def __iter__(self):
        while self._time_cells:
            occurring, event_list = self._time_cells.popitem(0)
            for event in event_list:
                yield occurring, event


class EventLoop:
    def __init__(
            self,
            initial_events: Iterable[NewEvent],
            handlers: Dict[type, Callable[[Any], Iterable[NewEvent]]],
            end_time: Optional[int] = None,
    ):
        self.timeline = Timeline()
        self.handlers = handlers
        self.end_time = end_time

        for event, occurring in initial_events:
            self.timeline.add(event, occurring)

    def handle_event(self, occurring: int, event: Any):
        new_events = self.handlers[type(event)](event)
        for event, time_shift in new_events:
            self.timeline.add(event, occurring + time_shift)

    def run(self):
        for occurring, event in self.timeline:
            if self.end_time and occurring < self.end_time:
                break
            self.handle_event(occurring, event)


IP = int


class NetworkTraits:
    CLIENT = 0
    SERVER = 1
    ROUTER = 2


@dataclass
class Node:
    identifier: Any
    ip: IP
    trait: NetworkTraits
    table: Dict[IP, List[Tuple["Channel", int]]] = field(default_factory=dict)
    interfaces: List["Channel"] = field(default_factory=list)
    default_channel: Optional["Channel"] = None


@dataclass
class Channel:
    from_: Node
    to: Node
    bandwidth: float
    failed: bool = False
    used_for: int = 0
    dual: Optional["Channel"] = None

    def __repr__(self):
        return f"Channel({self.from_.ip},{self.to.ip})"


BroadcastEvent = NamedTuple("BroadcastEvent", (("channel", Channel), ("hops", int), ("ip", IP)))


def broadcast_events(channels, hops, ip):
    for ch in channels:
        yield NewEvent(BroadcastEvent(ch, hops + 1, ip), 1)


def handle_broadcast(event: BroadcastEvent) -> Iterable[NewEvent]:
    ch, hops, ip = event
    node = ch.to
    if len(node.interfaces) == 1:
        if node.interfaces[0] != ch.dual:
            raise Exception()
        return []

    if ip in node.table:
        channels = node.table[ip]
        if all(other_ch is not ch.dual for other_ch, other_hops in enumerate(channels)):
            channels.append((ch.dual, hops))
        return []

    node.table[ip] = [(ch.dual, hops)]
    return broadcast_events(
        (interface for interface in ch.to.interfaces if interface is not ch.dual),
        hops,
        ip
    )


def simulate(
        network: networkx.Graph,
        initial_events: Iterable[NewEvent],
        handlers: Dict[int, Callable[[Any], Iterable[NewEvent]]],
        end_time: Optional[int] = None,
):
    nodes = {}
    node_count = count()
    for identifier, d in network.nodes.data():
        nodes[identifier] = Node(
            identifier=identifier,
            ip=next(node_count),
            trait=d.get("trait", (NetworkTraits.CLIENT, NetworkTraits.SERVER, NetworkTraits.ROUTER))
        )

    for node1, node2, data in network.edges.data():
        node1 = nodes[node1]
        node2 = nodes[node2]

        c1 = Channel(node1, node2, data["bandwidth"])
        c2 = Channel(node2, node1, data["bandwidth"])

        c1.dual = c2
        c2.dual = c1

        node1.interfaces.append(c1)
        node2.interfaces.append(c2)

    for _, node in nodes.items():
        node.default_channel = node.interfaces[0]

    # building routing tables
    EventLoop(
        initial_events=list(chain.from_iterable(
            broadcast_events(node.interfaces, 0, node.ip)
            for node in nodes.values()
        )),
        handlers={BroadcastEvent: handle_broadcast}
    ).run()

    basic_handlers = {

    }
    basic_handlers.update(handlers)

    EventLoop(
        initial_events=initial_events,
        handlers=basic_handlers,
        end_time=end_time,
    ).run()
