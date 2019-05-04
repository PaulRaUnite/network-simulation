from dataclasses import dataclass, field
from itertools import count, chain
from typing import Dict, NamedTuple, Any, List, Iterable, Optional, Tuple, Type

import networkx

import event_loop

NANOSECOND = 1
MICROSECOND = 1000 * NANOSECOND
MILLISECOND = 1000 * MICROSECOND
SECOND = 1000 * MILLISECOND
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR

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

    message_queue: List["PackageEmitEvent"] = field(default=list)


@dataclass
class Channel:
    from_: Node
    to: Node
    bandwidth: float
    failed: bool = False
    used_for: int = 0
    dual: "Channel" = None

    def __repr__(self):
        return f"Channel({self.from_.ip},{self.to.ip})"

    def clear(self):
        self.failed = False
        self.used_for = 0


BroadcastEvent = NamedTuple("BroadcastEvent", (("channel", Channel), ("hops", int), ("ip", IP)))


def broadcast_events(channels, hops, ip):
    for ch in channels:
        yield event_loop.NewEvent(BroadcastEvent(ch, hops + 1, ip), 1)


def handle_broadcast(event: BroadcastEvent) -> Iterable[event_loop.NewEvent]:
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


StopWorldEvent = NamedTuple("StopWorldEvent", ())
ChannelErrorEvent = NamedTuple(
    "ConnectionEvent",
    (("channel", Channel),),
)

MessageEvent = NamedTuple(
    "SendMessageEvent",
    (
        ("from_", IP),
        ("to", IP),
        ("byte_length", int)
    )
)
PackageEmitEvent = NamedTuple(
    "PackageEmitEvent",
    (
        ("from_", IP),
        ("to", IP),
        ("byte_length", int),
        ("message_id", int),
        ("number", int),
        ("parts", int),
    )
)
PackageTransitEvent = NamedTuple(
    "FrameSendingEvent",
    (
        ("channel", Channel),
        ("from_", IP),
        ("to", IP),
        ("byte_length", int),
        ("message_id", int),
        ("number", int),
        ("parts", int),
    )
)


@dataclass
class Networking:
    nodes: List[Node]
    channels: List[Channel]
    message_period: Tuple[int, int]
    stop_world_period: Optional[int] = None

    def initial_events(self) -> List[event_loop.NewEvent]:
        return [
            event_loop.NewEvent(StopWorldEvent(), self.stop_world_period),
        ]

    def stop_world(self, _) -> List[event_loop.NewEvent]:
        print("world stops, collecting statistics...")
        return [event_loop.NewEvent(StopWorldEvent(), self.stop_world_period)]

    def channel_error(self, event: ChannelErrorEvent) -> List[event_loop.NewEvent]:
        pass

    def message(self, event: MessageEvent) -> List[event_loop.NewEvent]:
        pass

    def emit_package(self, event: PackageEmitEvent) -> List[event_loop.NewEvent]:
        pass

    def transit_package(self, event: PackageTransitEvent) -> List[event_loop.NewEvent]:
        pass


def simulate(
        network: networkx.Graph,
        networking_class: Type[Networking] = Networking,
        message_period=(1, 5 * MILLISECOND),
        stop_world_period=50 * MILLISECOND,
        end_time: Optional[int] = None,
) -> Networking:
    nodes = {}
    node_count = count()
    for identifier, d in sorted(network.nodes.data(), key=lambda x: x[0]):
        nodes[identifier] = Node(
            identifier=identifier,
            ip=next(node_count),
            trait=d.get("trait", (NetworkTraits.CLIENT, NetworkTraits.SERVER, NetworkTraits.ROUTER))
        )

    channels = []
    for node1, node2, data in network.edges.data():
        node1 = nodes[node1]
        node2 = nodes[node2]

        c1 = Channel(node1, node2, data["bandwidth"])
        c2 = Channel(node2, node1, data["bandwidth"])

        c1.dual = c2
        c2.dual = c1

        node1.interfaces.append(c1)
        node2.interfaces.append(c2)

        channels.extend((c1, c2))

    for _, node in nodes.items():
        node.default_channel = node.interfaces[0]

    # building routing tables
    event_loop.EventLoop(
        initial_events=list(chain.from_iterable(
            broadcast_events(node.interfaces, 0, node.ip)
            for node in nodes.values()
        )),
        handlers={BroadcastEvent: handle_broadcast}
    ).run()

    controller = networking_class(
        nodes=nodes,
        channels=channels,
        message_period=message_period,
        stop_world_period=stop_world_period,
    )
    handlers = {
        StopWorldEvent: controller.stop_world,
        ChannelErrorEvent: controller.channel_error,
        MessageEvent: controller.message,
        PackageEmitEvent: controller.emit_package,
        PackageTransitEvent: controller.transit_package,
    }

    event_loop.EventLoop(
        initial_events=controller.initial_events(),
        handlers=handlers,
        end_time=end_time,
    ).run()

    return controller
