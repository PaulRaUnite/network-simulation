import functools
import itertools
import random
from dataclasses import dataclass, field
from itertools import count, chain
from typing import Dict, NamedTuple, Any, List, Iterable, Optional, Tuple, Type, Set

import networkx

import event_loop as eloop

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
    trait: Set[NetworkTraits]
    table: Dict[IP, List[Tuple["Channel", int]]] = field(default_factory=dict)
    interfaces: List["Channel"] = field(default_factory=list)
    default_channel: Optional["Channel"] = None

    routing_interval: int = 50 * NANOSECOND
    queue: List["PacketEmitEvent"] = field(default_factory=list)


@dataclass
class Channel:
    from_: Node
    to: Node
    bandwidth: float
    used_for: int = 0
    fail: bool = False
    busy: bool = False
    dual: "Channel" = None

    error_ratio: Tuple[int, int] = (0, 1)
    packet_interval: int = 10 * NANOSECOND
    queue: List["PacketEmitEvent"] = field(default_factory=list)

    def __repr__(self):
        return f"Channel({self.from_.ip},{self.to.ip})"

    def lambd(self) -> float:
        return self.error_ratio[0] / self.error_ratio[1]


BroadcastEvent = NamedTuple("BroadcastEvent", (("channel", Channel), ("hops", int), ("ip", IP)))


def broadcast_events(channels, hops, ip):
    for ch in channels:
        yield eloop.NewEvent(BroadcastEvent(ch, hops + 1, ip), 1)


def handle_broadcast(occurred: int, event: BroadcastEvent) -> Iterable[eloop.NewEvent]:
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
    "ChannelErrorEvent",
    (
        ("channel", Channel),
        ("rand", random.Random)
    ),
)

MessageEvent = NamedTuple(
    "MessageEvent",
    (
        ("from_", IP),
        ("to", IP),
        ("byte_length", int),
        ("rand", random.Random)
    )
)
PacketEmitEvent = NamedTuple(
    "PacketEmitEvent",
    (
        ("from_", IP),
        ("to", IP),
        ("byte_length", int),
        ("message_id", int),
        ("number", int),
        ("parts", int),
    )
)
PacketTransitEvent = NamedTuple(
    "PacketTransitEvent",
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

RoutingEvent = NamedTuple(
    "PacketQueueEvent",
    (
        ("node", Node),
        ("interval", int),
    )
)


def send_packet(ch: Channel, packet: PacketEmitEvent) -> eloop.NewEvent:
    interval = int(ch.bandwidth * packet.byte_length) + ch.packet_interval if ch.busy else 0
    ch.busy = True
    ch.used_for = interval
    return eloop.NewEvent(PacketTransitEvent(ch, *packet), interval)


def remit_packet(computer: Node, packet: PacketEmitEvent) -> Optional[eloop.NewEvent]:
    events = []
    if not computer.queue:
        interval = computer.routing_interval
        events.append(eloop.NewEvent(RoutingEvent(computer, interval), interval))
    computer.queue.append(packet)
    return events


class Networking:
    def __init__(
            self, nodes: List[Node],
            channels: List[Channel],
            message_ratio: Tuple[int, int],
            emit_ratio: Tuple[int, int],
            rand: random.Random,
            stop_world_period: Optional[int] = None
    ):
        self.nodes: Dict[IP, Node] = {node.ip: node for node in nodes}
        self.channels = channels
        self.message_period: float = message_ratio[0] / message_ratio[1]
        self.stop_world_period = stop_world_period
        self.emit_period: float = emit_ratio[0] / emit_ratio[1]
        self.rand = rand

        self.conn_ip_candidates = [
            (node1.ip, node2.ip) for node1, node2 in
            itertools.product(
                (node for node in nodes if NetworkTraits.CLIENT in node.trait),
                (node for node in nodes if NetworkTraits.SERVER in node.trait),
            ) if node1 is not node2
        ]
        self.message_index = count()
        self.messages_start = {}
        self.messages_finish = {}

    def _generate_message(self, rand: random.Random):
        return eloop.NewEvent(
            MessageEvent(
                *rand.choice(self.conn_ip_candidates),
                rand.randint(128, 2 ** 20),
                rand,
            ),
            int(rand.expovariate(self.message_period)),
        )

    def _generate_error(self, channel: Channel, rand: random.Random) -> Optional[eloop.NewEvent]:
        if channel.error_ratio[0] == 0:
            return
        return eloop.NewEvent(
            ChannelErrorEvent(channel, rand),
            int(rand.expovariate(channel.lambd()))
        )

    def initial_events(self) -> List[eloop.NewEvent]:
        next_int = functools.partial(self.rand.randint, 0, 1000)
        events = [
            self._generate_message(random.Random(next_int()))
        ]
        for ch in self.channels:
            event = self._generate_error(ch, random.Random(next_int()))
            if event:
                events.append(event)
        if self.stop_world_period:
            events.append(eloop.NewEvent(StopWorldEvent(), self.stop_world_period))
        return events

    def stop_world(self, _, __) -> List[eloop.NewEvent]:
        print("simulation stops, collecting statistics...")
        return [eloop.NewEvent(StopWorldEvent(), self.stop_world_period)]

    def channel_error(self, _, event: ChannelErrorEvent) -> List[eloop.NewEvent]:
        event.channel.fail = True
        return [self._generate_error(*event)]

    def message(self, occurred: int, event: MessageEvent) -> List[eloop.NewEvent]:
        message_id = next(self.message_index)
        whole, tail = event.byte_length // 1024, event.byte_length % 1024
        parts = itertools.chain(itertools.repeat(1024, times=whole), (tail,) if tail else ())
        parts_amount = whole + int(bool(tail))
        rand = random.Random(event.rand.randint(0, 1000))

        self.messages_start[message_id] = occurred
        events = [
            eloop.NewEvent(
                PacketEmitEvent(event.from_, event.to, part, message_id, i, parts_amount),
                int(rand.expovariate(self.emit_period)),
            )
            for i, part in enumerate(parts)
        ]
        events.append(self._generate_message(event.rand))
        return events

    def emit_package(self, _, event: PacketEmitEvent) -> List[eloop.NewEvent]:
        computer = self.nodes[event.from_]
        return remit_packet(computer, event)

    def transit_package(self, _, event: PacketTransitEvent) -> List[eloop.NewEvent]:
        ch = event.channel
        if ch.fail:
            ch.fail = False
            return [send_packet(ch, PacketEmitEvent(*event[1:]))]

        events = remit_packet(ch.to, PacketEmitEvent(*event[1:]))
        if ch.queue:
            events.append(send_packet(ch, ch.queue.pop()))
        else:
            ch.busy = False
        return events

    def _packet_strategy(self, node: Node) -> PacketEmitEvent:
        return node.queue.pop(0)

    def _routing_strategy(self, node: Node, packet: PacketEmitEvent) -> Channel:
        if node.table and packet.to in node.table:
            channels = node.table[packet.to]
            return channels[0][0]
        else:
            return node.default_channel

    def routing(self, occurred: int, event: RoutingEvent) -> List[eloop.NewEvent]:
        node = event.node
        events = []

        packet = self._packet_strategy(node)
        if packet.to == node.ip:
            if packet.number == packet.parts - 1:
                self.messages_finish[packet.message_id] = occurred
            return []

        ch = self._routing_strategy(node, packet)
        if ch.busy:
            ch.queue.append(packet)
        else:
            events.append(send_packet(ch, packet))

        if node.queue:
            events.append(eloop.NewEvent(event, event.interval))

        return events


def simulate(
        network: networkx.Graph,
        networking_class: Type[Networking] = Networking,
        message_ratio=(1, 5 * MILLISECOND),
        stop_world_period=50 * MILLISECOND,
        end_time: Optional[int] = None,
) -> Networking:
    nodes = {}
    node_count = count()
    for identifier, d in sorted(network.nodes.data(), key=lambda x: x[0]):
        nodes[identifier] = Node(
            identifier=identifier,
            ip=next(node_count),
            trait=d.get("trait", frozenset((NetworkTraits.CLIENT, NetworkTraits.SERVER, NetworkTraits.ROUTER)))
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
    eloop.EventLoop(
        initial_events=list(chain.from_iterable(
            broadcast_events(node.interfaces, 0, node.ip)
            for node in nodes.values()
        )),
        handlers={BroadcastEvent: handle_broadcast}
    ).run()

    controller = networking_class(
        nodes=list(nodes.values()),
        channels=channels,
        message_ratio=message_ratio,
        stop_world_period=stop_world_period,
        rand=random.Random(0),
        emit_ratio=(1, MILLISECOND)
    )
    handlers = {
        StopWorldEvent: controller.stop_world,
        ChannelErrorEvent: controller.channel_error,
        MessageEvent: controller.message,
        PacketEmitEvent: controller.emit_package,
        PacketTransitEvent: controller.transit_package,
        RoutingEvent: controller.routing
    }

    eloop.EventLoop(
        initial_events=controller.initial_events(),
        handlers=handlers,
        end_time=end_time,
    ).run()

    return controller
