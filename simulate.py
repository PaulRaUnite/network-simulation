import random
from collections import OrderedDict
from typing import Tuple

import networkx as nx
import numpy as np
from matplotlib import pyplot as plt

import framework
from heatmaps import heatmap, annotate_heatmap

net = nx.Graph()
net.add_edges_from((f, t, {"bandwidth": (16 * framework.MEBIBYTE, framework.SECOND)}) for f, t in
                   [(0, 1), (0, 10), (1, 4), (2, 3), (2, 4), (1, 3),
                    (0, 5), (0, 6), (5, 6), (5, 7), (5, 8), (3, 7),
                    (6, 9)])

pos = nx.kamada_kawai_layout(net)
nx.draw(net, pos)
nx.draw_networkx_labels(net, pos, {i: str(i) for i in net.nodes})
nx.draw_networkx_edge_labels(net, pos, {
    (n1, n2): "{}MB/s".format(d["bandwidth"][0] / framework.MEBIBYTE * (framework.SECOND / d["bandwidth"][1]))
    for n1, n2, d in net.edges.data()})
plt.show()


def simulate_and_draw(description: str, networking_type) -> Tuple[float, float, float, float]:
    random.seed(0)
    simulation_time = framework.SECOND
    c = framework.simulate(net, networking_class=networking_type, end_time=simulation_time)

    channels_usage = np.zeros((11, 11), dtype=int)
    for snapshot in c.channel_usage:
        for from_, to, usage in snapshot:
            channels_usage[from_][to] += usage
    channels_usage = channels_usage / simulation_time

    channels_avg_buf = np.zeros((11, 11), dtype=int)
    for snapshot in c.channel_buffers:
        for from_, to, buffered in snapshot:
            channels_avg_buf[from_][to] += buffered
    channels_avg_buf = channels_avg_buf / len(c.channel_buffers)

    fig, axs = plt.subplots(3, 2, figsize=(30, 36))

    im, cbar = heatmap(channels_usage, list(range(11)), list(range(11)), ax=axs[0, 0],
                       cmap="YlGn", cbarlabel="efficiency [percents]")
    texts = annotate_heatmap(im, valfmt="{x:.1f}%")

    im, cbar = heatmap(channels_avg_buf, list(range(11)), list(range(11)), ax=axs[0, 1],
                       cmap="viridis", cbarlabel="avg amount of buffered packets")
    texts = annotate_heatmap(im, valfmt="{x:.1f}")

    nodes_avg_buf = np.zeros(11, dtype=int)
    for snapshot in c.node_buffers:
        for node_ip, buffered in snapshot:
            nodes_avg_buf[node_ip] += buffered
    nodes_avg_buf = nodes_avg_buf / len(c.node_buffers)
    axs[1, 0].bar(list(range(11)), nodes_avg_buf)
    axs[1, 0].set_title("avg buffer size on node")

    messages_statistics = []
    for message_id, finish_time in c.messages_finish.items():
        start_time = c.messages_start[message_id]
        byte_length = c.message_index[message_id]
        in_net_time = finish_time - start_time
        messages_statistics.append((message_id, in_net_time, byte_length, in_net_time / byte_length))
    messages_statistics = np.array(messages_statistics, dtype=int)

    index = np.arange(len(messages_statistics))

    axs[1, 1].bar(index, messages_statistics[..., 1], color="C1")
    axs[1, 1].set_xticks(index)
    axs[1, 1].set_xticklabels(messages_statistics[..., 0])
    axs[1, 1].set(xlabel='messages', ylabel="messages time")
    axs[2, 0].bar(index, messages_statistics[..., 2], color="C2")
    axs[2, 0].set(xlabel='messages', ylabel="messages length")
    axs[2, 0].set_xticks(index)
    axs[2, 0].set_xticklabels(messages_statistics[..., 0])
    axs[2, 1].bar(index, messages_statistics[..., 3], color="C3")
    axs[2, 1].set_xticks(index)
    axs[2, 1].set_xticklabels(messages_statistics[..., 0])
    axs[2, 1].set(xlabel='messages', ylabel="messages time/length ratio")

    fig.suptitle(description)
    plt.show()

    return (
        channels_usage.sum() / len(c.channels),
        channels_avg_buf.sum() / len(c.channels),
        nodes_avg_buf.sum() / len(c.nodes),
        messages_statistics[..., 3].sum() / len(messages_statistics),
    )


def random_strategy(node: framework.Node, packet: framework.PacketEmitEvent) -> framework.Channel:
    if node.table and packet.to in node.table:
        return random.choice(node.table[packet.to])[0]
    else:
        return node.default_channel


class FifoRandom(framework.Networking):
    def _routing_strategy(self, node: framework.Node, packet: framework.PacketEmitEvent) -> framework.Channel:
        return random_strategy(node, packet)


class LifoRandom(framework.Networking):
    def _packet_strategy(self, node: framework.Node) -> framework.PacketEmitEvent:
        return node.queue.pop(-1)

    def _routing_strategy(self, node: framework.Node, packet: framework.PacketEmitEvent) -> framework.Channel:
        return random_strategy(node, packet)


def wrandom_strategy(node: framework.Node, packet: framework.PacketEmitEvent) -> framework.Channel:
    if node.table and packet.to in node.table:
        channels = node.table[packet.to]
        weights = np.array([hops for _, hops in channels], dtype=float)
        weights /= weights.sum()
        return np.random.choice(np.array(channels)[..., 0], 1, p=weights)[0]
    else:
        return node.default_channel


class FifoWeightedRandom(framework.Networking):
    def _routing_strategy(self, node: framework.Node, packet: framework.PacketEmitEvent) -> framework.Channel:
        return wrandom_strategy(node, packet)


class LifoWeightedRandom(framework.Networking):
    def _packet_strategy(self, node: framework.Node) -> framework.PacketEmitEvent:
        return node.queue.pop(-1)

    def _routing_strategy(self, node: framework.Node, packet: framework.PacketEmitEvent) -> framework.Channel:
        return wrandom_strategy(node, packet)


results = OrderedDict()
for descr, net_class in (("FIFO+FIRST", framework.Networking),
                         ("FIFO+RANDOM", FifoRandom),
                         ("LIFO+RANDOM", LifoRandom),
                         ("FIFO+WEIGHT RANDOM", FifoWeightedRandom),
                         ("LIFO+WEIGHT RANDOM", LifoWeightedRandom),
                         ):
    results[descr] = simulate_and_draw(descr, net_class)

names = list(results.keys())
results = np.array(list(results.values()))

fig, ax = plt.subplots(2, 2, figsize=(30, 30))
index = np.arange(len(results))
bar_width = 0.2
opacity = 0.8

comp = ("Avg channel usage, %",
        "Avg channel buffered packets per 50ms",
        "Avg nodes buffered packets per 50ms",
        "Avg time/length message ratio")

for j in range(4):
    x, y = ((0, 0), (0, 1), (1, 0), (1, 1))[j]
    ax[x, y].bar(index, results[..., j], color=["blue", "orange", "green", "red", "purple"], alpha=opacity)
    ax[x, y].set_xticks(index)
    ax[x, y].set_xticklabels(names)
    ax[x, y].set(xlabel='Algorithms', ylabel=comp[j])

fig.suptitle('Algorithms comparison')
plt.show()
