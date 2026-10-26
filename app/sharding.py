"""A consistent-hash ring: maps arbitrary string keys onto a fixed set of
named nodes, deterministically, with two properties naive `hash(key) % N`
doesn't have -- the reason to reach for this over the obvious approach:

1. The same key always maps to the same node for a given ring membership,
   which is the actual correctness requirement here: a rate-limited
   identity's counter must always land on the same Redis shard, or its
   count silently splits across shards and the limit stops being enforced
   correctly.
2. Adding or removing a node only remaps ~1/N of keys, not ~(N-1)/N of them
   -- see test_sharding.py for the measured comparison against naive
   modulo hashing, the textbook reason consistent hashing exists at all.

Virtual nodes (many ring positions per physical node) exist purely to fix
naive consistent hashing's real weakness: with one point per node, the
arcs between nodes are wildly uneven sizes, so load distribution is poor.
Scattering each node across many points evens that out.
"""

import bisect
import hashlib


class ConsistentHashRing:
    def __init__(self, nodes: list[str], virtual_nodes: int = 150) -> None:
        if not nodes:
            raise ValueError("a ring needs at least one node")
        self._virtual_nodes = virtual_nodes
        self._ring: dict[int, str] = {}
        self._sorted_hashes: list[int] = []
        for node in nodes:
            self.add_node(node)

    @staticmethod
    def _hash(value: str) -> int:
        return int(hashlib.md5(value.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            h = self._hash(f"{node}#{i}")
            self._ring[h] = node
            bisect.insort(self._sorted_hashes, h)

    def remove_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            h = self._hash(f"{node}#{i}")
            del self._ring[h]
            idx = bisect.bisect_left(self._sorted_hashes, h)
            del self._sorted_hashes[idx]

    def get_node(self, key: str) -> str:
        """The node owning `key`: walk clockwise from key's position on the
        ring to the nearest node point, wrapping around to the first node
        past the ring's end.
        """
        h = self._hash(key)
        idx = bisect.bisect(self._sorted_hashes, h)
        if idx == len(self._sorted_hashes):
            idx = 0
        return self._ring[self._sorted_hashes[idx]]

    @property
    def nodes(self) -> list[str]:
        return sorted(set(self._ring.values()))
