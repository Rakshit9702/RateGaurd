import pytest

from app.sharding import ConsistentHashRing


def _sample_keys(n: int) -> list[str]:
    return [f"user-{i}" for i in range(n)]


def test_same_key_always_maps_to_the_same_node() -> None:
    ring = ConsistentHashRing(["shard-a", "shard-b", "shard-c"])
    key = "rate-limit-key-42"

    assert all(ring.get_node(key) == ring.get_node(key) for _ in range(50))


def test_two_independent_rings_with_the_same_membership_agree() -> None:
    """The actual correctness property this exists for: every app instance
    builds its own ring, independently, from the same config -- they must
    all route a given key to the same shard, or its counter splits.
    """
    nodes = ["shard-a", "shard-b", "shard-c"]
    ring_1 = ConsistentHashRing(nodes)
    ring_2 = ConsistentHashRing(nodes)

    keys = _sample_keys(500)
    assert [ring_1.get_node(k) for k in keys] == [ring_2.get_node(k) for k in keys]


def test_distribution_is_reasonably_even_across_nodes() -> None:
    nodes = ["shard-a", "shard-b", "shard-c", "shard-d"]
    ring = ConsistentHashRing(nodes)
    keys = _sample_keys(10_000)

    counts = {node: 0 for node in nodes}
    for key in keys:
        counts[ring.get_node(key)] += 1

    ideal = len(keys) / len(nodes)
    # Consistent hashing with virtual nodes isn't perfectly even -- assert
    # within 25% of the ideal share per node, not exact equality.
    for node, count in counts.items():
        assert abs(count - ideal) / ideal < 0.25, f"{node} got {count}, ideal {ideal}"


def test_consistent_hashing_remaps_far_fewer_keys_than_naive_modulo_on_node_add() -> None:
    """The textbook reason consistent hashing exists. Measured, not asserted."""
    keys = _sample_keys(10_000)

    def naive_mod_node(key: str, n: int) -> int:
        return hash(key) % n

    before_naive = [naive_mod_node(k, 3) for k in keys]
    after_naive = [naive_mod_node(k, 4) for k in keys]
    naive_remapped = sum(1 for b, a in zip(before_naive, after_naive, strict=True) if b != a)

    ring_before = ConsistentHashRing(["shard-a", "shard-b", "shard-c"])
    before_consistent = [ring_before.get_node(k) for k in keys]
    ring_before.add_node("shard-d")
    after_consistent = [ring_before.get_node(k) for k in keys]
    consistent_remapped = sum(1 for b, a in zip(before_consistent, after_consistent, strict=True) if b != a)

    naive_pct = 100 * naive_remapped / len(keys)
    consistent_pct = 100 * consistent_remapped / len(keys)
    print(f"\nnaive modulo remapped {naive_pct:.1f}% of keys; consistent hashing remapped {consistent_pct:.1f}%")

    # Naive modulo remaps the large majority of keys on almost any N->N+1
    # change; consistent hashing should remap close to the theoretical
    # ideal of 1/(N+1) =~ 25%, and in any case dramatically less than naive.
    assert naive_remapped > len(keys) * 0.5
    assert consistent_remapped < len(keys) * 0.35
    assert consistent_remapped < naive_remapped


def test_remove_node_only_remaps_that_nodes_keys() -> None:
    nodes = ["shard-a", "shard-b", "shard-c"]
    ring = ConsistentHashRing(nodes)
    keys = _sample_keys(2_000)

    before = {k: ring.get_node(k) for k in keys}
    ring.remove_node("shard-b")
    after = {k: ring.get_node(k) for k in keys}

    for key in keys:
        if before[key] == "shard-b":
            assert after[key] != "shard-b"
        else:
            # A key that wasn't on the removed node must stay exactly
            # where it was -- removal shouldn't perturb unrelated keys.
            assert after[key] == before[key]


def test_empty_ring_rejected() -> None:
    with pytest.raises(ValueError):
        ConsistentHashRing([])
