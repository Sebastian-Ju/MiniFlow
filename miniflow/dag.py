from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence


class InvalidDAGError(ValueError):
    """Raised when a workflow contains missing dependencies or a cycle."""


def topological_order(graph: Mapping[str, Sequence[str]]) -> list[str]:
    """Return dependency-first node order using Kahn's algorithm."""
    node_names = set(graph)
    for node, dependencies in graph.items():
        unknown = set(dependencies) - node_names
        if unknown:
            raise InvalidDAGError(f"Node '{node}' depends on unknown node '{min(unknown)}'")

    indegree = {node: len(set(dependencies)) for node, dependencies in graph.items()}
    children: dict[str, list[str]] = {node: [] for node in graph}
    for node, dependencies in graph.items():
        for dependency in set(dependencies):
            children[dependency].append(node)

    ready = deque(sorted(node for node, degree in indegree.items() if degree == 0))
    order: list[str] = []
    while ready:
        node = ready.popleft()
        order.append(node)
        for child in sorted(children[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)

    if len(order) != len(graph):
        cyclic = sorted(node for node, degree in indegree.items() if degree > 0)
        nodes = ", ".join(cyclic)
        raise InvalidDAGError(f"Workflow contains a dependency cycle involving: {nodes}")
    return order
