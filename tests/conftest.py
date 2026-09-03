"""Choose which node class the replication tests run against.

    pytest                                   # the starter, NodeMultiLeader
    pytest --node MyNode                     # a class exported from dslabs.nodes
    pytest --node dslabs.nodes.my_node:MyNode
    pytest --node A --node B                 # run the suite against each

Tests that take the ``node_class`` fixture are parametrised over every
``--node`` given.
"""

import pytest

from dslabs.nodes import load_node_class


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--node",
        action="append",
        metavar="CLASS",
        help="node class to test: ClassName (from dslabs.nodes) or module:ClassName; repeatable",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "node_class" in metafunc.fixturenames:
        specs = metafunc.config.getoption("node") or ["NodeMultiLeader"]
        classes = [load_node_class(spec) for spec in specs]
        metafunc.parametrize("node_class", classes, ids=[c.__name__ for c in classes])
