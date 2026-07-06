"""Built-in node types. Importing this package registers them all."""
from .base import Node, NodeContext, NodeInputs, NodeOutputs, Records
from .registry import NODE_REGISTRY, get_node_class, register_node

from . import api_source  # noqa: F401  (registers api_source)
from . import iterator  # noqa: F401  (registers iterator)
from . import merge  # noqa: F401  (registers merge)
from . import transform  # noqa: F401  (registers transform)

__all__ = [
    "NODE_REGISTRY",
    "Node",
    "NodeContext",
    "NodeInputs",
    "NodeOutputs",
    "Records",
    "get_node_class",
    "register_node",
]
