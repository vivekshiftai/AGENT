"""Registry for tracking node execution start times and completion status for accurate timing calculations."""
from typing import Dict, Optional, Set
import threading
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Thread-local storage for node timing
_thread_local = threading.local()


def get_node_timing_registry() -> Optional['NodeTimingRegistry']:
    """Get the node timing registry for the current thread/query."""
    return getattr(_thread_local, 'node_timing_registry', None)


def set_node_timing_registry(registry: 'NodeTimingRegistry') -> None:
    """Set the node timing registry for the current thread/query."""
    _thread_local.node_timing_registry = registry


def clear_node_timing_registry() -> None:
    """Clear the node timing registry for the current thread/query."""
    if hasattr(_thread_local, 'node_timing_registry'):
        delattr(_thread_local, 'node_timing_registry')


class NodeTimingRegistry:
    """
    Registry to track actual node start times and completion status for accurate duration calculations.
    
    Nodes record their start time at the very beginning of execution,
    and the WebSocket manager uses these actual start times to calculate
    accurate durations instead of estimates.
    
    Also tracks which nodes have completed to prevent duplicate execution
    in LangGraph parallel execution scenarios.
    """
    
    def __init__(self, query_id: str):
        """Initialize registry for a specific query."""
        self.query_id = query_id
        self._node_starts: Dict[str, datetime] = {}
        self._node_completions: Dict[str, datetime] = {}
        self._nodes_in_progress: Set[str] = set()
        self._lock = threading.Lock()
    
    def record_node_start(self, node_name: str, start_time: datetime) -> None:
        """
        Record when a node actually started executing.
        
        This should be called at the very beginning of each node function,
        before any actual work begins.
        """
        with self._lock:
            self._node_starts[node_name] = start_time
            self._nodes_in_progress.add(node_name)
            logger.debug(f"[NodeTimingRegistry] Recorded start for {node_name}: {start_time.isoformat()}")
    
    def record_node_completion(self, node_name: str, end_time: datetime = None) -> None:
        """
        Record when a node completed executing.
        
        This should be called at the end of each node function,
        after all work is done.
        """
        if end_time is None:
            end_time = datetime.now()
        with self._lock:
            self._node_completions[node_name] = end_time
            self._nodes_in_progress.discard(node_name)
            logger.debug(f"[NodeTimingRegistry] Recorded completion for {node_name}: {end_time.isoformat()}")
    
    def is_node_completed(self, node_name: str) -> bool:
        """Check if a node has already completed execution."""
        with self._lock:
            return node_name in self._node_completions
    
    def is_node_in_progress(self, node_name: str) -> bool:
        """Check if a node is currently executing (started but not completed)."""
        with self._lock:
            return node_name in self._nodes_in_progress
    
    def try_start_node(self, node_name: str, start_time: datetime = None) -> bool:
        """
        Atomically check if a node can start and mark it as started.
        
        Returns True if the node can proceed (not completed and not in progress).
        Returns False if the node should skip execution (already completed or in progress).
        
        This is the preferred method for preventing duplicate execution in parallel scenarios.
        """
        if start_time is None:
            start_time = datetime.now()
        with self._lock:
            # If already completed, don't start again
            if node_name in self._node_completions:
                logger.debug(f"[NodeTimingRegistry] Node {node_name} already completed - skipping")
                return False
            # If already in progress, don't start again (prevents race conditions)
            if node_name in self._nodes_in_progress:
                logger.debug(f"[NodeTimingRegistry] Node {node_name} already in progress - skipping")
                return False
            # Mark as started
            self._node_starts[node_name] = start_time
            self._nodes_in_progress.add(node_name)
            logger.debug(f"[NodeTimingRegistry] Node {node_name} started: {start_time.isoformat()}")
            return True
    
    def get_node_start(self, node_name: str) -> Optional[datetime]:
        """Get the recorded start time for a node."""
        with self._lock:
            return self._node_starts.get(node_name)
    
    def get_node_completion(self, node_name: str) -> Optional[datetime]:
        """Get the recorded completion time for a node."""
        with self._lock:
            return self._node_completions.get(node_name)
    
    def get_all_starts(self) -> Dict[str, datetime]:
        """Get all recorded start times."""
        with self._lock:
            return self._node_starts.copy()
    
    def get_all_completions(self) -> Dict[str, datetime]:
        """Get all recorded completion times."""
        with self._lock:
            return self._node_completions.copy()

