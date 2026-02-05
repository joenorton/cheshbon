"""Build dependency graph of derived outputs."""

from typing import Dict, Set, List
from collections import defaultdict
from .spec import MappingSpec


class KernelValidationError(Exception):
    """Base exception for kernel validation errors."""
    pass


class MissingDependenciesError(KernelValidationError):
    """Raised when dependencies are referenced but not defined."""
    def __init__(self, missing: set[str]):
        self.missing = missing
        missing_str = ", ".join(sorted(missing))
        super().__init__(f"Dependencies referenced but not defined: {missing_str}")


class CycleDetectedError(KernelValidationError):
    """Raised when a cycle is detected in the dependency graph."""
    def __init__(self, cycle: list[str], edge_types: list[str] | None = None):
        self.cycle = cycle
        self.edge_types = edge_types or []
        # Format cycle for message (remove duplicate final node)
        if len(cycle) > 1 and cycle[0] == cycle[-1]:
            cycle_ids = cycle[:-1]
        else:
            cycle_ids = cycle
        cycle_id_str = " -> ".join(cycle_ids) + f" -> {cycle_ids[0]}"
        edge_types_str = ", ".join(sorted(set(self.edge_types))) if self.edge_types else ""
        msg = f"Cycle detected in dependency graph:\n  Cycle: {cycle_id_str}"
        if edge_types_str:
            msg += f"\n  Edge types: {edge_types_str}"
        super().__init__(msg)


class DependencyGraph:
    """Dependency graph for mapping spec."""
    
    def __init__(self, spec: MappingSpec):
        self.spec = spec
        self.nodes: Set[str] = set()
        self.edges: Dict[str, Set[str]] = defaultdict(set)  # node -> set of dependencies
        self.reverse_edges: Dict[str, Set[str]] = defaultdict(set)  # dependency -> set of nodes that depend on it
        self._build()
    
    def _build(self):
        """Build the dependency graph from the spec using stable IDs."""
        # Build ID to name mapping for sources
        source_id_to_name = {s.id: s.name for s in self.spec.sources}
        derived_id_to_name = {d.id: d.name for d in self.spec.derived}
        
        # Add all source columns as nodes (using IDs)
        for source in self.spec.sources:
            self.nodes.add(source.id)
            self.edges[source.id] = set()  # Sources have no dependencies
        
        # Add all derived variables as nodes (using IDs)
        for derived in self.spec.derived:
            self.nodes.add(derived.id)
            dependencies = set()
            
            # Parse inputs (now using IDs directly: s:xxx, d:xxx, or c:xxx)
            for inp_id in derived.inputs:
                if inp_id.startswith("s:"):
                    dependencies.add(inp_id)
                    self.reverse_edges[inp_id].add(derived.id)
                elif inp_id.startswith(("d:", "v:")):
                    dependencies.add(inp_id)
                    self.reverse_edges[inp_id].add(derived.id)
                elif inp_id.startswith("c:"):
                    dependencies.add(inp_id)
                    self.reverse_edges[inp_id].add(derived.id)
            
            self.edges[derived.id] = dependencies
        
        # Add all constraint nodes as nodes (using IDs)
        # Constraints are first-class graph nodes with boolean outputs
        for constraint in (self.spec.constraints or []):
            self.nodes.add(constraint.id)
            dependencies = set()
            
            # Parse inputs (constraints can depend on sources, derived vars, or other constraints)
            for inp_id in constraint.inputs:
                if inp_id.startswith("s:"):
                    dependencies.add(inp_id)
                    self.reverse_edges[inp_id].add(constraint.id)
                elif inp_id.startswith(("d:", "v:")):
                    dependencies.add(inp_id)
                    self.reverse_edges[inp_id].add(constraint.id)
                elif inp_id.startswith("c:"):
                    dependencies.add(inp_id)
                    self.reverse_edges[inp_id].add(constraint.id)
            
            self.edges[constraint.id] = dependencies
        
        # Ensure all referenced dependencies exist as nodes
        all_deps = set()
        for deps in self.edges.values():
            all_deps.update(deps)
        
        missing = all_deps - self.nodes
        if missing:
            raise MissingDependenciesError(missing)
        
        # Detect cycles (critical for constraints: a constraint depending on a derived var
        # that itself depends on constraint outcome creates a cycle)
        cycles = self._detect_cycles()
        if cycles:
            # Get the minimal cycle (first one found, which is minimal by DFS)
            cycle = cycles[0]
            # Extract edge types as list
            if len(cycle) > 1 and cycle[0] == cycle[-1]:
                cycle_nodes = cycle[:-1]
            else:
                cycle_nodes = cycle
            edge_type_list = []
            for i in range(len(cycle_nodes)):
                from_node = cycle_nodes[i]
                to_node = cycle_nodes[(i + 1) % len(cycle_nodes)]
                from_type = "source" if from_node.startswith("s:") else \
                           "derived" if from_node.startswith(("d:", "v:")) else \
                           "constraint" if from_node.startswith("c:") else "unknown"
                to_type = "source" if to_node.startswith("s:") else \
                         "derived" if to_node.startswith(("d:", "v:")) else \
                         "constraint" if to_node.startswith("c:") else "unknown"
                edge_type_list.append(f"{from_type}->{to_type}")
            
            # Remove duplicate final node for cycle path
            if len(cycle) > 1 and cycle[0] == cycle[-1]:
                cycle_path = cycle[:-1]
            else:
                cycle_path = cycle
            
            raise CycleDetectedError(cycle_path, edge_type_list)
    
    def _detect_cycles(self) -> List[List[str]]:
        """Detect cycles in the dependency graph using DFS.
        
        Returns:
            List of cycles found (each cycle is a list of node IDs, minimal cycle first).
            Empty list if no cycles exist.
        """
        cycles = []
        WHITE = 0  # Unvisited
        GRAY = 1   # Currently being visited (in recursion stack)
        BLACK = 2  # Fully visited
        
        color = {node: WHITE for node in self.nodes}
        parent = {}
        
        def dfs(node: str, path: List[str]) -> None:
            """DFS to detect cycles."""
            color[node] = GRAY
            path.append(node)
            
            for dependent in sorted(self.get_dependents(node)):  # Sort for deterministic order
                if color[dependent] == WHITE:
                    parent[dependent] = node
                    dfs(dependent, path)
                elif color[dependent] == GRAY:
                    # Cycle detected: find the minimal cycle path
                    cycle_start = path.index(dependent)
                    # Cycle is from dependent back to dependent (don't duplicate the start node)
                    cycle = path[cycle_start:] + [dependent]
                    # Only add if we haven't seen this cycle (avoid duplicates)
                    # Normalize cycle to start at lexicographically smallest node for comparison
                    normalized_cycle = self._normalize_cycle(cycle)
                    if normalized_cycle not in cycles:
                        cycles.append(cycle)  # Store original cycle, not normalized
                        # Return early after first cycle found (minimal by DFS)
                        return
            
            color[node] = BLACK
            path.pop()
        
        # Check all nodes (handles disconnected components)
        for node in sorted(self.nodes):  # Sort for deterministic order
            if color[node] == WHITE:
                dfs(node, [])
                # Stop after first cycle found (minimal)
                if cycles:
                    break
        
        return cycles
    
    def _format_cycle_with_names(self, cycle: List[str]) -> str:
        """Format cycle path with both IDs and names for diagnostic clarity.
        
        Removes duplicate final node (cycle closes back to start).
        """
        # Remove duplicate final node if present (cycle: [A, B, A] -> format as A -> B -> A)
        if len(cycle) > 1 and cycle[0] == cycle[-1]:
            cycle_nodes = cycle[:-1]  # Remove duplicate
        else:
            cycle_nodes = cycle
        
        path_parts = []
        for node_id in cycle_nodes:
            # Get name from spec
            if node_id.startswith("s:"):
                source = self.spec.get_source_by_id(node_id)
                name = source.name if source else node_id
                path_parts.append(f"{name} ({node_id})")
            elif node_id.startswith(("d:", "v:")):
                derived = self.spec.get_derived_by_id(node_id)
                name = derived.name if derived else node_id
                path_parts.append(f"{name} ({node_id})")
            elif node_id.startswith("c:"):
                constraint = self.spec.get_constraint_by_id(node_id)
                name = constraint.name if constraint else node_id
                path_parts.append(f"{name} ({node_id})")
            else:
                path_parts.append(node_id)
        # Close the cycle
        return " -> ".join(path_parts) + f" -> {path_parts[0]}"
    
    def _normalize_cycle(self, cycle: List[str]) -> List[str]:
        """Normalize cycle to start at lexicographically smallest node for comparison.
        
        This allows us to detect duplicate cycles regardless of starting point.
        """
        if not cycle:
            return cycle
        # Find the smallest node ID
        min_idx = min(range(len(cycle) - 1), key=lambda i: cycle[i])  # Exclude last (duplicate)
        # Rotate cycle to start at smallest node
        normalized = cycle[min_idx:-1] + cycle[:min_idx] + [cycle[min_idx]]
        return normalized
    
    def _get_cycle_edge_types(self, cycle: List[str]) -> str:
        """Get edge types involved in the cycle for diagnostic clarity."""
        # Remove duplicate final node if present
        if len(cycle) > 1 and cycle[0] == cycle[-1]:
            cycle_nodes = cycle[:-1]
        else:
            cycle_nodes = cycle
        
        edge_types = []
        for i in range(len(cycle_nodes)):
            from_node = cycle_nodes[i]
            to_node = cycle_nodes[(i + 1) % len(cycle_nodes)]  # Wrap around for cycle
            
            from_type = "source" if from_node.startswith("s:") else \
                       "derived" if from_node.startswith(("d:", "v:")) else \
                       "constraint" if from_node.startswith("c:") else "unknown"
            to_type = "source" if to_node.startswith("s:") else \
                     "derived" if to_node.startswith(("d:", "v:")) else \
                     "constraint" if to_node.startswith("c:") else "unknown"
            
            edge_types.append(f"{from_type}->{to_type}")
        
        # Return unique edge types (e.g., "derived->constraint, constraint->derived")
        unique_types = sorted(set(edge_types))
        return ", ".join(unique_types)
    
    def get_dependencies(self, node: str) -> Set[str]:
        """Get direct dependencies of a node."""
        return self.edges.get(node, set())
    
    def get_dependents(self, node: str) -> Set[str]:
        """Get nodes that depend on this node (reverse edges)."""
        return self.reverse_edges.get(node, set())
    
    def get_transitive_dependencies(self, node: str) -> Set[str]:
        """Get all transitive dependencies (recursive)."""
        visited = set()
        stack = [node]
        
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            for dep in self.get_dependencies(current):
                if dep not in visited:
                    stack.append(dep)
        
        visited.discard(node)  # Don't include the node itself
        return visited
    
    def get_transitive_dependents(self, node: str) -> Set[str]:
        """Get all transitive dependents (what depends on this node, recursively)."""
        visited = set()
        stack = [node]
        
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            for dependent in self.get_dependents(current):
                if dependent not in visited:
                    stack.append(dependent)
        
        visited.discard(node)  # Don't include the node itself
        return visited
    
    def get_dependency_path(self, from_node: str, to_node: str) -> List[str] | None:
        """Get a dependency path from from_node to to_node, or None if no path exists."""
        # BFS to find shortest path
        if from_node == to_node:
            return [from_node]
        
        queue = [(from_node, [from_node])]
        visited = {from_node}
        
        while queue:
            current, path = queue.pop(0)
            
            for dependent in self.get_dependents(current):
                if dependent == to_node:
                    return path + [dependent]
                
                if dependent not in visited:
                    visited.add(dependent)
                    queue.append((dependent, path + [dependent]))
        
        return None
    
    def count_alternative_paths(self, from_node: str, to_node: str) -> int:
        """
        Count alternative dependency paths from from_node to to_node.
        
        Returns a bounded count of simple paths (no cycles) that are longer than
        the shortest path. This helps identify diamond dependencies where
        multiple paths exist between nodes.
        
        Implementation is bounded to prevent exponential explosion on dense graphs.
        Returns at most 10 (reported as "10+" if more exist).
        
        Returns:
            Number of alternative paths (paths longer than shortest path), capped at 10.
            Returns 0 if no path exists or only shortest path exists.
        """
        if from_node == to_node:
            return 0
        
        # First, find shortest path length using BFS
        shortest_path = self.get_dependency_path(from_node, to_node)
        if shortest_path is None:
            return 0
        
        shortest_length = len(shortest_path) - 1  # Number of edges
        
        # Bounded path counting: we only need to know ">1" exists, plus a small integer if cheap
        # Use iterative deepening with early termination to keep runtime bounded
        MAX_ALTERNATIVE_PATHS = 10  # Cap at 10 for reporting
        max_path_length = shortest_length + 10  # Allow paths up to 10 edges longer
        
        def count_paths_bounded(current: str, target: str, visited: Set[str], max_length: int, max_count: int) -> int:
            """Count simple paths from current to target, avoiding cycles, with early termination."""
            if current == target:
                return 1
            
            if len(visited) >= max_length:
                return 0  # Prune paths longer than max_length
            
            count = 0
            for dependent in sorted(self.get_dependents(current)):  # Sort for deterministic iteration
                if dependent not in visited:
                    new_visited = visited | {dependent}
                    count += count_paths_bounded(dependent, target, new_visited, max_length, max_count - count)
                    if count >= max_count:
                        return max_count  # Early termination if we hit the cap
            return count
        
        total_paths = count_paths_bounded(from_node, to_node, {from_node}, max_path_length, MAX_ALTERNATIVE_PATHS + 1)
        
        # Subtract 1 for the shortest path itself, cap at MAX_ALTERNATIVE_PATHS
        alternative_count = min(max(0, total_paths - 1), MAX_ALTERNATIVE_PATHS)
        
        return alternative_count
