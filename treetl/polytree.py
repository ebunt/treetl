

class TreeNode(object):
    def __init__(self, id, data):
        self.id = id
        self.data = data


class PolyTree(object):
    def __init__(self, nodes=None):
        self.__node_map = { }
        self.__graph = { }
        if nodes:
            for node in nodes:
                self.add_node(node)

    def nodes(self):
        return self.__node_map.values()

    # allows for creation of new dependencies on add
    def add_node(self, node, parents=None, children=None):
        if node.id not in self.__node_map:
            self.__node_map[node.id] = node
            self.__graph[node.id] = []

        if parents is not None:
            for p in parents:
                self.add_child(p, node)

        if children is not None:
            for c in children:
                self.add_child(c)

        return self

    def add_child(self, node, child_node):
        if not self.node_exists(child_node):
            self.add_node(child_node)

        if not self.node_exists(node):
            self.add_node(node)

        self.__graph[node.id].append(child_node.id)

        return self

    def node_exists(self, node):
        return node.id in self.__node_map

    def get_node(self, id, default=None):
        try:
            return self.__node_map[id]
        except:
            return default

    def clear_nodes(self):
        self.__node_map = { }
        self.__graph = { }

    def root_nodes(self):
        is_a_child_to_someone = set([
            child_id
            for children in self.__graph.values()
            for child_id in children
        ])
        return [
            self.get_node(id)
            for id in self.__node_map.keys()
            if id not in is_a_child_to_someone
        ]

    def end_nodes(self):
        return [ self.get_node(id) for id, children in self.__graph.items() if len(children) == 0 ]

    def is_solo_node(self, node):
        return len(self.__graph[node.id]) == 0 and node.id in [ root.id for root in self.root_nodes() ]

    def solo_nodes(self):
        return [
            root_node
            for root_node in self.root_nodes()
            if len(self.__graph[root_node.id]) == 0
        ]

    def parents(self, node):
        return [
            self.__node_map[parent_id]
            for parent_id, children in self.__graph.items()
            if node.id in children
        ]

    def children(self, node):
        return [ self.__node_map[id] for id in self.__graph[node.id] ]

    def __all_paths(self, end_node):
        paths, next_path = [], [ end_node ]
        parents = self.parents(end_node)
        if parents:
            for p in parents:
                for new_path in self.__all_paths(p):
                    paths.append(next_path + new_path)
                next_path = [ end_node ]
        else:
            paths.append(next_path)

        return paths

    def all_paths(self, end_node):
        return [ p[::-1] for p in self.__all_paths(end_node) ]
