#!/usr/bin/env python
# coding:utf-8

from copy import copy
from textwrap import dedent
from collections import deque, OrderedDict


class DAG(object):
    """ Directed acyclic graph implementation. """

    def __init__(self):
        self.reset_graph()
        self.all_nodes = set()

    def add_node(self, node_name, graph=None):
        if not graph:
            graph = self.graph
        if node_name in graph:
            raise KeyError('node %s already exists' % node_name)
        self.all_nodes.add(node_name)
        graph[node_name] = set()

    def add_node_if_not_exists(self, node_name, graph=None):
        try:
            self.add_node(node_name, graph=graph)
        except KeyError:
            pass

    def delete_node(self, node_name, graph=None):
        if not graph:
            graph = self.graph
        if node_name not in graph:
            raise KeyError('node %s does not exist' % node_name)
        graph.pop(node_name)
        self.all_nodes.remove(node_name)
        for node, edges in graph.items():
            if node_name in edges:
                edges.remove(node_name)

    def rename_node(self, node_name, new_name, graph=None):
        if not graph:
            graph = self.graph
        if node_name not in graph:
            raise KeyError('node %s does not exist' % node_name)
        self.all_nodes.remove(node_name)
        self.add_node_if_not_exists(new_name)
        edges = graph.pop(node_name)
        for edge in edges:
            self.add_edge(new_name, edge)
        for _, edges in graph.items():
            if node_name in edges:
                edges.remove(node_name)
                edges.add(new_name)

    def delete_node_if_exists(self, node_name, graph=None):
        try:
            self.delete_node(node_name, graph=graph)
        except KeyError:
            pass

    def add_edge(self, ind_node, dep_node, graph=None):
        if not graph:
            graph = self.graph
        if ind_node not in graph or dep_node not in graph:
            raise KeyError('one or more nodes do not exist in graph')
        graph[ind_node].add(dep_node)

    def delete_edge(self, ind_node, dep_node, graph=None):
        if not graph:
            graph = self.graph
        if dep_node not in graph.get(ind_node, []):
            raise KeyError('this edge does not exist in graph')
        graph[ind_node].remove(dep_node)

    def predecessors(self, node, graph=None):
        if graph is None:
            graph = self.graph
        return [key for key in graph if node in graph[key]]

    def downstream(self, node, graph=None):
        if graph is None:
            graph = self.graph
        if node not in graph:
            raise KeyError('node %s is not in graph' % node)
        return list(graph[node])

    def all_downstreams(self, node, graph=None):
        if graph is None:
            graph = self.graph
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            downstreams = self.downstream(nodes[i], graph)
            for downstream_node in downstreams:
                if downstream_node not in nodes_seen:
                    nodes_seen.add(downstream_node)
                    nodes.append(downstream_node)
            i += 1
        return list(
            filter(
                lambda node: node in nodes_seen,
                self.topological_sort(graph=graph)
            )
        )

    def all_leaves(self, graph=None):
        if graph is None:
            graph = self.graph
        return [key for key in graph if not graph[key]]

    def from_dict(self, graph_dict):
        self.reset_graph()
        for new_node in graph_dict:
            self.add_node(new_node)
        for ind_node, dep_nodes in graph_dict.items():
            if not isinstance(dep_nodes, list):
                raise TypeError('dict values must be lists')
            for dep_node in dep_nodes:
                self.add_node_if_not_exists(dep_node)
                self.add_edge(ind_node, dep_node)

    def reset_graph(self):
        self.graph = OrderedDict()

    def ind_nodes(self, graph=None):
        if graph is None:
            graph = self.graph
        nodes2 = set(n2 for n2s in graph.values() for n2 in n2s)
        return [n1 for n1 in graph.keys() if n1 not in nodes2]

    def end_nodes(self, graph=None):
        if graph is None:
            graph = self.graph
        return [n1 for n1, n2 in graph.items() if not n2]

    def validate(self, graph=None):
        graph = graph if graph is not None else self.graph
        if len(self.ind_nodes(graph)) == 0:
            return (False, 'no independent nodes detected')
        try:
            self.topological_sort(graph)
        except ValueError:
            return (False, 'failed topological sort')
        return (True, 'valid')

    def topological_sort(self, graph=None):
        if graph is None:
            graph = self.graph

        in_degree = {}
        for u in graph:
            in_degree[u] = 0

        for u in graph:
            for v in graph[u]:
                in_degree[v] += 1

        queue = deque()
        for u in in_degree:
            if in_degree[u] == 0:
                queue.appendleft(u)

        l = []
        while queue:
            u = queue.pop()
            l.append(u)
            for v in graph[u]:
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.appendleft(v)

        if len(l) == len(graph):
            return l
        else:
            raise ValueError('graph is not acyclic')

    def size(self):
        return len(self.graph)

    def dot(self, node2rule={}):
        all_nodes = sorted(self.all_nodes)
        ids = {node: i for i, node in enumerate(all_nodes)}
        huefactor = 2 / (3 * len(ids))
        node2rule = {node: node2rule.get(node, node)
                     for node in all_nodes}
        rules = sorted(set(node2rule.values()))
        rulecolor = {
            rule: "{:.2f} 0.6 0.85".format(i * huefactor)
            for i, rule in enumerate(rules)
        }
        node_markup = '\t{}[label = "{}", color = "{}", style="{}"];'.format
        edge_markup = "\t{} -> {}".format
        nodes = [
            node_markup(
                ids[node],
                node,
                rulecolor[node2rule[node]],
                "rounded",
            )
            for node in all_nodes
        ]
        edges = [edge_markup(*i) for i in sorted([(ids[node], ids[dep]) for node, deps in self.graph.items()
                                                  for dep in deps if deps], key=lambda x: (x[1], x[0]))]
        return dedent(
            """\
            digraph {name} {{
                graph[bgcolor=white, margin=0];
                node[shape=box, style=rounded, fontname=sans, \
                fontsize=10, penwidth=1];
                edge[penwidth=1, color=grey];
            {items}
            }}\
            """
        ).format(name=__package__ + "_dag", items="\n".join(nodes + edges))

    def copy(self):
        dag = self.__class__()
        dag.all_nodes = self.all_nodes.copy()
        dag.graph = OrderedDict(
            {node: self.graph[node].copy() for node in self.graph})
        return dag

    def __str__(self):
        return self.dot()

    __repr__ = __str__
