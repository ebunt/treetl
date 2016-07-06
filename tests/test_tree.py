
import unittest


class TestJobExecutionOrder(unittest.TestCase):

    def setUp(self):
        from string import ascii_lowercase
        from treetl.tools.polytree import TreeNode

        self.nodes = [
            TreeNode(i, lett)
            for i, lett in enumerate(ascii_lowercase[:9])
        ]

        # tree structure is as follows
        self.node_arcs = [
            (0, 7),  # A(1) -> H(8)
            (1, 7),  # B(2) -> H(8)
            (1, 5),  # B(2) -> F(6)
            (2, 5),  # C(3) -> F(6)
            (3, 6),  # D(4) -> G(7)
            (5, 8),  # F(6) -> I(9)
            (6, 8),  # G(7) -> I(9)
            (2, 8),  # C(3) -> I(9)
            (2, 6),  # G(7) -> I(9)
            (5, 7)   # F(6) -> H(8)
                     # E(5) is on its own
        ]

    def test_tree(self):

        from treetl.tools.polytree import PolyTree

        poly_tree = PolyTree(nodes=self.nodes)
        for parent, child in self.node_arcs:
            poly_tree.add_child(self.nodes[parent], self.nodes[child])

        for node in self.nodes:
            self.assertTrue(poly_tree.node_exists(node), msg='Node: {} not found in poly_tree'.format(node))

        self.assertEqual(poly_tree.get_node(0), self.nodes[0], msg='Node 0 and A node are different')

        self.assertItemsEqual(
            expected_seq=self.nodes[:5],
            actual_seq=poly_tree.root_nodes(),
            msg='Incorrect root nodes'
        )

        self.assertItemsEqual(
            expected_seq=[ self.nodes[4], self.nodes[7], self.nodes[8] ],
            actual_seq=poly_tree.end_nodes(),
            msg='Incorrect end nodes'
        )

        # check the many paths to I
        self.assertItemsEqual(
            expected_seq=[
                [ self.nodes[1], self.nodes[5], self.nodes[8] ],  # B -> F -> I
                [ self.nodes[2], self.nodes[5], self.nodes[8] ],  # C -> F -> I
                [ self.nodes[2], self.nodes[8] ],                 # C -> I
                [ self.nodes[2], self.nodes[6], self.nodes[8] ],  # C -> G -> I
                [ self.nodes[3], self.nodes[6], self.nodes[8] ]   # D -> G -> I
            ],
            actual_seq=poly_tree.all_paths(self.nodes[8]),
            msg='Incorrect paths to node I'
        )

        self.assertTrue(poly_tree.is_solo_node(self.nodes[4]), msg='Node not identified as solo')
        self.assertEqual(first=len(poly_tree.solo_nodes()), second=1, msg='Incorrent number of solo nodes')

        self.assertItemsEqual(
            expected_seq=[ self.nodes[5], self.nodes[8], self.nodes[6] ],
            actual_seq=poly_tree.children(self.nodes[2]),
            msg='Incorrect children of Node C'
        )

        self.assertItemsEqual(
            expected_seq=[ self.nodes[0], self.nodes[1], self.nodes[5] ],
            actual_seq=poly_tree.parents(self.nodes[7]),
            msg='Incorrect parents of Node H'
        )
