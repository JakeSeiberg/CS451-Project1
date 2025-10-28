
import math

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        tree = self.indices[column]

        return tree.locate(value)


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        tree = self.indices[column]

        return tree.locate_range(begin, end)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if (self.indices[column_number] is None):
            self.indices[column_number] = BPlusTree(order=4)
            for rid, (page_idx, slot_idx) in self.table.page_directory.items():
                value = self.table.read_column(column_number, page_idx, slot_idx)
                self.indices[column_number].insert(value, rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

class BPlusTree:
    def __init__(self, order=4):
        self.root = Node(order)
        self.order = order

    def insert(self, key, rid):
        root = self.root
        
        if (len(root.keys) == self.order - 1):
            new_root = Node(self.order)
            new_root.is_leaf = False
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root

        self.insert_non_full(self.root, key, rid)

    def insert_non_full(self, node, key, rid):
        if (node.is_leaf):
            idx = 0

            while (idx < len(node.keys) and node.keys[idx][0] < key):
                idx += 1
            
            node.keys.insert(idx, (key,rid))
        else:
            i = 0
            while (i < len(node.keys) and key >= node.keys[i]):
                i += 1
            
            if (i < len(node.children[i].keys) and key >= node.keys[i]):
                self._split_child(node, i)
                if key >= node.keys[i]:
                    i += 1

            self.insert_non_full(node.children[i], key, rid)
        
    def _split_child(self, parent, index):
        node = parent.children[index]
        mid = self.order // 2
        new_node = Node(self.order)
        new_node.is_leaf = node.is_leaf

        parent.keys.insert(index, node.keys[mid][0])
        new_node.keys = node.keys[mid:] if node.is_leaf else node.keys[mid + 1:]
        node.keys = node.keys[:mid]

        if (not node.is_leaf):
            new_node.children = node.children[mid + 1:]
            node.children = node.children[:mid + 1]
        else:
            new_node.next = node.next
            node.next = new_node

        parent.children.insert(index + 1, new_node)
        
    def locate(self, key):
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]

        return [rid for k, rid in node.keys if k == key]
    
    def locate_range(self, start, end):
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and start >= node.keys[i]:
                i += 1
            node = node.children[i]

        results = []
        while node:
            for k, rid in node.keys:
                if start <= k <= end:
                    results.append(rid)
                elif k > end:
                    return results
            node = node.next
        return results



class Node:

    def __init__(self, order):
        self.order = order
        self.keys = []
        self.children = []
        self.is_leaf = True
        self.next = None
        
def main():
    print("Running basic B+ Tree insert tests...\n")

    tree = BPlusTree(order=4)
    tree.insert(10, "rid10")
    print("Test 1 - Single insert:")
    print("Root keys:", tree.root.keys)
    assert tree.root.keys == [(10, "rid10")]
    print("Passed\n")
    tree = BPlusTree(order=4)
    for k, rid in [(5, "r5"), (2, "r2"), (8, "r8")]:
        tree.insert(k, rid)
    print("Test 2 - Multiple inserts (no split):")
    print("Root keys:", tree.root.keys)
    assert [k for k, _ in tree.root.keys] == [2, 5, 8]
    print("Passed\n")
    tree = BPlusTree(order=4)
    for k in [1, 2, 3, 4, 5]:
        tree.insert(k, f"rid{k}")
    print("Test 3 - Insert causing split:")
    print("Root keys:", tree.root.keys)
    print("Children count:", len(tree.root.children))
    assert not tree.root.is_leaf
    assert len(tree.root.children) == 2
    print("Passed\n")
main()
        