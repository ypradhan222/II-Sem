import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BPlusTree {

    private Node root;
    private int degree;

    public BPlusTree(int degree) {
        this.root = null;
        this.degree = degree;
    }

    public void insert(double key, String value) {
        if (root == null) {
            root = new LeafNode();
            root.addKey(key, value);
        } else {
            root = insert(key, value, root);
        }
    }

    private Node insert(double key, String value, Node node) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            leaf.addKey(key, value);

            if (leaf.isOverflow()) {
                return splitLeaf(leaf);
            }
        } else {
            InternalNode internalNode = (InternalNode) node;
            int index = internalNode.findChildIndex(key);

            Node child = internalNode.getChild(index);
            Node newChild = insert(key, value, child);

            if (newChild != child) {
                internalNode.addChild(index, newChild);
                internalNode.addKey(newChild.getFirstKey(), null);
                if (internalNode.isOverflow()) {
                    return splitInternal(internalNode);
                }
            }
        }

        return node;
    }

    private Node splitInternal(InternalNode node) {
        InternalNode newNode = new InternalNode();

        int midIndex = node.getKeyCount() / 2;

        InternalNode parent = node.getParent();
        if (parent == null) {
            parent = new InternalNode();
            root = parent;
        }

        newNode.addKey(node.removeKey(midIndex), null);
        newNode.addKeys(node.removeKeys(midIndex));
        newNode.addChildren(node.removeChildren(midIndex + 1));

        parent.addChild(parent.findChildIndex(newNode.getFirstKey()), newNode);

        if (parent.isOverflow()) {
            return splitInternal(parent);
        }

        return parent;
    }

    private LeafNode splitLeaf(LeafNode leaf) {
        LeafNode newLeaf = new LeafNode();

        int midIndex = leaf.getKeyCount() / 2;

        newLeaf.addKeys(leaf.removeKeys(midIndex));
        newLeaf.setNext(leaf.getNext());
        leaf.setNext(newLeaf);

        return newLeaf;
    }

    public List<String> search(double key) {
        List<String> result = new ArrayList<>();
        search(key, root, result);
        return result;
    }

    private void search(double key, Node node, List<String> result) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            result.addAll(leaf.getValuesForKey(key));
        } else {
            InternalNode internalNode = (InternalNode) node;
            int index = internalNode.findChildIndex(key);
            search(key, internalNode.getChild(index), result);
        }
    }

    private abstract class Node {
        List<Double> keys;
        Node parent;

        Node() {
            this.keys = new ArrayList<>();
        }

        abstract boolean isLeaf();

        abstract int getKeyCount();

        abstract double getFirstKey();

        abstract List<Node> getChildren();

        abstract Node getChild(int index);

        abstract void addChild(int index, Node child);

        abstract void addKey(double key, String value);

        abstract List<Double> removeKeys(int startIndex);

        abstract List<Node> removeChildren(int startIndex);
    }

    private class InternalNode extends Node {
        List<Node> children;

        InternalNode() {
            this.children = new ArrayList<>();
        }

        @Override
        boolean isLeaf() {
            return false;
        }

        @Override
        int getKeyCount() {
            return keys.size();
        }

        @Override
        double getFirstKey() {
            return keys.get(0);
        }

        @Override
        List<Node> getChildren() {
            return children;
        }

        @Override
        Node getChild(int index) {
            return children.get(index);
        }

        @Override
        void addChild(int index, Node child) {
            children.add(index, child);
            child.parent = this;
        }

        @Override
        void addKey(double key, String value) {
            keys.add(key);
        }

        @Override
        List<Double> removeKeys(int startIndex) {
            List<Double> removedKeys = new ArrayList<>(keys.subList(startIndex, keys.size()));
            keys.subList(startIndex, keys.size()).clear();
            return removedKeys;
        }

        @Override
        List<Node> removeChildren(int startIndex) {
            List<Node> removedChildren = new ArrayList<>(children.subList(startIndex, children.size()));
            children.subList(startIndex, children.size()).clear();
            return removedChildren;
        }

        int findChildIndex(double key) {
            int index = 0;
            while (index < getKeyCount() && key >= keys.get(index)) {
                index++;
            }
            return index;
        }

        boolean isOverflow() {
            return getKeyCount() > degree - 1;
        }
    }

    private class LeafNode extends Node {
        List<String> values;
        LeafNode next;

        LeafNode() {
            this.values = new ArrayList<>();
            this.next = null;
        }

        @Override
        boolean isLeaf() {
            return true;
        }

        @Override
        int getKeyCount() {
            return keys.size();
        }

        @Override
        double getFirstKey() {
            return keys.get(0);
        }

        @Override
        List<Node> getChildren() {
            throw new UnsupportedOperationException("Leaf node does not have children.");
        }

        @Override
        Node getChild(int index) {
            throw new UnsupportedOperationException("Leaf node does not have children.");
        }

        @Override
        void addChild(int index, Node child) {
            throw new UnsupportedOperationException("Leaf node does not have children.");
        }

        @Override
        void addKey(double key, String value) {
            keys.add(key);
            values.add(value);
        }

        @Override
        List<Double> removeKeys(int startIndex) {
            List<Double> removedKeys = new ArrayList<>(keys.subList(startIndex, keys.size()));
            keys.subList(startIndex, keys.size()).clear();
            return removedKeys;
        }

        @Override
        List<Node> removeChildren(int startIndex) {
            throw new UnsupportedOperationException("Leaf node does not have children.");
        }

        List<String> getValuesForKey(double key) {
            List<String> result = new ArrayList<>();
            int index = keys.indexOf(key);
            if (index != -1) {
                result.addAll(values.subList(index, index + 1));
            }
            return result;
        }
    }

    public void printTree() {
        if (root != null) {
            Queue<Node> queue = new LinkedList<>();
            queue.add(root);

            while (!queue.isEmpty()) {
                int size = queue.size();

                for (int i = 0; i < size; i++) {
                    Node current = queue.poll();
                    current.keys.forEach(key -> System.out.print(key + " "));
                    if (current.isLeaf()) {
                        LeafNode leaf = (LeafNode) current;
                        while (leaf != null) {
                            leaf.keys.forEach(key -> System.out.print(key + " "));
                            leaf = leaf.next;
                        }
                    } else {
                        InternalNode internalNode = (InternalNode) current;
                        queue.addAll(internalNode.children);
                    }
                    System.out.print("| ");
                }

                System.out.println();
            }
        }
    }

    public static void main(String[] args) {
        BPlusTree bPlusTree = new BPlusTree(3);

        bPlusTree.insert(1.1, "One");
        bPlusTree.insert(2.2, "Two");
        bPlusTree.insert(3.3, "Three");
        bPlusTree.insert(4.4, "Four");
        bPlusTree.insert(5.5, "Five");
        bPlusTree.insert(6.6, "Six");
        bPlusTree.insert(7.7, "Seven");
        bPlusTree.insert(8.8, "Eight");
        bPlusTree.insert(9.9, "Nine");

        bPlusTree.printTree();

        System.out.println("\nSearch results for key 4.4: " + bPlusTree.search(4.4));
        System.out.println("Search results for keys between 3.0 and 7.0: " + bPlusTree.search(3.0, 7.0));
    }
}

