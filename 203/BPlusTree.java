import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
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

    public Node insert(double key, String value, Node node) {
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

    public Node splitInternal(InternalNode node) {
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

    public LeafNode splitLeaf(LeafNode leaf) {
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

    public void search(double key, Node node, List<String> result) {
        if (node.isLeaf()) {
            LeafNode leaf = (LeafNode) node;
            result.addAll(leaf.getValuesForKey(key));
        } else {
            InternalNode internalNode = (InternalNode) node;
            int index = internalNode.findChildIndex(key);
            search(key, internalNode.getChild(index), result);
        }
    }

    public abstract class Node {
        List<Double> keys;
        Node parent;

        Node() {
            this.keys = new ArrayList<>();
        }

        abstract boolean isLeaf();
        abstract boolean isOverflow();
        abstract int getKeyCount();
        abstract Node getParent();
        abstract double removeKey(int index);
        abstract double getFirstKey();

        abstract List<Node> getChildren();

        abstract Node getChild(int index);

        abstract void addChild(int index, Node child);

        abstract void addKey(double key, String value);

        abstract List<Double> removeKeys(int startIndex);

        abstract List<Node> removeChildren(int startIndex);
    }

    public class InternalNode extends Node {
        List<Node> children;

        InternalNode() {
            this.children = new ArrayList<>();
        }

        @Override
        boolean isLeaf() {
            return false;
        }
        Node getParent(){
            return parent;
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
        double removeKey(int index){
            return keys.remove(index);
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

    public class LeafNode extends Node {
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
        boolean isOverflow(){
            return getKeyCount()>degree-1;
        }
        LeafNode getNext(){
            return next;
        }
        void setNext(){
            this.next = next;
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
         List<Double> addKeys(List<Double> keys) {
             this.keys.addAll(keys);
             return this.keys;
        }

         @Override
         List<Node> addChildren(List<Node> children) {
        // LeafNode does not have children, so you can leave this empty
            return Collections.emptyList();
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
        // Check if a file name is provided as a command-line argument
        if (args.length != 1) {
            System.out.println("Usage: java BPlusTreeMain <filename>");
            return;
        }

        String fileName = args[0];

        // Create a B+ tree with a degree of 3
        BPlusTree bPlusTree = new BPlusTree(3);

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Assuming each line in the file is a string to be indexed
                bPlusTree.insert(line.hashCode(), line);
            }

            // Print the B+ tree after indexing
            System.out.println("B+ Tree after indexing strings from the file:");
            bPlusTree.printTree();

            // Example search: searching for a string
            String searchString = "exampleString";
            System.out.println("\nSearch results for key " + searchString.hashCode() + ": " + bPlusTree.search(searchString.hashCode()));

        } catch (IOException e) {
            System.out.println("Error reading the file: " + e.getMessage());
        }
    }
}


