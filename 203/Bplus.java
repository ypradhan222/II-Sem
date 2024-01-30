import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
public class Bplus {

    static class Node implements Serializable{
        String[] keys;
        long[] fileOffsets;
        Node nextLeaf;
        Node[] children;
        boolean leaf;
        int size;
        Node parent;

        public Node(int degree, boolean leaf) {
            this.keys = new String[2 * degree - 1];
            this.fileOffsets = new long[2 * degree - 1];
            this.nextLeaf = null;
            this.children = new Node[2 * degree];
            this.leaf = leaf;
            this.size = 0;
            this.parent = null;
        }
    }

    static Node root;
    static int degree = 3;
    static final int KEY_LENGTH = 25;
private static void insertData(Node node, String key, long fileOffset) {
    int index = node.size - 1;
    while (index >= 0 && key.compareTo(node.keys[index]) < 0) {
        node.keys[index + 1] = node.keys[index];
        node.fileOffsets[index + 1] = node.fileOffsets[index];

        if (!node.leaf && index + 1 < node.children.length) {
            node.children[index + 2] = node.children[index + 1];
        }

        index--;
    }
    // Increment the index before using it to insert the new data
    index++;
    node.keys[index] = key;
    node.fileOffsets[index] = fileOffset;

    if (!node.leaf && index + 1 < node.children.length) {
        node.children[index + 1] = null; // Set the new child reference to null
    }
    node.size++;
}

private static void add(String key, long fileOffset) {
    if (root == null) {
        Node leafNode = new Node(degree, true);
        insertData(leafNode, key, fileOffset);
        root = leafNode;
        return;
    }
    Node currentNode = root;
    Node parent = null;

    // Find the leaf node that should contain the key
    while (!currentNode.leaf) {
        parent = currentNode;
        int index = currentNode.size - 1;
        while (index >= 0 && key.compareTo(currentNode.keys[index]) < 0) {
            index--;
        }
        currentNode = currentNode.children[index + 1];
    }

    // Insert into the leaf node
    insertData(currentNode, key, fileOffset);

    // Check if the leaf node is full and split if necessary
    if (currentNode.size == 2 * degree - 1) {
        splitNode(parent, currentNode);
        // splitNode(parent, key, fileOffset);
    }
}

private static void splitNode(Node parent, Node childNode) {
    int mid = childNode.size / 2;

    Node newNode = new Node(degree, childNode.leaf);

    for (int i = 0; i < childNode.size - mid; i++) {
        newNode.keys[i] = childNode.keys[i + mid];
        newNode.fileOffsets[i] = childNode.fileOffsets[i + mid];
        newNode.children[i] = childNode.children[i + mid];
        
        childNode.children[i + mid] = null;
    }

    newNode.size = childNode.size - mid;
    childNode.size = mid;

    newNode.children[newNode.size] = childNode.children[childNode.size];
    childNode.children[childNode.size] = null;

    if (parent == null) {
        // Create a new root
        Node newRoot = new Node(degree, false);
        insertData(newRoot, childNode.keys[mid - 1], childNode.fileOffsets[mid - 1]);
        newRoot.children[0] = childNode;
        newRoot.children[1] = newNode;
        childNode.parent = newRoot;
        newNode.parent = newRoot;
        root = newRoot;
    } else {
        insertData(parent, childNode.keys[mid - 1], childNode.fileOffsets[mid - 1]);
        int childIndex = 0;
        while (parent.children[childIndex] != childNode) {
            childIndex++;
        }

        // Shift pointers to make space for the new child
        for (int i = parent.size; i > childIndex + 1; i--) {
            parent.children[i + 1] = parent.children[i];
        }

        parent.children[childIndex + 1] = newNode;
        newNode.parent = parent;

        // Check if the parent is also full and split if necessary
        if (parent.size == 2 * degree - 1) {
            splitNode(parent.parent, parent);
        }
    }
}



    public static void display(Node node, int level) {
        if (node == null) {
            return;
        }
        System.out.print("Level: " + level + " Keys: ");
        for (int i = 0; i <node.size; i++) {
            System.out.print(formatKey(node.keys[i]) + " (Offset: " + node.fileOffsets[i] + ") ");
        }
        System.out.println();
        if (!node.leaf) {
            for (int i = 0; i <= node.size; i++) {
                display(node.children[i], level + 1);
            }
        } else if (node.nextLeaf != null) {
            display(node.nextLeaf, level);
        }
    }

    private static long search(Node node, String key) {
    int index = 0;
    while (index < node.size && key.compareTo(node.keys[index]) > 0) {
        index++;
    }
    if (node.leaf) {
        // Node is a leaf, search for the key
        for (int i = 0; i < node.size; i++) {
            if (key.equals(node.keys[i])) {
                return node.fileOffsets[i];
            }
        }
        return -1; // Key not found
    } else {
        // Node is internal, continue searching in the appropriate child
        return search(node.children[index], key);
    }
}

    public static long search(String key) {
        if (root == null) {
        return -1; // Tree is empty
    }
        return search(root, key);
}
    public static void delete(String key) {
    if (root == null) {
        System.out.println("Tree is empty");
        return;
    }
    // Find the leaf node containing the key
    Node leafNode = findLeafNode(root, key);
    if (leafNode != null) {
        // Delete the key from the leaf node
        deleteKeyFromNode(leafNode, key);
        // Handle underflow and propagate changes upward
        handleUnderflow(leafNode);
        System.out.println("Key: " + key + " deleted from the B+ tree index.");
    } else {
        System.out.println("Key: " + key + " not found in the B+ tree index.");
    }
}
private static Node findLeafNode(Node node, String key) {
    int index = 0;
    // Find the child index where the key might be present
    while (index < node.size && key.compareTo(node.keys[index]) > 0) {
        index++;
    }
    if (node.leaf) {
        // If it's a leaf node, return the node
        return node;
    } else {
        // If it's an internal node, recursively search in the appropriate child
        return findLeafNode(node.children[index], key);
    }
}
/*private static void deleteKeyFromNode(Node node, String key) {
    int index = 0;
    // Find the index of the key to be deleted in the node
    while (index < node.size && key.compareTo(node.keys[index]) > 0) {
        index++;
    }
    // Shift keys and fileOffsets to cover the deleted key
    for (int i = index; i < node.size - 1; i++) {
        node.keys[i] = node.keys[i + 1];
        node.fileOffsets[i] = node.fileOffsets[i + 1];
    }
    // Decrement the size of the node
    node.size--;
}
*/
private static void deleteKeyFromNode(Node node, String key) {
    int index = 0;
    // Find the index of the key to be deleted in the node
    while (index < node.size && key.compareTo(node.keys[index]) > 0) {
        index++;
    }
    // Shift keys and fileOffsets to cover the deleted key
    for (int i = index; i < node.size - 1; i++) {
        node.keys[i] = node.keys[i + 1];
        node.fileOffsets[i] = node.fileOffsets[i + 1];
    }
    // Decrement the size of the node
    node.size--;

    // Adjust nextLeaf pointers
    if (node.size == 0 && node.nextLeaf != null) {
        node.nextLeaf.nextLeaf = null;
    }
}

private static void handleUnderflow(Node node) {
    // Check if the node is underflowed
    if (node.size < degree - 1) {
        Node parent = node.parent;
        int index = 0;

        // Find the index of the underflowed child in the parent
        while (index < parent.size && parent.children[index] != node) {
            index++;
        }
        // Try to borrow a key from the left sibling
        if (index > 0 && parent.children[index - 1].size > degree - 1) {
            borrowFromLeftSibling(parent, index);
        }
        // Try to borrow a key from the right sibling
        else if (index < parent.size && parent.children[index + 1].size > degree - 1) {
            borrowFromRightSibling(parent, index);
        }
        // Merge with the left sibling if borrowing is not possible
        else if (index > 0) {
            mergeWithLeftSibling(parent, index);
        }
        // Merge with the right sibling if borrowing is not possible
        else {
            mergeWithRightSibling(parent, index);
        }

        // Recursively handle underflow in the parent
        handleUnderflow(parent);
    }
}
private static void borrowFromLeftSibling(Node parent, int index) {
    Node node = parent.children[index];
    Node leftSibling = parent.children[index - 1];

    // Shift keys and fileOffsets in the node to create space for the borrowed key
    for (int i = node.size - 1; i >= 0; i--) {
        node.keys[i + 1] = node.keys[i];
        node.fileOffsets[i + 1] = node.fileOffsets[i];
    }

    // Shift children pointers in the node for non-leaf nodes
    if (!node.leaf) {
        for (int i = node.size; i >= 0; i--) {
            node.children[i + 1] = node.children[i];
        }
    }
    // Copy the key and fileOffset from the left sibling to the node
    node.keys[0] = leftSibling.keys[leftSibling.size - 1];
    node.fileOffsets[0] = leftSibling.fileOffsets[leftSibling.size - 1];
    // Adjust the size of the node and the left sibling
    node.size++;
    leftSibling.size--;
    // Update the key in the parent that was borrowed
    parent.keys[index - 1] = node.keys[0];
}
private static void borrowFromRightSibling(Node parent, int index) {
    Node node = parent.children[index];
    Node rightSibling = parent.children[index + 1];

    // Copy the key and fileOffset from the right sibling to the node
    node.keys[node.size] = rightSibling.keys[0];
    node.fileOffsets[node.size] = rightSibling.fileOffsets[0];

    // Shift keys and fileOffsets in the right sibling to cover the borrowed key
    for (int i = 0; i < rightSibling.size - 1; i++) {
        rightSibling.keys[i] = rightSibling.keys[i + 1];
        rightSibling.fileOffsets[i] = rightSibling.fileOffsets[i + 1];
    }
    // Shift children pointers in the right sibling for non-leaf nodes
    if (!rightSibling.leaf) {
        for (int i = 0; i < rightSibling.size; i++) {
            rightSibling.children[i] = rightSibling.children[i + 1];
        }
    }
    node.size++;
    rightSibling.size--;
    // Update the key in the parent that was borrowed
    parent.keys[index] = rightSibling.keys[0];
}

private static void mergeWithLeftSibling(Node parent, int index) {
    Node node = parent.children[index];
    Node leftSibling = parent.children[index - 1];

    // Copy keys and fileOffsets from the node to the left sibling
    for (int i = 0; i < node.size; i++) {
        leftSibling.keys[leftSibling.size + i] = node.keys[i];
        leftSibling.fileOffsets[leftSibling.size + i] = node.fileOffsets[i];
    }

    // Copy children pointers for non-leaf nodes
    if (!node.leaf) {
        for (int i = 0; i <= node.size; i++) {
            leftSibling.children[leftSibling.size + i] = node.children[i];
        }
    }
    // Adjust the size of the left sibling
    leftSibling.size += node.size;
    // Remove the underflowed node from the parent's children array
    for (int i = index; i < parent.size - 1; i++) {
        parent.children[i] = parent.children[i + 1];
    }
    parent.size--;
    // Update the key in the parent
    parent.keys[index - 1] = parent.children[index].keys[0];
}
    private static void mergeWithRightSibling(Node parent, int index) {
        Node node = parent.children[index];
        Node rightSibling = parent.children[index + 1];
        
        // Copy keys and fileOffsets from the right sibling to the node
        for (int i = 0; i < rightSibling.size; i++) {
            node.keys[node.size + i] = rightSibling.keys[i];
            node.fileOffsets[node.size + i] = rightSibling.fileOffsets[i];
        }
        // Copy children pointers for non-leaf nodes
        if (!rightSibling.leaf) {
            for (int i = 0; i <= rightSibling.size; i++) {
                node.children[node.size + i] = rightSibling.children[i];
            }
        }
        node.size += rightSibling.size;
        // Remove the right sibling from the parent's children array
        for (int i = index + 1; i < parent.size; i++) {
            parent.children[i] = parent.children[i + 1];
        }
        parent.size--;
        // Update the key in the parent
        parent.keys[index] = parent.children[index].keys[0];
}  
      public static void writeIndexToFile(String indexFileName) {
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(indexFileName))) {
            writeNodeToStream(outputStream, root);
            System.out.println("B+ Tree index written to file: " + indexFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method to recursively write a node and its children to the stream
    private static void writeNodeToStream(ObjectOutputStream outputStream, Node node) throws IOException {
        if (node != null) {
            // Write node information to the stream
            outputStream.writeObject(node);

            // Recursively write children nodes for non-leaf nodes
            if (!node.leaf) {
                for (int i = 0; i <= node.size; i++) {
                    writeNodeToStream(outputStream, node.children[i]);
                }
            }
        }
    }
    public static void main(String[] args) {
    if (args.length != 1) {
        System.out.println("Usage: java BPlusTreeFileIndex <filename>");
        return;
    }

    String filename = args[0];

    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
        String line;
        long fileOffset = 0;

        while ((line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
                String key;
                try {
                    key = line;
                    add(formatKey(String.valueOf(key)), fileOffset);
                    fileOffset += line.length() + 1; // +1 for newline character
                } catch (NumberFormatException e) {
                    System.out.println("Skipping invalid line: " + line);
                }
            }
        }
        display(root, 1);
        String deleteKey = "yogen";
        System.out.println("\nDeleting Key: " + deleteKey);
        delete(formatKey(deleteKey));

        System.out.println("\nB+ Tree after deletion:");
        display(root, 1);
        
        String searchKey = "yogen"; 
        searchKey = formatKey(searchKey);
        long searchResult = search(searchKey);
        if (searchResult != -1) {
            System.out.println("Key: " + searchKey + " found at offset: " + searchResult);
        } else {
            System.out.println("Key: " + searchKey + " not found");
        }

        String indexFileName = "BPlusTreeIndex.txt";
        writeIndexToFile(indexFileName);

    } catch (IOException e) {
        e.printStackTrace();
    }
}    
    
    //Padding and truncating done here
    public static String formatKey(String key) {
        if (key.length() == KEY_LENGTH) {
            return key;
        } else if (key.length() < KEY_LENGTH) {
            return String.format("%-" + KEY_LENGTH + "s", key);
        } else {
            return key.substring(0, KEY_LENGTH);
        }
    }
}
