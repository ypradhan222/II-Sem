import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.io.*;
public class Bplus2 {
    static class Node implements Serializable {
        List<String> keys;
        List<Long> fileOffsets;
        Node nextLeaf;
        List<Node> children;
        boolean leaf;
        int size;
        Node parent;

        public Node(int degree, boolean leaf) {
            this.keys = new ArrayList<>(2 * degree - 1);
            this.fileOffsets = new ArrayList<>(2 * degree - 1);
            this.nextLeaf = null;
            this.children = new ArrayList<>(2 * degree);
            this.leaf = leaf;
            this.size = 0;
            this.parent = null;
        }
    }
    static Node root;
    static int degree =3;
    static final int KEY_LENGTH =25;
    // Inside your Bplus class
private static void insertData(Node node, String key, long fileOffset) {
    int index = node.size - 1;

    while (index >= 0 && key.compareTo(node.keys.get(index)) < 0) {
        node.keys.add(index + 1, node.keys.get(index));
        node.fileOffsets.add(index + 1, node.fileOffsets.get(index));

        if (!node.leaf && index + 1 < node.children.size()) {
            node.children.add(index + 2, node.children.get(index + 1));
        }

        index--;
    }

    // Increment the index before using it to insert the new data
    index++;
    node.keys.set(index, key);
    node.fileOffsets.set(index, fileOffset);

    if (!node.leaf && index + 1 < node.children.size()) {
        node.children.set(index + 1, null); // Set the new child reference to null
    }

    node.size++;
}

// private static void splitNode(Node parent, Node childNode) {
//     int mid = childNode.size / 2;

//     Node newNode = new Node(degree, childNode.leaf);

//     for (int i = 0; i < childNode.size - mid; i++) {
//         newNode.keys.add(childNode.keys.get(i + mid));
//         newNode.fileOffsets.add(childNode.fileOffsets.get(i + mid));

//         if (!childNode.leaf) {
//             newNode.children.add(childNode.children.get(i + mid));
//             childNode.children.set(i + mid, null);
//         }
//     }

//     newNode.size = childNode.size - mid;
//     childNode.size = mid;

//     if (parent == null) {
//         // Create a new root
//         Node newRoot = new Node(degree, false);
//         insertData(newRoot, childNode.keys.get(mid - 1), childNode.fileOffsets.get(mid - 1));
//         newRoot.children.set(0, childNode);
//         newRoot.children.set(1, newNode);
//         childNode.parent = newRoot;
//         newNode.parent = newRoot;
//         root = newRoot;
//     } else {
//         insertData(parent, childNode.keys.get(mid - 1), childNode.fileOffsets.get(mid - 1));
//         int childIndex = 0;
//         while (parent.children.get(childIndex) != childNode) {
//             childIndex++;
//         }

//         // Shift pointers to make space for the new child
//         for (int i = parent.size; i > childIndex + 1; i--) {
//             parent.children.set(i + 1, parent.children.get(i));
//         }

//         parent.children.set(childIndex + 1, newNode);
//         newNode.parent = parent;

//         // Check if the parent is also full and split if necessary
//         if (parent.size == 2 * degree - 1) {
//             splitNode(parent.parent, parent);
//         }
//     }
// }

private static void splitNode(Node parent, Node childNode) {
    int mid = childNode.size / 2;

    Node newNode = new Node(degree, childNode.leaf);

    for (int i = 0; i < childNode.size - mid; i++) {
        newNode.keys.add(childNode.keys.get(i + mid));
        newNode.fileOffsets.add(childNode.fileOffsets.get(i + mid));
        newNode.children.add(childNode.children.get(i + mid));

      //   childNode.children.set(i + mid, null);
    }

    newNode.size = childNode.size - mid;
    childNode.size = mid;

    newNode.children.add(childNode.children.get(childNode.size));
    childNode.children.set(childNode.size, null);

    if (parent == null) {
        // Create a new root
        Node newRoot = new Node(degree, false);
        insertData(newRoot, childNode.keys.get(mid - 1), childNode.fileOffsets.get(mid - 1));
        newRoot.children.set(0, childNode);
        newRoot.children.set(1, newNode);
        childNode.parent = newRoot;
        newNode.parent = newRoot;
        root = newRoot;
    } else {
        insertData(parent, childNode.keys.get(mid - 1), childNode.fileOffsets.get(mid - 1));
        int childIndex = 0;
        while (parent.children.get(childIndex) != childNode) {
            childIndex++;
        }

        // Shift pointers to make space for the new child
        for (int i = parent.size; i > childIndex + 1; i--) {
            parent.children.set(i + 1, parent.children.get(i));
        }

        parent.children.set(childIndex + 1, newNode);
        newNode.parent = parent;

        // Check if the parent is also full and split if necessary
        if (parent.size == 2 * degree - 1) {
            splitNode(parent.parent, parent);
        }
    }
}


private static void deleteKeyFromNode(Node node, String key) {
    int index = 0;
    // Find the index of the key to be deleted in the node
    while (index < node.size && key.compareTo(node.keys.get(index)) > 0) {
        index++;
    }
    // Shift keys and fileOffsets to cover the deleted key
    for (int i = index; i < node.size - 1; i++) {
        node.keys.set(i, node.keys.get(i + 1));
        node.fileOffsets.set(i, node.fileOffsets.get(i + 1));
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
        while (index < parent.size && parent.children.get(index) != node) {
            index++;
        }
        // Try to borrow a key from the left sibling
        if (index > 0 && parent.children.get(index - 1).size > degree - 1) {
            borrowFromLeftSibling(parent, index);
        }
        // Try to borrow a key from the right sibling
        else if (index < parent.size && parent.children.get(index + 1).size > degree - 1) {
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
//    private static void add(String key, long fileOffset) {
//     if (root == null) {
//         Node leafNode = new Node(degree, true);
//         insertData(leafNode, key, fileOffset);
//         root = leafNode;
//         return;
//     }
//     Node currentNode = root;
//     Node parent = null;

//     // Find the leaf node that should contain the key
//     while (!currentNode.leaf) {
//         parent = currentNode;
//         int index = currentNode.size - 1;
//         while (index >= 0 && key.compareTo(currentNode.keys.get(index)) < 0) {
//             index--;
//         }
//         currentNode = currentNode.children.get(index + 1);
//     }

//     // Insert into the leaf node
//     insertData(currentNode, key, fileOffset);

//     // Check if the leaf node is full and split if necessary
//     if (currentNode.size == 2 * degree - 1) {
//         splitNode(parent, currentNode);
//     }
// }
private static void addNode(String key, long fileOffset) {
    key = formatKey(key);
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
        while (index >= 0 && key.compareTo(currentNode.keys.get(index)) < 0) {
            index--;
        }
        currentNode = currentNode.children.get(index + 1);
    }

    // Insert into the leaf node
    insertData(currentNode, key, fileOffset);

    // Check if the leaf node is full and split if necessary
    if (currentNode.size == 2 * degree - 1) {
        splitNode(parent, currentNode);
    }
}

   // Inside your Bplus2 class
private static void delete(String key) {
    if (root == null) {
        System.out.println("B+ Tree is empty. Cannot delete key.");
        return;
    }
    Node leafNode = findLeafNode(root, key);
    deleteKeyFromNode(leafNode, key);

    if (root.size == 0) {
        // If root becomes empty, update the root
        root = root.children.get(0);
        if (root != null) {
            root.parent = null;
        }
    } else if (leafNode.size < degree - 1) {
        // Handle underflow starting from the root
        handleUnderflow(root);
    }
}


// Inside your Bplus2 class
private static long search(String key) {
    return search(root, key);
}

   private static long search(Node node, String key) {
    int index = 0;
    while (index < node.size && key.compareTo(node.keys.get(index)) > 0) {
        index++;
    }
    if (node.leaf) {
        // Node is a leaf, search for the key
        for (int i = 0; i < node.size; i++) {
            if (key.equals(node.keys.get(i))) {
                return node.fileOffsets.get(i);
            }
        }
        return -1; // Key not found
    } else {
        // Node is internal, continue searching in the appropriate child
        return search(node.children.get(index), key);
    }
}


// Inside your Bplus class
public static void display(Node node, int level) {
    if (node == null) {
        return;
    }
    System.out.print("Level: " + level + " Keys: ");
    for (int i = 0; i < node.size; i++) {
        System.out.print(formatKey(node.keys.get(i)) + " (Offset: " + node.fileOffsets.get(i) + ") ");
    }
    System.out.println();
    if (!node.leaf) {
        for (int i = 0; i <= node.size; i++) {
            display(node.children.get(i), level + 1);
        }
    } else if (node.nextLeaf != null) {
        display(node.nextLeaf, level);
    }
}
private static Node findLeafNode(Node node, String key) {
    int index = 0;
    // Find the child index where the key might be present
    while (index < node.size && key.compareTo(node.keys.get(index)) > 0) {
        index++;
    }
    if (node.leaf) {
        // If it's a leaf node, return the node
        return node;
    } else {
        // If it's an internal node, recursively search in the appropriate child
        return findLeafNode(node.children.get(index), key);
    }
}

private static void borrowFromLeftSibling(Node parent, int index) {
    Node node = parent.children.get(index);
    Node leftSibling = parent.children.get(index - 1);

    // Shift keys and fileOffsets in the node to create space for the borrowed key
    for (int i = node.size - 1; i >= 0; i--) {
        node.keys.add(i + 1, node.keys.get(i));
        node.fileOffsets.add(i + 1, node.fileOffsets.get(i));
    }

    // Shift children pointers in the node for non-leaf nodes
    if (!node.leaf) {
        for (int i = node.size; i >= 0; i--) {
            node.children.add(i + 1, node.children.get(i));
        }
    }
    // Copy the key and fileOffset from the left sibling to the node
    node.keys.set(0, leftSibling.keys.get(leftSibling.size - 1));
    node.fileOffsets.set(0, leftSibling.fileOffsets.get(leftSibling.size - 1));
    // Adjust the size of the node and the left sibling
    node.size++;
    leftSibling.size--;
    // Update the key in the parent that was borrowed
    parent.keys.set(index - 1, node.keys.get(0));
}

private static void borrowFromRightSibling(Node parent, int index) {
    Node node = parent.children.get(index);
    Node rightSibling = parent.children.get(index + 1);

    // Copy the key and fileOffset from the right sibling to the node
    node.keys.add(rightSibling.keys.get(0));
    node.fileOffsets.add(rightSibling.fileOffsets.get(0));

    // Shift keys and fileOffsets in the right sibling to cover the borrowed key
    for (int i = 0; i < rightSibling.size - 1; i++) {
        rightSibling.keys.set(i, rightSibling.keys.get(i + 1));
        rightSibling.fileOffsets.set(i, rightSibling.fileOffsets.get(i + 1));
    }
    // Shift children pointers in the right sibling for non-leaf nodes
    if (!rightSibling.leaf) {
        for (int i = 0; i < rightSibling.size; i++) {
            rightSibling.children.add(i, rightSibling.children.get(i + 1));
        }
    }
    node.size++;
    rightSibling.size--;
    // Update the key in the parent that was borrowed
    parent.keys.set(index, rightSibling.keys.get(0));
}

private static void mergeWithLeftSibling(Node parent, int index) {
    Node node = parent.children.get(index);
    Node leftSibling = parent.children.get(index - 1);

    // Copy keys and fileOffsets from the node to the left sibling
    for (int i = 0; i < node.size; i++) {
        leftSibling.keys.add(leftSibling.size + i, node.keys.get(i));
        leftSibling.fileOffsets.add(leftSibling.size + i, node.fileOffsets.get(i));
    }

    // Copy children pointers for non-leaf nodes
    if (!node.leaf) {
        for (int i = 0; i <= node.size; i++) {
            leftSibling.children.add(leftSibling.size + i, node.children.get(i));
        }
    }
    // Adjust the size of the left sibling
    leftSibling.size += node.size;
    // Remove the underflowed node from the parent's children array
    parent.children.remove(index);
    parent.size--;
    // Update the key in the parent
    parent.keys.set(index - 1, parent.children.get(index).keys.get(0));
}

private static void mergeWithRightSibling(Node parent, int index) {
    Node node = parent.children.get(index);
    Node rightSibling = parent.children.get(index + 1);

    // Copy keys and fileOffsets from the right sibling to the node
    for (int i = 0; i < rightSibling.size; i++) {
        node.keys.add(node.size + i, rightSibling.keys.get(i));
        node.fileOffsets.add(node.size + i, rightSibling.fileOffsets.get(i));
    }
    // Copy children pointers for non-leaf nodes
    if (!rightSibling.leaf) {
        for (int i = 0; i <= rightSibling.size; i++) {
            node.children.add(node.size + i, rightSibling.children.get(i));
        }
    }
    node.size += rightSibling.size;
    // Remove the right sibling from the parent's children array
    parent.children.remove(index + 1);
    parent.size--;
    // Update the key in the parent
    parent.keys.set(index, parent.children.get(index).keys.get(0));
}

// Rest of your code remains the same...
// Inside your Bplus class
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
                writeNodeToStream(outputStream, node.children.get(i));
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

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r")) {
            String line;
            // long offset = 0;
            long offset = randomAccessFile.getFilePointer();


            while ((line = randomAccessFile.readLine()) != null) {
                // bPlusTree.insert(line.trim(), offset);
                addNode(formatKey(line), offset);
                offset = randomAccessFile.getFilePointer();
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

        String indexFileName = "BPlusTreeIndex.dat";
        writeIndexToFile(indexFileName);

    } catch (IOException e) {
        e.printStackTrace();
    }
}
    public static String formatKey(String key) {
        if (key.length() == KEY_LENGTH) {
            return key;
        } else if (key.length() < KEY_LENGTH) {
            return String.format("%-" + KEY_LENGTH + "s", key);
        } else {
            return key.substring(0, KEY_LENGTH);
        }
    }
    // Rest of your code remains the same...
}
