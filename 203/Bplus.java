import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Bplus {

    static class Node {
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

    // Ensure there is enough space in the node
    if (node.size < node.keys.length && node.size < node.fileOffsets.length) {
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
    } else {
        // Handle the case where the node is full (split logic)
        splitNode(node, key, fileOffset);
    }
}
   private static void splitNode(Node parentNode, String key, long fileOffset) {
    int childIndex = 0;
    while (childIndex < parentNode.size && key.compareTo(parentNode.keys[childIndex]) > 0) {
        childIndex++;
    }

    Node childNode = parentNode.children[childIndex];

    // Check if childNode is not null
    if (childNode != null) {
        Node newNode = new Node(degree, childNode.leaf);

        int mid = childNode.size / 2;

        for (int i = mid + 1; i < childNode.size; i++) {
            newNode.keys[i - mid - 1] = childNode.keys[i];
            newNode.fileOffsets[i - mid - 1] = childNode.fileOffsets[i];
            newNode.children[i - mid - 1] = childNode.children[i];
            childNode.children[i] = null;
        }

        newNode.size = childNode.size - mid - 1;
        childNode.size = mid;

        newNode.children[newNode.size] = childNode.children[childNode.size];
        childNode.children[childNode.size] = null;

        if (key.compareTo(childNode.keys[mid]) < 0) {
            insertData(childNode, key, fileOffset);
        } else {
            insertData(newNode, key, fileOffset);
        }

        // Insert the median key into the parent node
        insertData(parentNode, childNode.keys[mid], childNode.fileOffsets[mid]);

        // Connect the new node to the parent
        parentNode.children[childIndex + 1] = newNode;
        newNode.parent = parentNode;

        if (parentNode.size == 2 * degree - 1) {
            // If the parent is also full, recursively split it
            splitNode(parentNode.parent, childNode.keys[mid], childNode.fileOffsets[mid]);
        }
    } else {
        // Handle the case where childNode is null (create a new leaf node)
        Node newLeafNode = new Node(degree, true);
        insertData(newLeafNode, key, fileOffset);
        parentNode.children[childIndex] = newLeafNode;
    }
}




private static void add(String key, long fileOffset) {
    System.out.println("\nInserting Key: " + key + ", File Offset: " + fileOffset);

    if (root == null) {
        Node leafNode = new Node(degree, true);
        insertData(leafNode, key, fileOffset);
        root = leafNode;
        return;
    }

    Node currentNode = root;

    while (true) {
        if (currentNode.leaf || currentNode.size < 2 * degree - 1) {
            insertData(currentNode, key, fileOffset);
            break;
        }

        int index = currentNode.size - 1;
        while (index >= 0 && key.compareTo(currentNode.keys[index]) < 0) {
            index--;
        }

        Node nextNode = currentNode.children[index + 1];

        if (nextNode.size == 2 * degree - 1) {
            splitNode(currentNode, key, fileOffset);
            if (key.compareTo(currentNode.keys[index]) > 0) {
                nextNode = currentNode.children[index + 1];
            }
        }

        currentNode = nextNode;
    }
}


//     private static void add(String key, long fileOffset) {
//     System.out.println("\nInserting Key: " + key + ", File Offset: " + fileOffset);

//     if (root == null) {
//         Node leafNode = new Node(degree, true);
//         insertData(leafNode, key, fileOffset);
//         root = leafNode;
//         return;
//     }

//     Node currentNode = root;
//     Node parent = null;
//     int childIndex = -1;

//     while (true) {
//         if (currentNode.leaf || currentNode.size < 2 * degree - 1) {
//             insertData(currentNode, key, fileOffset);
//             break;
//         }

//         parent = currentNode;
//         childIndex = 0;

//         while (childIndex < currentNode.size && key.compareTo(currentNode.keys[childIndex]) > 0) {
//             childIndex++;
//         }

//         Node nextNode = currentNode.children[childIndex];

//         if (nextNode.size == 2 * degree - 1) {
//             splitNode(currentNode,  key, fileOffset); // Updated splitNode call
//             if (key.compareTo(currentNode.keys[childIndex]) > 0) {
//                 nextNode = currentNode.children[childIndex + 1];
//             }
//         }

//         currentNode = nextNode;

//         if (parent != null) {
//             while (parent != null) {
//                 if (currentNode.size == 2 * degree - 1) {
//                     int index = 0;
//                     while (index < parent.size && currentNode.keys[0].compareTo(parent.keys[index]) > 0) {
//                         index++;
//                     }

//                     splitNode(parent, null, 0); // Updated splitNode call

//                     currentNode = parent;
//                     parent = currentNode.parent;
//                 }
//             }
//         } else {
//             Node newRoot = new Node(degree, false);
//             insertData(newRoot, currentNode.keys[0], currentNode.fileOffsets[0]);
//             newRoot.children[0] = root;
//             newRoot.children[1] = currentNode;
//             root.parent = newRoot;
//             currentNode.parent = newRoot;
//             root = newRoot;
//             break;
//         }
//     }
// }


    public static void display(Node node, int level) {
        if (node == null) {
            return;
        }

        System.out.print("Level: " + level + " Keys: ");
        for (int i = 0; i < node.size; i++) {
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

        display(root, 2);

        // Example: Search for a key
        String searchKey = "yogen"; // Replace with the key you want to search
        long searchResult = search(searchKey);

        if (searchResult != -1) {
            System.out.println("Key: " + searchKey + " found at offset: " + searchResult);
        } else {
            System.out.println("Key: " + searchKey + " not found");
        }

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
}
