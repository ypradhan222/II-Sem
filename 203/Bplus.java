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

    while (index >= 0 && key.compareTo(node.keys[index]) < 0) {
        node.keys[index + 1] = node.keys[index];
        node.fileOffsets[index + 1] = node.fileOffsets[index];

        if (!node.leaf) {
            // Shift child references to the right
            if (index + 1 < node.children.length) {
                node.children[index + 2] = node.children[index + 1];
            }
        }

        index--;
    }

    // Increment the index before using it to insert the new data
    index++;

    node.keys[index] = key;
    node.fileOffsets[index] = fileOffset;

    if (!node.leaf) {
        // Adjust child references
        if (index + 1 < node.children.length) {
            node.children[index + 1] = null; // Set the new child reference to null
        }
    }

    node.size++;
}

private static void splitNode(Node parentNode, int childIndex) {
    Node childNode = parentNode.children[childIndex];
    Node newNode = new Node(degree, childNode.leaf);

    int mid = childNode.size / 2;

    for (int i = mid; i < childNode.size; i++) {
        newNode.keys[i - mid] = childNode.keys[i];
        newNode.fileOffsets[i - mid] = childNode.fileOffsets[i];
        newNode.children[i - mid] = childNode.children[i];
        childNode.children[i] = null; // Set child references to null in the original node
    }

    newNode.size = childNode.size - mid;
    childNode.size = mid;

    newNode.children[newNode.size] = childNode.children[childNode.size];
    childNode.children[childNode.size] = null; // Set the last child reference to null in the original node

    // Insert the median key into the parent node
    insertData(parentNode, childNode.keys[childNode.size - 1], childNode.fileOffsets[childNode.size - 1]);

    // Insert the first key of the new node into the parent node
    insertData(parentNode, newNode.keys[0], newNode.fileOffsets[0]);

    parentNode.children[childIndex] = newNode;
    newNode.parent = parentNode;
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
    Node parent = null;
    int childIndex = -1;

    while (true) {
        if (currentNode.leaf || currentNode.size < 2 * degree - 1) {
            insertData(currentNode, key, fileOffset);
            break;
        }

        parent = currentNode;
        childIndex = 0;

        while (childIndex < currentNode.size && key.compareTo(currentNode.keys[childIndex]) > 0) {
            childIndex++;
        }

        Node nextNode = currentNode.children[childIndex];

        if (nextNode.size == 2 * degree - 1) {
            splitNode(currentNode, childIndex);
            if (key.compareTo(currentNode.keys[childIndex]) > 0) {
                nextNode = currentNode.children[childIndex + 1];
            }
        }

        currentNode = nextNode;
    }

    if (parent != null) {
        while (parent != null) {
            if (currentNode.size == 2 * degree - 1) {
                int index = 0;
                while (index < parent.size && currentNode.keys[0].compareTo(parent.keys[index]) > 0) {
                    index++;
                }

                splitNode(parent, index);

                currentNode = parent;
                parent = currentNode.parent;

                if (parent == null) {
                    break;
                }
            } else {
                break;
            }
        }
    }
}

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
