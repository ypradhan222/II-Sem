import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
public class BPlusTree2 {
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

        static String keysToString(Node node) {
            StringBuilder result = new StringBuilder("[");
            for (int i = 0; i < node.size; i++) {
                result.append(node.keys[i]);
                if (i < node.size - 1) {
                    result.append(", ");
                }
            }
            result.append("]");
            return result.toString();
        }
    }

    static Node root;
    static int degree = 3;
    static final int KEY_LENGTH = 25;
   private static void insert(String key, long fileOffset) {
    System.out.println("\nInserting Key: " + key + ", File Offset: " + fileOffset);

    if (root == null) {
        Node leafNode = new Node(degree, true);
        insertIntoLeaf(leafNode, key, fileOffset);
        root = leafNode;
        System.out.println("Root: " + Node.keysToString(root));
        return;
    }

    Node currentNode = root;
    Node parent = null;
    int childIndex = -1;

    while (true) {
        if (currentNode.leaf || currentNode.size < 2 * degree - 1) {
            insertIntoLeaf(currentNode, key, fileOffset);
            System.out.println("Leaf Node: " + Node.keysToString(currentNode));
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
private static void insertIntoLeaf(Node leaf, String key, long fileOffset) {
    int index = leaf.size - 1;

    while (index >= 0 && key.compareTo(leaf.keys[index]) < 0) {
        leaf.keys[index + 1] = leaf.keys[index];
        leaf.fileOffsets[index + 1] = leaf.fileOffsets[index];
        index--;
    }

    index++;

    if (index < leaf.keys.length) {
        leaf.keys[index] = key;
        leaf.fileOffsets[index] = fileOffset;
        leaf.size++;
    } else {
        System.out.println("Leaf Node: " + Node.keysToString(leaf));
        System.out.println("Index: " + index);
        System.out.println("Key: " + key);
        System.out.println("File Offset: " + fileOffset);
        throw new ArrayIndexOutOfBoundsException("Index out of bounds");
    }
}


private static void splitNode(Node parentNode, int childIndex) {
    Node childNode = parentNode.children[childIndex];
    Node newNode = new Node(degree, childNode.leaf);

    int mid = childNode.size / 2;

    System.out.println("Splitting Node:");
    System.out.println("Child Node: " + Node.keysToString(childNode));
    System.out.println("New Node: " + Node.keysToString(newNode));

    // Copy keys and file offsets to the new node
    for (int i = 0; i < mid; i++) {
        newNode.keys[i] = childNode.keys[i + mid];
        newNode.fileOffsets[i] = childNode.fileOffsets[i + mid];
        newNode.size++; // Increment the size of the new node
    }

    // Move children references to the new node
    for (int i = 0; i < mid; i++) {
        newNode.children[i] = childNode.children[i + mid];
        childNode.children[i + mid] = null; // Set the original node's children references to null
    }

    // Update the size of the original node
    childNode.size = mid;

    // Insert the median key and file offset into the parent node
    insertIntoLeaf(parentNode, childNode.keys[childNode.size - 1], childNode.fileOffsets[childNode.size - 1]);
    insertIntoLeaf(parentNode, newNode.keys[0], newNode.fileOffsets[0]);

    // Adjust parent's children references
    parentNode.children[childIndex + 1] = newNode;
    newNode.parent = parentNode;

    System.out.println("After Split:");
    System.out.println("Child Node: " + Node.keysToString(childNode));
    System.out.println("New Node: " + Node.keysToString(newNode));
    System.out.println("Parent Node: " + Node.keysToString(parentNode));
}

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Bplus <filename>");
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
                        insert(formatKey(String.valueOf(key)), fileOffset);
                        fileOffset += line.length() + 1; // +1 for newline character
                    } catch (NumberFormatException e) {
                        System.out.println("Skipping invalid line: " + line);
                    }
                }
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


// public class Node {
//    boolean Leaf;
//    List<String> keys;
//    List<Node> children;
//    Node(boolean Leaf){
//       this.Leaf = Leaf;
//       this.keys = new ArrayList<>();
//       this.children = new ArrayList<>();
//    }
// }
// public class BplusTree2 {
//    private Node root;
//    private int order;
//    public BplusTree2(int order){
//       this.root = new Node(true);
//       this.order = order;
//    }
//    public void insert(String key){
//       insert(root,key);
//    }
//    private void insert(Node node,String node){
//       if(node.Leaf){

//       }
//    }
// }
// class BPlusTree2 {
//     private BPlusTreeNode root;
//     private int order;

//     public BPlusTree2(int order) {
//         this.root = new BPlusTreeNode(true);
//         this.order = order;
//     }

//     public void insert(String key) {
//         insert(root, key);
//     }

//     public class BPlusTreeNode {
//         boolean isLeaf;
//         List<String> keys;
//         List<BPlusTreeNode> children;

//         BPlusTreeNode(boolean isLeaf) {
//             this.isLeaf = isLeaf;
//             this.keys = new ArrayList<>();
//             this.children = new ArrayList<>();
//         }
//     }

//     private void insert(BPlusTreeNode node, String key) {
//         if (node.isLeaf) {
//             insertIntoLeaf(node, key);
//         } else {
//             int i = 0;
//             while (i < node.keys.size() && key.compareTo(node.keys.get(i)) > 0) {
//                 i++;
//             }
//             insert(node.children.get(i), key);
//         }

//         if (node.keys.size() == order) {
//             split(node);
//         }
//     }

//     private void insertIntoLeaf(BPlusTreeNode leaf, String key) {
//         int i = 0;
//         while (i < leaf.keys.size() && key.compareTo(leaf.keys.get(i)) > 0) {
//             i++;
//         }
//         leaf.keys.add(i, key);
//     }
//     private void split(BPlusTreeNode node) {
//     BPlusTreeNode newNode = new BPlusTreeNode(node.isLeaf);

//     int midIndex = node.keys.size() / 2;

//     newNode.keys.addAll(node.keys.subList(midIndex, node.keys.size()));
//     node.keys.subList(midIndex, node.keys.size()).clear();

//     if (!node.isLeaf) {
//         newNode.children.addAll(node.children.subList(midIndex, node.children.size()));
//         node.children.subList(midIndex, node.children.size()).clear();
//     }

//     if (node == root) {
//         BPlusTreeNode newRoot = new BPlusTreeNode(false);
//         newRoot.keys.add(newNode.keys.get(0)); // Fix: Use the first key of newNode
//         newRoot.children.add(node);
//         newRoot.children.add(newNode);
//         root = newRoot;
//     } else {
//         BPlusTreeNode parent = findParent(root, node);
//         int index = parent.children.indexOf(node);
//         parent.keys.add(index, newNode.keys.get(0)); // Fix: Use the first key of newNode
//         parent.children.add(index + 1, newNode);
//     }
// }


//     private BPlusTreeNode findParent(BPlusTreeNode current, BPlusTreeNode child) {
//         if (current.isLeaf || (current.children.contains(child) && !current.isLeaf)) {
//             return current;
//         } else {
//             int i = 0;
//             while (i < current.children.size() && child.keys.get(0).compareTo(current.keys.get(i)) > 0) {
//                 i++;
//             }
//             return findParent(current.children.get(i), child);
//         }
//     }

//     public static void main(String[] args) {
//         BPlusTree2 bPlusTree = new BPlusTree2(3); // Set the order according to your requirements
//         if (args.length != 1) {
//             System.out.println("Usage: java BPlusTreeFileIndex <filename>");
//             return;
//         }

//         String filename = args[0];

//         try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
//             String line;
//             long fileOffset = 0;

//             while ((line = reader.readLine()) != null) {
//                 if (!line.isEmpty()) {
//                     String key;
//                     try {
//                         key = line;
//                         bPlusTree.insert(key);
//                         fileOffset += line.length() + 1; // +1 for newline character
//                     } catch (NumberFormatException e) {
//                         System.out.println("Skipping invalid line: " + line);
//                     }
//                 }
//             }
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//     }
// }
