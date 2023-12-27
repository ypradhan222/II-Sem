import java.util.LinkedList;
import java.util.List;

public class BPTree {

    private BPTreeLeaf root;
    private final int capacity;

    public BPTree(int capacity) {
        if (capacity < 2) {
            capacity = 2;
        }
        this.root = new BPTreeLeaf(new LinkedList<>(), null, capacity);
        this.capacity = capacity;
    }

    public void readFromFile(String inputFile) {
        // Implement the logic to read from the file
    }

    public void insert(int key) {
        BPTreeInsertResult result = root.insert(key);
        if (result.promotedKey != null) {
            BPTreeNode newRoot = new BPTreeNode(
                List.of(result.promotedKey),
                List.of(root, result.newNode),
                capacity
            );
            root = newRoot;
        }
    }

    public List<Integer> keys() {
        return root.keys();
    }

    public boolean find(int key) {
        return root.find(key);
    }

    public int numNodes() {
        return root.numNodes();
    }

    public int numLeaves() {
        return root.numLeaves();
    }

    public int numKeys() {
        return root.numKeys();
    }

    public int height() {
        return root.height() + 1;
    }

    public void display() {
        // Implement the logic to display the B+ tree
    }
}

class BPTreeLeaf {

    private List<Integer> keys;
    private BPTreeLeaf next;
    private final int capacity;

    public BPTreeLeaf(List<Integer> keys, BPTreeLeaf next, int capacity) {
        this.keys = keys;
        this.next = next;
        this.capacity = capacity;
    }

    public List<Integer> keys() {
        List<Integer> allKeys = new LinkedList<>(keys);
        BPTreeLeaf current = this;
        while (current.next != null) {
            current = current.next;
            allKeys.addAll(current.keys);
        }
        return allKeys;
    }

    public boolean find(int key) {
        return keys.contains(key);
    }

    public BPTreeInsertResult insert(int key) {
        int index = binarySearch(keys, key);
        if (index == keys.size() || !keys.get(index).equals(key)) {
            keys.add(index, key);
        }

        int splitIndex = (capacity + 1) / 2;

        if (keys.size() > capacity) {
            BPTreeLeaf newLeaf = new BPTreeLeaf(
                new LinkedList<>(keys.subList(splitIndex, keys.size())),
                next,
                capacity
            );

            keys.subList(splitIndex, keys.size()).clear();
            next = newLeaf;

            return new BPTreeInsertResult(newLeaf.keys.get(0), newLeaf);
        }

        return new BPTreeInsertResult(null, null);
    }

    public int numNodes() {
        return 0;
    }

    public int numLeaves() {
        return 1;
    }

    public int numKeys() {
        return keys.size();
    }

    public int height() {
        return 0;
    }
}

class BPTreeNode {

    private List<Integer> keys;
    private List<BPTreeLeaf> pointers;
    private final int capacity;

    public BPTreeNode(List<Integer> keys, List<BPTreeLeaf> pointers, int capacity) {
        this.keys = keys;
        this.pointers = pointers;
        this.capacity = capacity;
    }

    public List<Integer> keys() {
        return pointers.get(0).keys();
    }

    public boolean find(int key) {
        int index = binarySearch(keys, key);
        return pointers.get(index).find(key);
    }

    public BPTreeInsertResult insert(int key) {
        int index = binarySearch(keys, key);
        BPTreeInsertResult result = pointers.get(index).insert(key);

        if (result.promotedKey != null) {
            int position = binarySearch(keys, result.promotedKey);
            BPTreeInsertResult nodeInsertResult = insertHere(position, result.promotedKey, result.newNode);
            if (nodeInsertResult.promotedKey != null) {
                return nodeInsertResult;
            }
        }

        return new BPTreeInsertResult(null, null);
    }

    public int numNodes() {
        return 1 + pointers.stream().mapToInt(BPTreeLeaf::numNodes).sum();
    }

    public int numLeaves() {
        return pointers.stream().mapToInt(BPTreeLeaf::numLeaves).sum();
    }

    public int numKeys() {
        return pointers.stream().mapToInt(BPTreeLeaf::numKeys).sum();
    }

    public int height() {
        return 1 + pointers.get(0).height();
    }

    private BPTreeInsertResult insertHere(int position, int key, BPTreeLeaf pointer) {
        keys.add(position, key);
        pointers.add(position + 1, pointer);

        int splitIndex = (capacity + 1) / 2;
        if (keys.size() > capacity) {
            int promotedKey = keys.get(splitIndex);
            keys.remove(splitIndex);
            BPTreeNode newLateralNode = new BPTreeNode(
                new LinkedList<>(keys.subList(splitIndex, keys.size())),
                new LinkedList<>(pointers.subList(splitIndex + 1, pointers.size())),
                capacity
            );
            keys.subList(splitIndex, keys.size()).clear();
            pointers.subList(splitIndex + 1, pointers.size()).clear();

            return new BPTreeInsertResult(promotedKey, newLateralNode);
        }

        return new BPTreeInsertResult(null, null);
    }
}

class BPTreeInsertResult {
    public Integer promotedKey;
    public BPTreeLeaf newNode;

    public BPTreeInsertResult(Integer promotedKey, BPTreeLeaf newNode) {
        this.promotedKey = promotedKey;
        this.newNode = newNode;
    }
}

// Utility method for binary search
int binarySearch(List<Integer> list, int key) {
    int low = 0;
    int high = list.size() - 1;

    while (low <= high) {
        int mid = (low + high) / 2;
        int midValue = list.get(mid);

        if (midValue == key) {
            return mid;
        } else if (midValue < key) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    return low;
}
