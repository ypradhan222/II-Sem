import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeFileIndex {
    private static final int ORDER = 4;
    private BPlusTree<String, Long> bPlusTree;

    private BPlusTreeFileIndex() {
        this.bPlusTree = new BPlusTree<>(ORDER);
    }

    private void buildIndex(String filename) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            long fileOffset = 0;

            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    String formattedKey = formatKey(line);
                    bPlusTree.put(formattedKey, fileOffset);
                    fileOffset += line.length() + 1; // +1 for newline character
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveIndexToFile(String indexFileName) {
        try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(indexFileName))) {
            outputStream.writeObject(bPlusTree);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadIndexFromFile(String indexFileName) {
        try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(indexFileName))) {
            bPlusTree = (BPlusTree<String, Long>) inputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String formatKey(String key) {
        if (key.length() == 25) {
            return key;
        } else if (key.length() < 25) {
            return String.format("%-25s", key);
        } else {
            return key.substring(0, 25);
        }
    }

    private void displayIndex() {
        System.out.println("B+ Tree Index:");
        bPlusTree.display();
    }

    private Long searchKey(String key) {
        String formattedKey = formatKey(key);
        return bPlusTree.get(formattedKey);
    }

    public static void main(String[] args) {
        BPlusTreeFileIndex indexer = new BPlusTreeFileIndex();
        String inputFilename = "your_large_text_file.txt";  // Replace with the actual filename
        String indexFilename = "bplus_tree_index.ser";       // Replace with the desired index filename

        // Build B+ tree index
        indexer.buildIndex(inputFilename);

        // Save B+ tree index to a secondary file
        indexer.saveIndexToFile(indexFilename);

        // Load B+ tree index from the secondary file
        indexer.loadIndexFromFile(indexFilename);

        // Display B+ tree index
        indexer.displayIndex();

        // Search for a key in the B+ tree index
        String searchKey = "YourSearchKey";
        Long fileOffset = indexer.searchKey(searchKey);

        if (fileOffset != null) {
            System.out.println("File Offset for key '" + searchKey + "': " + fileOffset);
        } else {
            System.out.println("Key '" + searchKey + "' not found in the index.");
        }
    }
}
