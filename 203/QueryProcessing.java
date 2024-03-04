import java.io.*;
import java.util.*;

public class QueryProcessing {

    public static String readFile(String path) {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                builder.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder.toString();
    }

    public static List<String[]> readCSV(String path) throws IOException {
        List<String[]> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",\\s*");
                data.add(values);
            }
        }
        System.out.println("Data from CSV file:");
        for (String[] row : data) {
            System.out.println(Arrays.toString(row));
        }
        return data;
    }

    public static List<String[]> select(List<String[]> tableData, String[] selectedColumns) {
        List<String[]> result = new ArrayList<>();
        Set<Integer> selectedIndexes = new HashSet<>();
        for (int i = 0; i < tableData.get(0).length; i++) {
            for (String column : selectedColumns) {
                if (tableData.get(0)[i].trim().equalsIgnoreCase(column.trim())) {
                    selectedIndexes.add(i);
                }
            }
        }
        for (String[] row : tableData) {
            String[] selectedRow = new String[selectedIndexes.size()];
            int idx = 0;
            for (int i : selectedIndexes) {
                selectedRow[idx++] = row[i];
            }
            result.add(selectedRow);
        }
        return result;
    }

    public static List<String[]> where(List<String[]> data, String condition) {
        List<String[]> filteredData = new ArrayList<>();
        String columnName = condition.split("=")[0].trim();
        String value = condition.split("=")[1].trim().replace("\"", "");

        int columnIndex = -1;
        for (int i = 0; i < data.get(0).length; i++) {
            if (data.get(0)[i].equalsIgnoreCase(columnName)) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex != -1) {
            for (int i = 1; i < data.size(); i++) {
                String columnValue = data.get(i)[columnIndex].trim();
                if (columnValue.equalsIgnoreCase(value)) {
                    filteredData.add(data.get(i));
                }
            }
        }
        return filteredData;
    }

    public static void from(String[] tokens, String[] attributes, List<String[]> data) {
        String joined = "";
        int index = 0;

        // Get the index of WHERE or ORDER
        if (Arrays.asList(tokens).contains("WHERE")) {
            index = Arrays.asList(tokens).indexOf("WHERE");
        } else if (Arrays.asList(tokens).contains("ORDER")) {
            index = Arrays.asList(tokens).indexOf("ORDER");
        } else {
            index = tokens.length;
        }

        // Joining two CSV files if provided or else retaining the old CSV file
        if (index > 1) {
            // Implement your join logic here
            joined = "joined.csv"; // Example filename
        } else if (index == 1) {
            // Assume the first element is the CSV filename
            joined = tokens[0];
        }

        // Go to WHERE or ORDER if present, otherwise process the join file
        if (Arrays.asList(tokens).contains("WHERE")) {
            index = Arrays.asList(tokens).indexOf("WHERE");
            List<String[]> whereData = where(data, tokens[index + 1]);
        } else if (Arrays.asList(tokens).contains("ORDER")) {
            index = Arrays.asList(tokens).indexOf("ORDER");
            orderBy(Arrays.copyOfRange(tokens, index + 2, tokens.length), joined);
        }

        // Read the data from the file based on WHERE or ORDER
        // List<String[]> data = readCSV("where.csv"); // Example filename

        // Limit function and write the output file
        if (Arrays.asList(tokens).contains("LIMIT")) {
            int limitIndex = Arrays.asList(tokens).indexOf("LIMIT");
            int limit = Integer.parseInt(tokens[limitIndex + 1]);
            List<String[]> limitedData = data.subList(0, limit);
            writeToFile("output.csv", limitedData);
        } else if (attributes == null) {
            writeToFile("output.csv", data);
        } else {
            writeToFile("output.csv", data, attributes);
        }
    }

    public static List<String[]> orderBy(String[] tokens, String joined) {
        List<String[]> data = null;
        try {
            data = readCSV(joined);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String columnName = tokens[0];
        String order = tokens[1];
        int columnIndex = -1;
        for (int i = 0; i < data.get(0).length; i++) {
            if (data.get(0)[i].equals(columnName)) {
                columnIndex = i;
                break;
            }
        }

        if (columnIndex == -1) {
            System.out.println("Column '" + columnName + "' not found.");
            return data;
        }

        final int finalColumnIndex = columnIndex;
        if (order.equals("ASC")) {
            data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex])));
        } else if (order.equals("DESC")) {
            data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex]), Comparator.reverseOrder()));
        }
        return data;
    }

    public static void writeToFile(String path, List<String[]> result) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
            for (String[] row : result) {
                bw.write(String.join(",", row));
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeToFile(String path, List<String[]> result, String[] attributes) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
            // Write header with selected attributes
            bw.write(String.join(",", attributes));
            bw.newLine();
            // Write data
            for (String[] row : result) {
                StringBuilder sb = new StringBuilder();
                for (String attr : attributes) {
                    sb.append(row[Arrays.asList(result.get(0)).indexOf(attr)]).append(",");
                }
                bw.write(sb.substring(0, sb.length() - 1)); // Remove trailing comma
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void select(String[] tokens, List<String[]> data) {
        int fromIndex = Arrays.asList(tokens).indexOf("FROM");
        if (fromIndex == 1 && tokens[0].equals("*")) { // Select everything
            // from(Arrays.copyOfRange(tokens, fromIndex + 1, tokens.length));
            from(Arrays.copyOfRange(tokens, fromIndex + 1, tokens.length), null, data);

        } else {
            List<String> fileAttr = new ArrayList<>();
            List<String> attributes = new ArrayList<>();

            for (int i = 0; i < fromIndex; i++) {
                if (tokens[i].contains(".")) { // If some csvfilename.attribute is there
                    String[] parts = tokens[i].split("\\.");
                    fileAttr.add(parts[0]); // Get file name
                    attributes.add(parts[1]); // Get the attribute
                } else {
                    fileAttr.add(""); // No file name specified
                    attributes.add(tokens[i]);
                }
            }
            from(Arrays.copyOfRange(tokens, fromIndex + 1, tokens.length), attributes.toArray(new String[0]), data);
        }
    }

    public static void main(String[] args) {
    Scanner scanner = new Scanner(System.in);
    System.out.print("Enter the evaluation statement: ");
    String statement = scanner.nextLine();
   //  List<String[]> data = null;

    if (!statement.isEmpty()) {
        String[] tokens = statement.split("\\s+");
        if (tokens[0].equals("SELECT") && statement.contains("FROM")) {
            String[] fileParts = tokens[tokens.length - 1].split("\\.");
            String fileName = fileParts[0] + ".csv"; // Ensure file extension is added
            String filePath = "./" + fileName; // Relative path to the CSV file
            
            // Check if the file exists
            File file = new File(filePath);
            if (file.exists()) {
                // Call the select method and pass the file path
                try {
                  List<String[]> data = readCSV(filePath);
                  select(Arrays.copyOfRange(tokens, 1, tokens.length), data);
                } catch (Exception e) {
                  System.out.println("Error reading file: " + e.getMessage());
                }
                
            } else {
                System.out.println("File not found: " + fileName);
            }

            // Additional checks for WHERE and ORDER conditions
            if (statement.contains("WHERE")) {
                // Perform actions for WHERE condition
                // For now, let's just print a message
                System.out.println("WHERE condition detected.");
            }
            if (statement.contains("ORDER")) {
                // Perform actions for ORDER condition
                // For now, let's just print a message
                System.out.println("ORDER /condition detected.");
            }
        } else {
            System.out.println("Invalid statement.");
        }

        // Additional cleanup code...
    } else {
        System.out.println("Please enter some query.");
    }
}

}
