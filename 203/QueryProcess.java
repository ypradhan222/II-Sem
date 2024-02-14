// import java.util.*;
// import java.io.*;
// public class QueryProcess{
//    public static String readFile(String path) {
//       StringBuilder builder = new StringBuilder();
//       try(BufferedReader br = new BufferedReader(new FileReader(path))) {
//          String line;
//          while((line = br.readLine())!= null){
//             builder.append(line).append("\n");
//          } 
//       }
//       catch(IOException e) {
//          e.printStackTrace();
//       }
//       return builder.toString();
//    }

// //    public static List<String[]> executeStatement(String pathcsv, String query) {
// //    String[] tokens = query.split("\\s+"); // Split based on whitespace characters
// //    String tableName = tokens[3].split("\\.")[0];
// //    String[] columns = tokens[1].equals("*") ? null : Arrays.copyOfRange(tokens, 1, tokens.length - 1);
// //    List<String[]> result = new ArrayList<>();
// //    try (BufferedReader br = new BufferedReader(new FileReader(pathcsv))) {
// //       String line;
// //       while ((line = br.readLine()) != null) {
// //          String[] values = line.split(",\\s*");
// //          if (columns != null) {
// //             List<String> selectedValues = new ArrayList<>();
// //             for (String column : columns) {
// //                int index = Arrays.asList(values).indexOf(column);
// //                if (index != -1) {
// //                   selectedValues.add(values[index]);
// //                }
// //             }
// //             result.add(selectedValues.toArray(new String[0]));
// //          } else {
// //             result.add(values);
// //          }
// //       }
// //    } catch (IOException e) {
// //       e.printStackTrace();
// //    }
// //    return result;
// // }
//    public static List<String[]> executeStatement(String[] paths, String query) {
//       List<String[]> result = new ArrayList<>();
//       String[] tokens = query.split("\\s+");
//       String[] columns = tokens[1].equals("*") ? null : Arrays.copyOfRange(tokens, 1, tokens.length - 1);
      
//       try {
//          if (query.contains("JOIN")) {
//             // Process join operation if JOIN clause is present
//             // Assuming simple INNER JOIN operation for illustration
//             String[] joinTokens = query.split("JOIN");
//             String table1Name = joinTokens[0].split("\\s+")[3].split("\\.")[0].trim();
//             String table2Name = joinTokens[1].split("\\s+")[0].trim();
            
//             // Read data from CSV files
//             List<String[]> table1Data = readCSV(paths[0]);
//             List<String[]> table2Data = readCSV(paths[1]);
//             // Perform JOIN operation
//             for (String[] row1 : table1Data) {
//                for (String[] row2 : table2Data) {
//                    // Check join condition, assuming simple equality condition for demonstration
//                   if (row1[0].equals(row2[0])) { // Assuming first column is the join column
//                      String[] joinedRow = new String[row1.length + row2.length];
//                      System.arraycopy(row1, 0, joinedRow, 0, row1.length);
//                      System.arraycopy(row2, 0, joinedRow, row1.length, row2.length);
//                      result.add(joinedRow);
//                   }
//                }
//             }
//          } else {
//              // Process single CSV file if no JOIN clause is present
//             String pathcsv = paths[0];
//             result = readCSV(pathcsv);
//          }
//       } catch (IOException e) {
//          e.printStackTrace();
//       }
//       return result;
//    }

//    public static List<String[]> readCSV(String path) throws IOException {
//       List<String[]> data = new ArrayList<>();
//       try (BufferedReader br = new BufferedReader(new FileReader(path))) {
//          String line;
//          while ((line = br.readLine()) != null) {
//             String[] values = line.split(",\\s*");
//             data.add(values);
//          }
//       }
//       return data;
//    }
//    public static void writeToFile(String path, List<String[]> result){
//       try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
//          for(String[] row: result){
//             bw.write(String.join(",",row));
//             bw.newLine();
//          }
//       } catch (Exception e) {
//          e.printStackTrace();
//       }
//    }
   
//    public static void main(String[] args) {
//       if(args.length<2 || args.length >3){
//          System.out.println("Usage: java QueryProcess <csvFile> <csvfile2> <sqlquery>");
//          System.exit(1);
//       }
//       // String csvFile = "/home/dmacs-5/Desktop/II Sem/203/large_data.csv";

//       // String sqlFile = "/home/dmacs-5/Desktop/II Sem/203/sqlquery.txt";
//       String[] csvFiles;
//       String sqlFile;
//       if(args.length == 3){
//          csvFiles = new String[]{args[0],args[1]};
//          sqlFile = args[2];
//       }
//       else{
//          csvFiles = new String[]{args[0]};
//          sqlFile = args[1];
//       }
//       String output = "/home/dmacs-5/Desktop/II Sem/203/output2.csv";
//       String query = readFile(sqlFile);
//       List<String[]> result = executeStatement(csvFiles, query);
//       writeToFile(output, result);
//    }
// }
// import java.util.*;
// import java.io.*;

// public class QueryProcess {

//     public static String readFile(String path) {
//         StringBuilder builder = new StringBuilder();
//         try (BufferedReader br = new BufferedReader(new FileReader(path))) {
//             String line;
//             while ((line = br.readLine()) != null) {
//                 builder.append(line).append("\n");
//             }
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//         return builder.toString();
//     }

//     public static List<String[]> executeStatement(String[] paths, String query) {
//         List<String[]> result = new ArrayList<>();
//         String[] tokens = query.split("\\s+");
//         String[] columns = tokens[1].equals("*") ? null : Arrays.copyOfRange(tokens, 1, tokens.length - 1);
//         try {
//          if (query.contains("JOIN")) {
//                 // JOIN operation
//               // Your existing code for JOIN operation
//             String[] joinTokens = query.split("JOIN");
//             String table1Name = joinTokens[0].split("\\s+")[3].split("\\.")[0].trim();
//             String table2Name = joinTokens[1].split("\\s+")[0].trim() ;       
//             // Read data from CSV files
//             List<String[]> table1Data = readCSV(paths[0]);
//             List<String[]> table2Data = readCSV(paths[1]);
//             // Perform JOIN operation
//             for (String[] row1 : table1Data) {
//                for (String[] row2 : table2Data) {
//                    // Check join condition, assuming simple equality condition for demonstration
//                   if (row1[0].equals(row2[0])) { // Assuming first column is the join column
//                      String[] joinedRow = new String[row1.length + row2.length];
//                      System.arraycopy(row1, 0, joinedRow, 0, row1.length);
//                      System.arraycopy(row2, 0, joinedRow, row1.length, row2.length);
//                      result.add(joinedRow);
//                   }
//                }
//             }
//             } else {
//                 // Single CSV file operation
//                 String pathcsv = paths[0];
//                 List<String[]> tableData = readCSV(pathcsv);
//                 System.out.println("CSV Data:");
//                 for (String[] row : tableData) {
//                   System.out.println(Arrays.toString(row));
//                 }
//                 // Check if tableData is empty
//                 if (tableData.isEmpty()) {
//                     System.out.println("CSV file is empty: " + pathcsv);
//                     return result;
//                 }
//                 // Applying WHERE clause
//                 if (query.contains("WHERE")) {
//                     String condition = query.split("WHERE")[1].trim();
//                     String columnName = condition.split("=")[0].trim();
//                     String value = condition.split("=")[1].trim().replace("\"", "");
//                     System.out.println("Applying WHERE clause: " + columnName + " = " + value);
//                     tableData = filterData(tableData, columnName, value);
//                     // System.out.println("Filtered Data:");
//                     for (String[] row : tableData) {
//                     System.out.println(Arrays.toString(row));
//                   }
//                   }
//                 // Applying ORDER BY clause
//                 if (query.contains("ORDER BY")) {
//                     String orderBy = query.split("ORDER BY")[1].trim();
//                     String orderColumn = orderBy.split("\\s+")[0].trim();
//                     String sortOrder = orderBy.split("\\s+")[1].trim();
//                     System.out.println("Applying ORDER BY clause: " + orderColumn + " " + sortOrder);
//                     tableData = sortData(tableData, orderColumn, sortOrder);
//                System.out.println("Sorted Data:");
//                for (String[] row : tableData) {
//                   System.out.println(Arrays.toString(row));
//                   }
//                 result = tableData;
//             }
//         } 
//       }catch (IOException e) {
//             e.printStackTrace();
//         }
//         return result;
//     }

//     public static List<String[]> readCSV(String path) throws IOException {
//         List<String[]> data = new ArrayList<>();
//         try (BufferedReader br = new BufferedReader(new FileReader(path))) {
//             String line;
//             while ((line = br.readLine()) != null) {
//                 String[] values = line.split(",\\s*");
//                 data.add(values);
//             }
//         }
//         return data;
//     }

//     public static List<String[]> filterData(List<String[]> data, String columnName, String value) {
//     List<String[]> filteredData = new ArrayList<>();
//     int columnIndex = -1;
//     System.out.println("Filtering data on column: " + columnName + ", with value: " + value);
//     for (int i = 0; i < data.get(0).length; i++) {
//         if (data.get(0)[i].equalsIgnoreCase(columnName)) {
//             columnIndex = i;
//             break;
//         }
//     }
//     System.out.println("Column index found: " + columnIndex);
//     if (columnIndex != -1) {
//         for (int i = 1; i < data.size(); i++) {
//             System.out.println("Checking value: " + data.get(i)[columnIndex]);
//             String classValue = data.get(i)[columnIndex].trim(); // Trim leading and trailing whitespace
//             if (classValue.equalsIgnoreCase(value)) { // Perform case-insensitive comparison
//                 filteredData.add(data.get(i));
//             }
//         }
//     }
//     System.out.println("Filtered Data:");
//     for(String[] row : filteredData){
//         System.out.println(Arrays.toString(row));
//     }
//     return filteredData;
// }

// /* 
//     public static List<String[]> sortData(List<String[]> data, String columnName, String order) {
//         int columnIndex = -1;
//     //     if(!data.isEmpty()){
//     //     for (int i = 0; i < data.get(0).length; i++) {
//     //         if (data.get(0)[i].equals(columnName)) {
//     //             columnIndex = i;
//     //             break;
//     //         }
//     //     }
//     //   }
//     //   else{
//     //      System.out.println("Empty Data.Cannot Sort");
//     //      return data;
//     //   }
//         if(data.size()<=1){
//             System.out.println("Empty Data.Cannot Sort");
//             return data;
//         }
//         for(int i=0; i<data.get(0).length;i++){
//             if(data.get(0)[i].equals(columnName)){
//                 columnIndex =i;
//                 break;
//             }
//         }
//         if(columnIndex==-1){
//             System.out.println("Column '" + columnName + "' not found.");
//             return data;
//         }
//         boolean allNumbers = true;
//         for (int i = 1; i < data.size(); i++) {
//         try {
//             Integer.parseInt(data.get(i)[columnIndex]);
//         } catch (NumberFormatException e) {
//             allNumbers = false;
//             break;
//         }
//         }
//         if (!allNumbers) {
//             System.out.println("Values in column '" + columnName + "' are not all numbers. Cannot sort.");
//             return data;
//         }
//         final int finalColumnIndex = columnIndex; // Capture columnIndex in a final variable
//         // if (finalColumnIndex != -1) {
//         //     if (order.equals("ASC")) {
//         //         data.subList(1, data.size()).sort(Comparator.comparing(row -> row[finalColumnIndex]));
//         //     } else if (order.equals("DESC")) {
//         //         data.subList(1, data.size()).sort(Comparator.comparing(row -> row[finalColumnIndex], Comparator.reverseOrder()));
//         //     }
//         // } else {
//         //     System.out.println("Column '" + columnName + "' not found.");
//         // }
//         if (order.equals("ASC")) {
//             data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex])));
//         } else if (order.equals("DESC")) {
//             data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex]), Comparator.reverseOrder()));
//         }
//         return data;
//     }
// */
//     public static List<String[]> sortData(List<String[]> data, String columnName, String order) {
//     // Check if data contains only header row
//     if (data.size() <= 1) {
//         System.out.println("Empty Data. Cannot Sort");
//         return data;
//     }

//     int columnIndex = -1;
//     // Find the column index
//     for (int i = 0; i < data.get(0).length; i++) {
//         if (data.get(0)[i].equals(columnName)) {
//             columnIndex = i;
//             break;
//         }
//     }

//     if (columnIndex == -1) {
//         System.out.println("Column '" + columnName + "' not found.");
//         return data;
//     }

//     // Check if all values in the column are numbers
//     boolean allNumbers = true;
//     for (int i = 1; i < data.size(); i++) {
//         try {
//             Integer.parseInt(data.get(i)[columnIndex]);
//         } catch (NumberFormatException e) {
//             allNumbers = false;
//             break;
//         }
//     }
//     if (!allNumbers) {
//         System.out.println("Values in column '" + columnName + "' are not all numbers. Cannot sort.");
//         return data;
//     }

//     // Capture columnIndex in a final variable
//     final int finalColumnIndex = columnIndex;

//     // Sort the data based on the specified column and order
//     if (order.equals("ASC")) {
//         data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex])));
//     } else if (order.equals("DESC")) {
//         data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex]), Comparator.reverseOrder()));
//     }

//     return data;
// }

//     public static void writeToFile(String path, List<String[]> result) {
//         try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
//             for (String[] row : result) {
//                 bw.write(String.join(",", row));
//                 bw.newLine();
//             }
//             System.out.println("Data written to file: " + path); 
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     }

//     public static void main(String[] args) {
//         if (args.length < 2 || args.length > 3) {
//             System.out.println("Usage: java QueryProcess <csvFile> [<csvfile2>] <sqlquery>");
//             System.exit(1);
//         }
//         String[] csvFiles;
//         String sqlFile;
//         if (args.length == 3) {
//             csvFiles = new String[]{args[0], args[1]};
//             sqlFile = args[2];
//         } else {
//             csvFiles = new String[]{args[0]};
//             sqlFile = args[1];
//         }
//         String output = "/home/dmacs-5/Desktop/II Sem/203/output3.csv";
//         String query = readFile(sqlFile);
//         List<String[]> result = executeStatement(csvFiles, query);
//         writeToFile(output, result);
//     }
// }
import java.io.*;
import java.util.*;

public class QueryProcess {

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

    public static List<String[]> executeStatement(String[] paths, String query) {
        List<String[]> result = new ArrayList<>();
        String[] tokens = query.split("\\s+");
        String[] columns = tokens[1].equals("*") ? null : Arrays.copyOfRange(tokens, 1, tokens.length - 1);
        try {
            if (query.contains("JOIN")) {
                // JOIN operation
                String[] joinTokens = query.split("JOIN");
                String table1Name = joinTokens[0].split("\\s+")[3].split("\\.")[0].trim();
                String table2Name = joinTokens[1].split("\\s+")[0].trim();
                // Read data from CSV files
                List<String[]> table1Data = readCSV(paths[0]);
                List<String[]> table2Data = readCSV(paths[1]);
                // Perform JOIN operation
                for (String[] row1 : table1Data) {
                    for (String[] row2 : table2Data) {
                        if (row1[0].equals(row2[0])) { // Assuming first column is the join column
                            String[] joinedRow = new String[row1.length + row2.length];
                            System.arraycopy(row1, 0, joinedRow, 0, row1.length);
                            System.arraycopy(row2, 0, joinedRow, row1.length, row2.length);
                            result.add(joinedRow);
                        }
                    }
                }
            } else {
                // Single CSV file operation
                String pathcsv = paths[0];
                List<String[]> tableData = readCSV(pathcsv);
                // Applying WHERE clause
                if (query.contains("WHERE")) {
                    String condition = query.split("WHERE")[1].trim();
                    String columnName = condition.split("=")[0].trim();
                    String value = condition.split("=")[1].trim().replace("\"", "");
                    tableData = filterData(tableData, columnName, value);
                }
                // Applying ORDER BY clause
                if (query.contains("ORDER BY")) {
                    String orderBy = query.split("ORDER BY")[1].trim();
                    String orderColumn = orderBy.split("\\s+")[0].trim();
                    String sortOrder = orderBy.split("\\s+")[1].trim();
                    tableData = sortData(tableData, orderColumn, sortOrder);
                }
                result = tableData;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
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

    public static List<String[]> filterData(List<String[]> data, String columnName, String value) {
        List<String[]> filteredData = new ArrayList<>();
        int columnIndex = -1;
        for (int i = 0; i < data.get(0).length; i++) {
            if (data.get(0)[i].equalsIgnoreCase(columnName)) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex != -1) {
            for (int i = 1; i < data.size(); i++) {
                String classValue = data.get(i)[columnIndex].trim(); // Trim leading and trailing whitespace
                if (classValue.equalsIgnoreCase(value.trim())) { // Perform case-insensitive comparison
                    filteredData.add(data.get(i));
                }
            }
        }
        return filteredData;
    }
    public static List<String[]> sortData(List<String[]> data, String columnName, String order) {
    // Debugging: Print the contents of the data list before sorting
    System.out.println("Data before sorting:");
    for (String[] row : data) {
        System.out.println(Arrays.toString(row));
    }

    // Find the column index
    int columnIndex = -1;
    for (int i = 0; i < data.get(0).length; i++) {
        if (data.get(0)[i].equals(columnName)) {
            columnIndex = i;
            break;
        }
    }

    // Debugging: Print the column index
    System.out.println("Column index for '" + columnName + "': " + columnIndex);

    // Check if column index was found
    if (columnIndex == -1) {
        System.out.println("Column '" + columnName + "' not found.");
        return data;
    }
    final int finalColumnIndex = columnIndex;
    // Sort the data based on the specified column and order
    try {
        if (order.equals("ASC")) {
            data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex])));
        } else if (order.equals("DESC")) {
            data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex]), Comparator.reverseOrder()));
        }
    } catch (NumberFormatException e) {
        System.out.println("Values in column '" + columnName + "' are not all numbers. Cannot sort.");
    }

    // Debugging: Print the contents of the data list after sorting
    System.out.println("Data after sorting:");
    for (String[] row : data) {
        System.out.println(Arrays.toString(row));
    }

    return data;
}




//     public static List<String[]> sortData(List<String[]> data, String columnName, String order) {
//         // Check if data contains only header row
//         if (data.size() <= 1) {
//             System.out.println("Empty Data. Cannot Sort");
//             return data;
//         }

//         int columnIndex = -1;
//         // Find the column index
//         for (int i = 0; i < data.get(0).length; i++) {
//             if (data.get(0)[i].equals(columnName)) {
//                 columnIndex = i;
//                 break;
//             }
//         }

//         if (columnIndex == -1) {
//             System.out.println("Column '" + columnName + "' not found.");
//             return data;
//         }

//         // Check if all values in the column are numbers
//         boolean allNumbers = true;
//         for (int i = 1; i < data.size(); i++) {
//             try {
//                 Integer.parseInt(data.get(i)[columnIndex]);
//             } catch (NumberFormatException e) {
//                 allNumbers = false;
//                 break;
           
//                 }
//         if (!allNumbers) {
//             System.out.println("Values in column '" + columnName + "' are not all numbers. Cannot sort.");
//             return data;
//         }

//         // Capture columnIndex in a final variable
//         final int finalColumnIndex = columnIndex;

//         // Sort the data based on the specified column and order
//         if (order.equals("ASC")) {
//             data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex])));
//         } else if (order.equals("DESC")) {
//             data.subList(1, data.size()).sort(Comparator.comparing(row -> Integer.parseInt(row[finalColumnIndex]), Comparator.reverseOrder()));
//         }
//     }
//         return data;
    
// }
    public static void writeToFile ( String path , List<String[]> result) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
            for (String[] row : result) {
                bw.write(String.join(",", row));
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: java QueryProcess <csvFile> [<csvfile2>] <sqlquery>");
            System.exit(1);
        }
        String[] csvFiles;
        String sqlFile;
        if (args.length == 3) {
            csvFiles = new String[]{args[0], args[1]};
            sqlFile = args[2];
        } else {
            csvFiles = new String[]{args[0]};
            sqlFile = args[1];
        }
        String output = "/home/dmacs-5/Desktop/II Sem/203/output3.csv";
        String query = readFile(sqlFile);
        List<String[]> result = executeStatement(csvFiles, query);

        // Print result to verify data before writing to file
        for (String[] row : result) {
            System.out.println(Arrays.toString(row));
        }

        writeToFile(output, result);

    }

}