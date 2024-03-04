import java.io.*;
import java.util.*;
public class QueryProcess3 {

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
    public static List<String> splitHead(String csvFile){
         List<String> headerList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // Read the first line which contains headers
            String headerLine = br.readLine();
            if (headerLine != null) {
                // Split the header line by comma (assuming CSV format)
                String[] headers = headerLine.split(",");
                // Split each header by space and add to the header list
                for (String header : headers) {
                    headerList.add(header.trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return headerList;
    }
    
    public static List<String[]> executeStatement(String[] paths, String query) {
        List<String[]> result = new ArrayList<>();
        String[] tokens = query.split("\\s+");
        String[] columns;
        boolean selectAll = false;

        // Check if SELECT clause is present and parse columns
        if (query.contains("SELECT")) {
            int selectIndex = Arrays.asList(tokens).indexOf("SELECT");
            if (tokens[selectIndex + 1].equals("*")) {
                columns = null; // All columns
                selectAll = true;
            } else {
                columns = Arrays.copyOfRange(tokens, selectIndex + 1, tokens.length - 1);
            }
        } else {
            System.out.println("SELECT clause is mandatory.");
            return result;
        }

        // Check if FROM clause is present and get tables
        if (!query.contains("FROM")) {
            System.out.println("FROM clause is mandatory.");
            return result;
        }

        // String[] tables = query.split("FROM")[1].trim().split("\\s+");
        String[] tables = query.split("FROM");
        if (tables.length < 2) {
            System.out.println("FROM clause not found in the query.");
            return result;
        }

        String tableName = tables[1].trim().split("\\s+")[0]; // The first non-empty string after FROM is the table name
        System.out.println(tables[1].trim().split(" ")[1] + " " + tables[1].trim().split(" " )[2]);
        String tableName2 = tables[1].trim().split("\\s+")[3] ;
        // String tableName = tables[0];
        String csvFilePath = null;
        for(String path:paths){
            if(path.contains(tableName)){
                csvFilePath = path;
                break;
            }
        }
        /* 
        System.out.println(tableName2);
        System.out.println(tables[1].trim().split("\\s+")[3]+"ddfdf");
        if(csvFilePath==null){
            System.out.println("CSV file path not found  for table:"+tableName);
            return result;
        }
        String csvFilePath2 = null;
        // for(String path:paths){
        //     if(path.contains(tableName2)){
        //         csvFilePath2 = path;
        //         break;
        //     }
        // }
        System.out.println(paths[1]);
         System.out.println(paths[1].equals(tableName2));
        if(paths[1].equals(tableName2)){
            csvFilePath2 = paths[1];
        }
         if(csvFilePath2==null){
            System.out.println("CSV file path not found  for table:"+tableName2);
            return result;
        }
       */
        //  if (query.contains("NATURAL JOIN")) {
        // List<String[]> table1Data = readCSV(csvFilePath);
        // // System.out.println(table1Data);
        // List<String[]> table2Data = readCSV(csvFilePath2);

       
        //     result = naturalJoin(table1Data, table2Data);
        // } else if (query.contains("JOIN")) {
        //     List<String[]> table1Data = readCSV(csvFilePath);
        // // System.out.println(table1Data);
        // List<String[]> table2Data = readCSV(csvFilePath2);
        //     result = crossJoin(table1Data, table2Data);
        // } else {
        //     System.out.println("Unsupported join type.");
        //     return result;
        // }
        
        result = readCSV(csvFilePath);
       
        // if(query.contains(joinType)){
        //     result = naturalJoin(paths, tables[0],tables[1], columns, selectAll);
        // }else if(query.contains("JOIN")){
        //     result = crossJoin(paths, tables[0], tables[1], columns, selectAll);
        // }else{
        //    result = readCSV(paths[0]); 
        // }
        // List<String> arributesNames = splitHeader(result);
        // System.out.println(Arrays.asList(arributesNames[1])+"ddd");
        // if (query.contains("JOIN")) {
        //     // Perform JOIN operation
        //     String joinType = query.split("JOIN")[0].trim().split("\\s+")[0];
        //     System.out.println(joinType);
        //     if (joinType.equalsIgnoreCase("NATURAL")) {
        //         result = naturalJoin(paths, tables[0], tables[1], columns, selectAll);
        //     } else {
        //         // Cross join
        //         result = crossJoin(paths, tables[0], tables[1], columns, selectAll);
        //     }
        // } else {
        //     // Single table operation
        //     result = readCSV(paths[0]);
        // }
        // if
        // Applying WHERE clause
        if (query.contains("WHERE")) {
            String condition = query.split("WHERE")[1].trim();
            System.out.println(condition+"yoo");//debugging 
            result = applyWhereClause(result, condition);
        }

        // Applying ORDER BY clause
        if (query.contains("ORDER BY")) {
              List<String> headerNames = splitHead(csvFilePath);
            //  if(!result.isEmpty()){
            // List<String> headers = new ArrayList<>(Arrays.asList(result.get(0)));
            // }
            System.out.println(headerNames.get(2)+ "dfdfdfdfd");
            String orderBy = query.split("ORDER BY")[1].trim();
            System.out.println(orderBy+"uooo");
            // String orderColumn = orderBy.split(" ")[0];
            // System.out.println(orderColumn);
            String[] orderColumn = orderBy.split("\\s+(?=ASC|DESC)");
            System.out.println(orderColumn[0]);
            // String sortOrder = "ASC"; // Default sort order
            // if (orderBy.split("\\s+").length > 1) {
            //     sortOrder = orderBy.split("\\s+")[1].trim();
            //     // System.out.println(sortOrder);
            // }
            System.out.println(orderColumn[1]+"dfdfd");;
            System.out.println(headerNames.indexOf(orderColumn[0])+"dfd");
            System.out.println(headerNames.size());
            result = applyOrderByClause(result, headerNames.indexOf(orderColumn[0]), orderColumn[1]);
        }

        return result;
    }
    

    public static List<String[]> readCSV(String path) {
        List<String[]> data = new ArrayList<>();
        List<String> headers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            if((line =br.readLine())!=null){
                String[] headerNames = line.split(",\\s*");
                headers.addAll(Arrays.asList(headerNames));
            }
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",\\s*");
                data.add(values);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public static List<String[]> applyWhereClause(List<String[]> data, String condition) {
        String[] tokens = condition.split(" ");
        // System.out.println(tokens[0]);
        // System.out.println(tokens[4]);
        String columnName = tokens[0].trim();
        System.out.println(columnName);
        String operator = tokens[1].trim();
        System.out.println(operator);
        // String ddf = tokens[2];
        // System.err.println(ddf );
        String value = tokens[2]+" "+tokens[3];
        System.out.println(value);
        int startIndex = value.indexOf('"');
        if(startIndex==-1){
            return null;
        }
        int endIndex = value.indexOf('"',startIndex+1);
        if(endIndex == -1){
            return null;
        }
        String value2 = value.substring(startIndex+1,endIndex); 
        System.out.println(value2);
        
        // Apply filtering based on the condition
        List<String[]> filteredData = new ArrayList<>();
        boolean skipFirst = true;
        for (String[] row : data) {
            if(skipFirst){
                skipFirst = false;
                continue;
            }
            System.out.println(Arrays.asList(row));
            System.out.println(row[1]);
            String cellValue = row[1];
            // System.out.println(cellValue);
            switch (operator) {
                case "=":
                    if (cellValue.equals(value2)) {
                        filteredData.add(row);
                    }
                    break;
                case "!=":
                    if (!cellValue.equals(value2)) {
                        filteredData.add(row);
                    }
                    break;
                default:
                    System.out.println("Unsupported operator: " + operator);
                    break;
            }
        }
        return filteredData;
    }
    public static List<String[]> applyOrderByClause(List<String[]> data, int columnIndex, String sortOrder) {
    Collections.sort(data, new Comparator<String[]>() {
        @Override
        public int compare(String[] row1, String[] row2) {
            String value1 = (columnIndex >= 0 && columnIndex < row1.length) ? row1[columnIndex].trim() : "";
            String value2 = (columnIndex >= 0 && columnIndex < row2.length) ? row2[columnIndex].trim() : "";

            // Numeric comparison if possible
            try {
                int numValue1 = Integer.parseInt(value1);
                int numValue2 = Integer.parseInt(value2);
                return (sortOrder.equalsIgnoreCase("ASC")) ? numValue1 - numValue2 : numValue2 - numValue1;
            } catch (NumberFormatException e) {
                // String comparison
                return (sortOrder.equalsIgnoreCase("ASC")) ? value1.compareToIgnoreCase(value2) : value2.compareToIgnoreCase(value1);
            }
        }
    });
    return data;
}

   public static List<String[]> naturalJoin(List<String[]> table1Data, List<String[]> table2Data) {
    List<String[]> result = new ArrayList<>();

    // Find common columns between the two tables
    List<String> commonColumns = findCommonColumns(table1Data, table2Data);
    if (commonColumns.isEmpty()) {
        System.out.println("No common columns found for natural join.");
        return result; // Return empty result
    }

    // Perform natural join
    for (String[] row1 : table1Data) {
        for (String[] row2 : table2Data) {
            boolean match = true;
            for (String col : commonColumns) {
                int index1 = getColumnIndex(row1, col);
                int index2 = getColumnIndex(row2, col);
                if (!row1[index1].trim().equalsIgnoreCase(row2[index2].trim())) {
                    match = false;
                    break;
                }
            }
            if (match) {
                String[] joinedRow = new String[row1.length + row2.length];
                System.arraycopy(row1, 0, joinedRow, 0, row1.length);
                System.arraycopy(row2, 0, joinedRow, row1.length, row2.length);
                result.add(joinedRow);
            }
        }
    }

    return result;
}
public static List<String> findCommonColumns(List<String[]> table1Data, List<String[]> table2Data) {
    List<String> commonColumns = new ArrayList<>();
    Set<String> table1Columns = new HashSet<>(Arrays.asList(table1Data.get(0)));
    for (String col : table2Data.get(0)) {
        if (table1Columns.contains(col)) {
            commonColumns.add(col);
        }
    }
    return commonColumns;
}
public static int getColumnIndex(String[] row, String columnName) {
    if (row == null || columnName == null) {
        return -1; // Handle null arrays or column names
    }
    for (int i = 0; i < row.length; i++) {
        if (row[i].equalsIgnoreCase(columnName.trim())) {
            return i;
        }
    }
    return -1; // Column not found
}



//     public static List<String[]> crossJoin(String[] paths, String table1, String table2, String[] columns, boolean selectAll) {
//         List<String[]> result = new ArrayList<>();

//     List<String[]> table1Data = readCSV(paths[0]); // Assuming first CSV path is for table1
//     List<String[]> table2Data = readCSV(paths[1]); // Assuming second CSV path is for table2

//     // Perform the cross join
//     for (String[] row1 : table1Data) {
//         for (String[] row2 : table2Data) {
//             // Combine the rows from both tables
//             String[] joinedRow = new String[row1.length + row2.length];
//             System.arraycopy(row1, 0, joinedRow, 0, row1.length);
//             System.arraycopy(row2, 0, joinedRow, row1.length, row2.length);
//             result.add(joinedRow);
//         }
//     }
//     return result;
//    }
    public static List<String[]> crossJoin(List<String[]> table1Data, List<String[]> table2Data) {
        List<String[]> result = new ArrayList<>();

        // Ensure that both tables have data
        if (table1Data == null || table1Data.isEmpty() || table2Data == null || table2Data.isEmpty()) {
            System.out.println("Table data is empty or null. Cannot perform cross join.");
            return result;
        }

        // Combine each row from table1Data with each row from table2Data
        for (String[] row1 : table1Data) {
            for (String[] row2 : table2Data) {
                System.out.println(row1.length);
                String[] joinedRow = concatenateArrays(row1, row2);
                result.add(joinedRow);
            }
        }

        return result;
    }
    public static String[] concatenateArrays(String[] array1, String[] array2) {
        String[] result = new String[array1.length + array2.length];
        System.arraycopy(array1, 0, result, 0, array1.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
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
        String output = "selectAll.csv"; // Output file path
        String query = readFile(sqlFile);
        List<String[]> result = executeStatement(csvFiles, query);
        //Debugging
        System.out.println("These values written in output.csv");
        for(String[] row: result){
         System.out.println(Arrays.toString(row));
        }
        writeToFile(output, result);
    }
}


