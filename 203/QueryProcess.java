import java.util.*;
import java.io.*;
public class QueryProcess{
   public static String readFile(String path) {
      StringBuilder builder = new StringBuilder();
      try(BufferedReader br = new BufferedReader(new FileReader(path))) {
         String line;
         while((line = br.readLine())!= null){
            builder.append(line).append("\n");
         } 
      }
      catch(IOException e) {
         e.printStackTrace();
      }
      return builder.toString();
   }

   public static List<String[]> executeStatement(String pathcsv,String query){
      String[] tokens = query.split(" ");
      // String name = tokens[3].split("\\.")[0];
      String[] columns = tokens[1].equals("*") ? null : Arrays.copyOfRange(tokens, 1,tokens.length -1 );
      List<String[]> result = new ArrayList<>();
      try (BufferedReader br = new BufferedReader(new FileReader(pathcsv))) {
         String line;
         while ((line = br.readLine()) != null) {
            String[] values = line.split(",\\s*"); // Split by comma and optional spaces
            if (columns != null) {
                 // Perform projection if specific columns are requested
               List<String> selectedValues = new ArrayList<>();
               for (String column : columns) {
                  int index = Arrays.asList(values).indexOf(column);
                  if (index != -1) {
                     selectedValues.add(values[index]);
                  }
               }
               result.add(selectedValues.toArray(new String[0]));
            } else {
                // Return all rows if projection is for all columns
               result.add(values);
            }
         }
      } catch (IOException e) {
            e.printStackTrace();
      }
      return result;
      
   }
   public static void writeToFile(String path, List<String[]> result){
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(path))) {
         for(String[] row: result){
            bw.write(String.join(",",row));
            bw.newLine();
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
   public static void main(String[] args) {
      String csvFile = "/home/dmacs-5/Desktop/II Sem/203/large_data.csv";
      String sqlFile = "/home/dmacs-5/Desktop/II Sem/203/sqlquery.txt";
      String output = "/home/dmacs-5/Desktop/II Sem/203/output.csv";
      String query = readFile(sqlFile);
      List<String[]> result = executeStatement(csvFile, query);
      writeToFile(output, result);

   }
}

