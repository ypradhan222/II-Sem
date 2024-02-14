import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CreateCSV {
   public static void main(String[] args) {
      String filePath = "large_data.csv";
      List<String> names = Arrays.asList("Alice", "Bob", "Charlie", "David", "Eva");
      List<String> classNames = Arrays.asList("I M.Tech", "II M.Tech", "II M.Sc(M)", "I M.Sc(M)", "B.Sc(Hons)");
      // List<String> courses = Arrays.asList("UG", "PG","PP");
      try (FileWriter writer = new FileWriter(filePath)) {
         writer.append("Name,  Class,  Reg. No,   Age\n");
         Random rand = new Random();
         for (int i = 1; i <= 50; i++) {
            String name = getRandomElement(names);
            String className = getRandomElement(classNames);
            // String course = getRandomElement(courses);
            String row = name + ",   " + className + ",  " + rand.nextInt(100001)+ ",  " + (rand.nextInt(11)+20) + "\n";
            writer.append(row);
         }
         System.out.println("Large CSV file created successfully.");
      } catch (IOException e) {
         e.printStackTrace();
      }
   }
   private static String getRandomElement(List<String> list) {
      Random rand = new Random();
      return list.get(rand.nextInt(list.size()));
   }
}
