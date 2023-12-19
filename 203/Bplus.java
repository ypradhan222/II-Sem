import java.util.*;
import java.io.*;
public class Bplus {
   private static final keylength = 25;
   public static void main(String[] args) {
      FileInputStream inputFIle;
      try {
         inputFIle = new FileInputStream(args[0]);
      } catch (Exception e) {
         System.out.println("Error Findind file");
      }
      try {
         DataInputStream dataInputStream = new DataInputStream(inputFIle);
         BPlusTree<String,Long> tree = new BPLusTree<>();

      } catch (Exception e) {
         // TODO: handle exception
      }
      private static Pad(String input){
         if(input.length() < keylength){
            return String.format("%-25s", input);
         }else{
            return input.substring(0, keylength);
         }
      }
   }
}
