import java.util.*;
import java.io.*;
public class Bplus1{
   public static void main(String[] args){
      FileInputStream input;
      int i;
      if(args.length !=1){
         System.out.println("ShowFIle file");
         return;
      }
      try{
      input = new FileInputStream(args[0]);
      }catch(FileNotFoundException exc){
         System.out.println("Error Opening file");
         return;
      }
      try{
         do{
            i = input.read();
            if(i != -1)
               System.out.println((char)i);
         }while(i!=-1);
      }catch(IOException ex){
         System.out.println("Error reading");
      }
      try{
      input.close();
      }catch(IOException exc){
         System.out.println("Error Closing File");
      }
   }
}