import java.util.Dictionary;

public class BPluss {
   public class Node{
      InternalNode root;
   }
   private class InternalNode extends Node{
      int maxDegree;
      int minDegree;
      int degree;
      InternalNode left;
      InternalError right;
      Integer[] keys;
      Node[] children;
      int size;
      private appendChild(Node child){
         this.children[degree] = child;
         this.degree++;
      }
      private int findIndex(Node child){
         for(int i=0;i<children.length;i++){
            if(children[i] == child){
               return i;
            }
            return -1;
         }
      }
      private void insertChild(Node child,int index){
         for (int i = degree-1; i >= index; i--) {
            children[i+1] = children[i];
         }
         this.children[index] = child;
         this.degree++;
      }
      private void insertAtFirst(Node child){
         for (int i = degree-1; i >= 0; i--) {
            children[i+1] = children[i];
         }
         this.children[0] = child;
         this.degree++;
      }
      private void removeChild(int index){
         this.children[index] = null;
         this.degree--;
      }
      private void removeChild(Node child){
         for (int i = 0; i < children.length; i++) {
            if (children[i] == child) {
               this.children[i] == null;
            }
            this.degree--;
         }
      }
      private InternalNode(int m,Integer[] keys,Node[] children){
         this.maxDegree=m;
         this.minDegree = (int)Math.ceil(m/2.0);
			this.degree = linearNullSearch(children);
			this.keys = keys;
			this.children = children;
      }
      public class LeafNode extends Node {
         int maxpair;
         int minpair;
         int size;
         LeafNode left;
         LeafNode right;
         DictionaryPair[] dictionary;
         
         public void delete(int index){
            this.dictionary[index] = null;
            size--;
         }
         public boolean insert(DictionaryPair pair){
            if(this.isFull()){
               return false;
            }
            else{
               this.dictionary[size] = pair;
               size++;
               Arrays.sort(this.dictionary,0,size);
               return true;
            }
         }
         public LeafNode(int m,DictionaryPair pair){
            this.maxpair = m - 1;
			   this.minpair = (int)(Math.ceil(m/2) - 1);
			   this.dictionary = new DictionaryPair[m];
			   this.size = 0;
			   this.insert(pair);
		}
      public LeafNode(int m, DictionaryPair[] dps, InternalNode parent) {
			this.maxpair = m - 1;
			this.minpair = (int)(Math.ceil(m/2) - 1);
			this.dictionary = dps;
			this.size= linearNullSearch(dps);
			this.root = root;
		}
      public class DictionaryPair implements Comparable<DictionaryPair >{
         int key;
         double value;
         public DictionaryPair(int key, double value) {
			   this.key = key;
			   this.value = value;
		}
       public int compareTo(DictionaryPair o) {
			if (key == o.key) { return 0; }
			else if (key > o.key) { return 1; }
			else { return -1; }
		}  
      }
         }
      }
   }
   
}
