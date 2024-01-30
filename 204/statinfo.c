#include "apue.h"
#include<sys/stat.h>
#include <unistd.h>
#include <time.h>
void printStats(char *fileName){
   struct stat buf;
   if(lstat(fileName,&buf) < 0){
         err_ret("lstat error");
      }
   if(stat(fileName,&buf)==0){
      printf("File Name :: %s\n",fileName);
      printf("Regular file :: %s\n",(S_ISREG(buf.st_mode)) ? "Yes" : "No");
      printf("Directory File:: %s\n",(S_ISDIR(buf.st_mode))? "yes" : "NO");
      printf("Character Special File:: %s\n",(S_ISCHR(buf.st_mode))? "yes" : "NO");
      printf("Block Special File:: %s\n",(S_ISBLK(buf.st_mode))? "yes" : "NO");
      printf("Pipe or FIFO :: %s\n",(S_ISFIFO(buf.st_mode))? "yes" : "NO");
      printf("Symbolic link:: %s\n",(S_ISLNK(buf.st_mode))? "yes" : "NO");
      printf("Socket:: %s\n",(S_ISSOCK(buf.st_mode))? "yes" : "NO");
      printf("Inode Number : %ld\n",(long) buf.st_ino);
      printf("Device Number: %ld\n" ,(long)buf.st_dev);
      printf("Device Number for Special files : %ld\n" ,(long)buf.st_rdev);
      printf("Number of links :: %ld\n",buf.st_nlink);
      printf("User ID : %d\n",buf.st_uid);
      printf("group ID: %d\n",buf.st_gid);
      printf("Size: %lld bytes\n",(long long)buf.st_size);
      printf("Last Accessed: %s", ctime(&buf.st_atime));
      printf("Last Modified: %s", ctime(&buf.st_mtime));
      printf("Last Status Change: %s", ctime(&buf.st_ctime));
      printf("I/O Block SIze:: %ld\n",(long) buf.st_blksize);
      printf("Number of disks allocated :: %ld\n",buf.st_blocks);
      printf("\n\n");
       
   }
   else{
      fprintf(stderr,"Error accessing file: %s\n",fileName);
      err_sys("Error");

   }
}
int main(int argc,char* argv[]){
   if (argc < 2) {
      printf("No files entered.\n");
      return 1;
    }
   for(int i=1;i<argc;i++){
      printf("%s :: \n ",argv[i]);
     
      printStats(argv[i]);
   }
   return 0;
}