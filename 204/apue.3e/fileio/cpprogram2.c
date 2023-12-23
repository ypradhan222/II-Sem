#include<stdio.h>
#include <stdlib.h>
#include<fcntl.h>
#include<unistd.h>
#define BUFFERSIZE 4096
int main(){
   //Open two files using system calls
   int source_f = open("file.hole",O_RDWR);
   int targe_f = open("target.txt",O_RDWR|O_CREAT|O_TRUNC,420);
   //Return error if it doesnot open
   if(source_f == -1 || targe_f == -1){
      perror("open");
      exit(EXIT_FAILURE);
   }
   char buffer[1024];
   ssize_t readbytes,writebytes;
   int nullbytes = 0;
   while((readbytes = read(source_f,buffer,sizeof(buffer)))>0){
      for(int i =0 ;i < readbytes;i++)
      {
         if(buffer[i]=='\0'){
            nullbytes++;
         }
         else{
            if(nullbytes > 0){
               off_t offset = lseek(targe_f,nullbytes,SEEK_CUR);
               if(offset == -1){
                  perror("Error lseek");
                  exit(EXIT_FAILURE);
               }
               nullbytes = 0;
            }
            writebytes = write(targe_f,buffer+i,1);
            if(writebytes == -1){
               perror("ERROR");
               exit(EXIT_FAILURE);
            }
         }
      }
   }

   close(source_f);
   close(targe_f);



   return 0;
}