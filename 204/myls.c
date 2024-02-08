#include <stdio.h>
#include "apue.h"
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
void list(const char *path,int show_hidden){
   DIR *dir;
   struct dirent *entry;
   if((dir=opendir(path))==NULL){
      err_sys("opendir");
   }
   while((entry=readdir(dir))!=NULL){
      if(strcmp(entry->d_name,".")!=0 && strcmp(entry->d_name,"..")!=0){
         if(show_hidden || entry->d_name[0]!='.'){
            printf("%s\n",entry->d_name);
            // continue;
         }
      }
      else{
         continue;
            // printf("%s\n",entry->d_name);
      }
   }
   closedir(dir);
}
int main(int argc,char *argv[]){
   
   const char *path = ".";
   int show_hidden = 0;
   if(argc>1 && strcmp(argv[1],"-a") == 0){
      show_hidden = 1;
   }
   list(path,show_hidden);
   
   return 0;

}