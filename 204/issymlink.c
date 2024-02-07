#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include"apue.h"
#include <fcntl.h>
#define BUFFER_SIZE 4096
int main(int argc, char *argv[]) {
   if (argc < 2) {
      err_sys("Wrong number of arguments");
      exit(EXIT_FAILURE);
   }
   const char *filename = argv[1];
   struct stat buf;

   if (lstat(filename, &buf) == 0) {
      if (S_ISLNK(buf.st_mode)) {
         char linkbuf[BUFFER_SIZE];
         ssize_t size = readlink(filename, linkbuf, sizeof(linkbuf) - 1);
         if (size != -1) {
            linkbuf[size] = '\0';  
            printf("%s is a symbolic link pointing to: %s\n", filename, linkbuf);
         } else {
            err_sys("readlink");
            exit(EXIT_FAILURE);
         }
      } else {
         printf("%s is not a symbolic link.\n", filename);
      }
   } else {
      err_sys("lstat");
      exit(EXIT_FAILURE);
   }
   return 0;
}