
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <sys/time.h>
#include "apue.h"
int main(int argc, char *argv[]) {
   if (argc != 2) {
      fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
      exit(EXIT_FAILURE);
   }

   char *filename = argv[1];
   int fd;
   if ((fd = open(filename, O_RDWR)) < 0) {
      perror("Error opening file");
      exit(EXIT_FAILURE);
   }
   struct timespec times[2];
   times[0].tv_sec = times[1].tv_sec = time(NULL);
   times[0].tv_nsec = times[1].tv_nsec = 0;
   if (futimens(fd, times) < 0) {
      perror("Error updating file times");
      close(fd);
      exit(EXIT_FAILURE);
   }

   printf("File times updated successfully for %s.\n", filename);
   close(fd);
   return 0;
}
