// #include "apue.h"
// #include <stdio.h>
// #include<fcntl.h>
// #include<stdlib.h>
// #include<unistd.h>
// #define BUFFERSIZE 4096

// int main(int argc,char *argv[]){
//    //Error if files on specified on arguments
//    if(argc!=3){
//       fprintf(stderr,"%s",argv[0]);
//       exit(EXIT_FAILURE);
//    }
//    //pointers for source and target files
//    char *source = argv[1];
//    char *target = argv[2];
//    int source_f,target_f;
   
//    ssize_t bytes_read,bytes_written;
//    char buffer[BUFFERSIZE];
//    //Opening the source file for reading
//    source_f = open(source,O_RDONLY);
//    if(source_f == -1){
//       perror("Error opening file");
//       exit(EXIT_FAILURE);
//    }
//    //Opening target file or create new file
//    target_f = open(target,O_WRONLY|O_CREAT|O_TRUNC,0666);
//    if(target_f== -1){
//       perror("Error opening file");
//       close(source_f);
//       exit(EXIT_FAILURE);
//    }
//    //copying 
//    while((bytes_read = read(source_f,buffer,BUFFERSIZE))>0){
//       //to write the holes too
//       // fallocate(target_f,FALLOC_FL_KEEP_SIZE,lseek(source_f,0,SEEK_CUR),bytes_read);
//       lseek(target_f,bytes_read-1,SEEK_CUR);
//       bytes_written = write(target_f,"",1);
//       lseek(target_f,0,SEEK_CUR);
//       bytes_written = write(target_f,buffer,bytes_read);
//       if(bytes_read!= bytes_written){
//          perror("WRITING ERROR");
//          close(source_f);
//          close(target_f);
//          exit(EXIT_FAILURE);
//       }
//    }
//    close(source_f);
//    close(target_f);
//    printf("SUCCESSFULLY written\n");
//    return 0;

// }
#include <stdlib.h>
#include <stdio.h>
int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s source_file target_file\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *source_file = argv[1];
    char *target_file = argv[2];

    // Execute cp command with --sparse=always option
    char command[256];
    snprintf(command, sizeof(command), "cp --sparse=always %s %s", source_file, target_file);

    if (system(command) == -1) {
        perror("Error executing cp command");
        exit(EXIT_FAILURE);
    }

    printf("File copy completed successfully.\n");

    return 0;
}


