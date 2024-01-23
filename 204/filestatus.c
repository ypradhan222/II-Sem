#include <stdio.h>
#include "apue.h"
#include <unistd.h>
#include <fcntl.h>

int main() {
    int val;
    char fileName[100];
    printf("Enter the filename: ");
    scanf("%s", fileName);
    int file = open(fileName, O_CREAT | O_SYNC);
    if (file == -1) {
        err_sys("Error opening file");
        exit(EXIT_FAILURE);
    }

    val = fcntl(file, F_GETFL);
    if (val == -1) {
        err_sys("Error querying");
        close(file);
        exit(EXIT_FAILURE);
    }

    switch (val & O_ACCMODE) {
        case O_RDONLY:
            printf("Read Only\n");
            break;
        case O_WRONLY:
            printf("Write Only\n");
            break;
        case O_RDWR:
            printf("Read Write\n");
            break;
        default:
            err_dump("Unknown access mode\n");
            break;
    }

    if (val & O_APPEND) printf("Append\n");
    if (val & O_NONBLOCK) printf("Non-blocking\n");
    if (val & O_CREAT) printf("Create file\n");
    if (val & O_EXCL) printf("Exclusive\n");
    if (val & __O_DIRECTORY) printf("Name is in directory\n");
    if (val & __O_NOATIME) printf("Not update at access time\n");
    if (val & O_NOCTTY) printf("Controlling terminal\n");
    if (val & O_TRUNC) printf("Truncate the file\n");
    if (val & O_NDELAY) printf("Non-blocking again\n");
    #if !defined(_POSIX_C_SOURCE) && defined(O_FSYNC) && (O_FSYNC != O_SYNC)
      if(val & O_FSYNC) printf("Synchronous writes\n");
    #endif
    putchar('\n');
    close(file);

    return 0;
}
