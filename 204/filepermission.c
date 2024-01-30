#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>

int main() {
    const char *filename = "newFilewith1.txt"; 
    umask(0);
    int fd = open(filename, O_CREAT | O_WRONLY, 0777); 
    if (fd == -1) {
        perror("Error creating file");
        exit(1);
    }

    printf("File created with ugo+rwx permissions: %s\n", filename);

    close(fd); 
    return 0;
}