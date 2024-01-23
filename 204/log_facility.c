#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <apue.h>
#include<time.h>
#define MAXLENGTH_NAME 20
#define MAXLENGTH_NO 20
struct entry {
    char name[MAXLENGTH_NAME];
    char phonenumber[MAXLENGTH_NO];
};
void logging(char *filename,char* action,char* content){
   FILE *logFile = fopen(filename,"a");
   if(logFile == NULL){
      err_sys("Error opening file");
      return;
   }
   time_t currentTime;
   time(&currentTime);
   struct tm *temp;
   temp = localtime(&currentTime);
   char buffer[100];
   strftime(buffer,30,"%Y-%m-%d %H:%M:%S",temp);
   fprintf(logFile, "[%s] %s: %s\n", buffer, action, content);
   fclose(logFile);
}
int main() {
    int size;
    char fileName[100];
    char logFileName[100] = "log.txt";
    printf("Please enter the file name: ");
    scanf("%s", fileName);
    printf("Enter the size of telbook: ");
    scanf("%d", &size);
    int file = open(fileName, O_RDWR | O_CREAT | O_TRUNC);
    if (file == -1) {
        err_sys("Error creating file");
    } else {
        printf("Created Successfully\n");
        logging(logFileName,"Open",fileName);
    }
    struct entry entry;
    if (lseek(file, size * sizeof(struct entry) - 1, SEEK_SET) == -1) {
        err_sys("Error setting the file size");
    }
    if (write(file, "", 1) == -1) {
        err_sys("Error writing the file");
    }
    lseek(file, 0, SEEK_SET);
    printf("Enter the names and phone numbers. Enter 'exit' to finish.\n");
    int index = 0;
    while (index<size) {
        printf("Name please: ");
        scanf("%19s", entry.name);
        if (strcmp(entry.name, "exit") == 0) {
            break;
        }

        printf("Number please: ");
        scanf("%19s", entry.phonenumber);
        if (strcmp(entry.name, "exit") == 0) {
            break;
        }
        if (write(file, &entry, sizeof(struct entry)) == sizeof(struct entry)) {
            index++;
            logging(logFileName,"Write","Written Successfully");
        } else {
            err_sys("Error writing entry at index");
        }
    }
    int choice;
    printf("What you want to do please have your choice:\n");
    printf("1. Display \n2. Write on index \n3. Exit\n");
    scanf("%d", &choice);
    switch (choice) {
        case 1:
            printf("Enter the index number to display entry: ");
            scanf("%d", &index);
            if (index >= 0 && index < size) {
                if (lseek(file, index * sizeof(struct entry), SEEK_SET) == -1) {
                    err_sys("Error seeking to index");
                }
                if (read(file, &entry, sizeof(struct entry)) == sizeof(struct entry)) {
                    printf("Entry at index %d:\n", index);
                    printf("Name: %s\n", entry.name);
                    printf("Phone Number: %s\n", entry.phonenumber);
                    logging(logFileName,"Read","Read Successfully");
                } else {
                    err_sys("Error reading entry at index");
                }
            } else {
                printf("Invalid index\n");
            }
            break;
        case 2:
            printf("Enter the index you want to write: ");
            scanf("%d", &index);
            if (index >= 0 && index < size) {
                printf("Enter Name: ");
                scanf("%19s", entry.name);
                printf("Enter the phone number: ");
                scanf("%19s", entry.phonenumber);
                if (lseek(file, index * sizeof(struct entry), SEEK_SET) == -1) {
                    err_sys("Error seeking to index");
                }
                if (write(file, &entry, sizeof(struct entry)) == sizeof(struct entry)) {
                    printf("Written successfully at index %d\n", index);
                    logging(logFileName,"Write","Written Successfully");
                } else {
                    err_sys("Error writing on desired index");
                }
            } else {
                printf("Invalid Index\n");
            }
            break;
        default:
            break;
    }
    close(file);
    logging(logFileName,"Close",fileName);
    return 0;
}