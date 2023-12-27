#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#define MAXLENGTH_NAME 20
#define MAXLENGTH_NO 20

struct entry {
    char name[MAXLENGTH_NAME];
    char phonenumber[MAXLENGTH_NO];
};

void display(int file,int size,struct entry entry,int index){
        if (index >= 0 && index < size) {
        lseek(file, index * sizeof(struct entry), SEEK_SET);
        if (read(file, &entry, sizeof(struct entry)) == sizeof(struct entry)) {
            printf("Entry at index %d:\n", index);
            printf("Name: %s\n", entry.name);
            printf("Phone Number: %s\n", entry.phonenumber);
        } else {
            printf("Error reading entry at index %d\n", index);
        }
    } else {
        printf("Invalid index\n");
    }
}
void writeEntry(int file,struct entry entry,int size){
    int index;
    printf("Enter the index you want to write::");
    scanf("%d",&index);
    if(index>=0 && index<size){
        printf("Enter Name::");
        scanf("%19s",entry.name);
        printf("Enter the phone number::");
        scanf("%19s",entry.phonenumber);
        lseek(file,index*sizeof(struct entry),SEEK_SET);
        if(write(file,&entry,sizeof(struct entry)) == sizeof(struct entry)){
            printf("Written successfully at index %d\n",index);
        }
        else{
            printf("Error writing on desired index");
        }
    }
    else{
        printf("Invalid Index");
    }
}
int main() {
    int size;
    char fileName[100];

    printf("Please enter the file name: ");
    scanf("%s", fileName);

    printf("Enter the size of telbook: ");
    scanf("%d", &size);

    int file = open(fileName, O_RDWR | O_CREAT | O_TRUNC);

    if (file == -1) {
        perror("Error creating file");
        exit(1);
    } else {
        printf("Created Successfully\n");
    }

    struct entry entry;

    if (lseek(file, size * sizeof(struct entry) - 1, SEEK_SET) == -1) {
        perror("Error setting the file size");
        close(file);
        exit(EXIT_FAILURE);
    }
    if (write(file, "", 1) == -1) {
        perror("Error writing the file");
        close(file);
        exit(EXIT_FAILURE);
    }

    lseek(file, 0, SEEK_SET);

    printf("Enter the names and phone numbers. Enter 'exit' to finish.\n");

    int index = 0;

    while (1) {
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
        write(file, &entry, sizeof(struct entry));

        index++;
    }
    int choice;
    printf("What you want to do please have your choice::\n");
    printf("1. Display \n2. write on index \n3. Exit\n");
    scanf("%d",&choice);
    
    switch (choice)
    {
    case 1:
        printf("Enter the index number to display entry: ");
        scanf("%d", &index);
        display(file,size,entry,index);
        break;
    case 2:
        writeEntry(file,entry,size);
    default:
        printf("Exited!! THAnks");
        break;
    }

    close(file);

    return 0;
}

