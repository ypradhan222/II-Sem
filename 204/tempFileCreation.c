
#include "apue.h"
#include <fcntl.h>
int main(void)
{
   if (open("tempfile", O_RDWR|O_CREAT) < 0)
   err_sys("open error");
   sleep(15);
   if (unlink("tempfile") < 0)
   err_sys("unlink error");
   printf("file unlinked\n");
   // sleep(15);
   printf("done\n");
   exit(0);
}
