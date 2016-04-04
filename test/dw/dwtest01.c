//===========================================================================
// dwtest01.c - recreate md/open race condition seen on Trinitite
//
// Issues 3 level mkdir from rank 0, then creates file in new dir on all
// ranks. Some ranks get errno 2.
//
// Before running, ensure directory to be created does not exist.
//
//===========================================================================
#define _GNU_SOURCE 1
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <mpi.h>

//#define PREFIX "/tmp/dw_scr/cornell/"
#define PREFIX getenv("DW_JOB_STRIPED")

#define MSG(fmt, args...)                                                    \
  fprintf(stdout, __FILE__ "(%4d) %s[%d/%d]: " fmt "\n", __LINE__, hostname, myrank, mpi_size, args);

#define ERRX(fmt, args...) {                                                 \
  MSG(fmt, args);                                                            \
  exit(EXIT_FAILURE);                                                        \
}

int main(int argc, char * * argv) {
  int rc, myrank, mpi_size, fd;
  char hostname[256], *p, path[256];
  MPI_Comm mpi_comm;

  MPI_Init(NULL, NULL);
  mpi_comm = MPI_COMM_WORLD;
  MPI_Comm_rank(mpi_comm, &myrank);
  MPI_Comm_size(mpi_comm, &mpi_size);

  rc = gethostname(hostname, sizeof(hostname));
  if (0 != rc) ERRX("gethostname rc: %d (%s)", rc, strerror(errno));
  p = strchr(hostname, '.');
  if (p) *p = '\0';

  MPI_Barrier(mpi_comm);

  if (0 == myrank) {
    strcpy(path, PREFIX);

    strcat(path, "d1");
    rc = mkdir(path, S_IRWXU);
    if (0 != rc) ERRX("mkdir %s errno %d (%s)", path, errno, strerror(errno));

    strcat(path, "/d2");
    rc = mkdir(path, S_IRWXU);
    if (0 != rc) ERRX("mkdir %s errno %d (%s)", path, errno, strerror(errno));

    strcat(path, "/d3");
    rc = mkdir(path, S_IRWXU);
    if (0 != rc) ERRX("mkdir %s errno %d (%s)", path, errno, strerror(errno));
    MSG("Directory created: %s", path);
  }

  MPI_Barrier(mpi_comm);

  strcpy(path, PREFIX);
  strcat(path, "d1/d2/d3/data_file");
  fd = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
  if (-1 == fd && EEXIST != errno) MSG("open %s errno %d (%s)", path, errno, strerror(errno));

  MPI_Barrier(mpi_comm);
  MPI_Finalize();

  return 0;
}

// --- end of dwtest02.c ---
