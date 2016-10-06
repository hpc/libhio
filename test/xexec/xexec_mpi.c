/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "xexec.h"
#ifdef MPI  // Entire module excluded if not MPI
#include <mpi.h>

//----------------------------------------------------------------------------
// xexec mpi module - contains MPI actions
//----------------------------------------------------------------------------
#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)

static char * help =
  "  mi <shift>    issue MPI_Init(), shift ranks by <shift> from original assignment\n"
  "  msr <size> <stride>\n"
  "                issue MPI_Sendreceive with specified buffer <size> to and\n"
  "                from ranks <stride> above and below this rank\n"
  "  mb            issue MPI_Barrier()\n"
  "  mgf           gather failures - send fails to and only report success from rank 0\n"
  "  mf            issue MPI_Finalize()\n"
;


static struct mpi_state {
  void *sbuf;
  void *rbuf;
  size_t buf_len;
} mpi_state;

MODULE_INIT(xexec_mpi_init) {
  //struct mpi_state * s = state;
  return 0;
}

MODULE_HELP(xexec_mpi_help) {
  fprintf(file, "%s", help);
  return 0;
}

#define S (* ((struct mpi_state *)(actionp->state)) )
//----------------------------------------------------------------------------
// mi, mb, mf (MPI init, barrier, finalize) mgf action handlers
//----------------------------------------------------------------------------
ACTION_RUN(mi_run) {
  MPI_CK(MPI_Init(NULL, NULL));
  int shift = V0.u;
  G.mpi_comm = MPI_COMM_WORLD;
  set_msg_id(&G);
  if (shift > 0) {
    MPI_Group oldgroup, newgroup;
    int ranks[G.mpi_size];

    for (int i=0; i<G.mpi_size; ++i) {
      ranks[i] = (i + shift) % G.mpi_size;
      if (G.myrank == 0) VERB3("New rank %d is old rank %d", i, ranks[i]);
    }

    MPI_CK(MPI_Comm_group(MPI_COMM_WORLD, &oldgroup));
    MPI_CK(MPI_Group_incl(oldgroup, G.mpi_size, ranks, &newgroup));
    MPI_CK(MPI_Comm_create(MPI_COMM_WORLD, newgroup, &G.mpi_comm));

    set_msg_id(&G);
  }
}

ACTION_RUN(msr_run) {
  size_t len = V0.u;
  int stride = V1.u;
  MPI_Status status;
  if (S.buf_len != len) {
    S.sbuf = REALLOCX(S.sbuf, len);
    S.rbuf = REALLOCX(S.rbuf, len);
    S.buf_len = len;
  }
  int dest = (G.myrank + stride) % G.mpi_size;
  int source = (G.myrank - stride + G.mpi_size) % G.mpi_size;
  DBG2("msr len: %d dest: %d source: %d", len, dest, source);
  MPI_CK(MPI_Sendrecv(S.sbuf, len, MPI_BYTE, dest, 0,
                      S.rbuf, len, MPI_BYTE, source, 0,
                      G.mpi_comm, &status));
}

ACTION_RUN(mb_run) {
  MPI_CK(MPI_Barrier(G.mpi_comm));
}

ACTION_RUN(mgf_run) {
  int new_global_fails = 0;
  MPI_CK(MPI_Reduce(&G.local_fails, &new_global_fails, 1, MPI_INT, MPI_SUM, 0, G.mpi_comm));
  DBG4("mgf: old local fails: %d new global fails: %d", G.local_fails, new_global_fails);
  if (G.myrank == 0) G.global_fails += new_global_fails;
  G.local_fails = 0;
  G.gather_fails = 1;
}

ACTION_RUN(mf_run) {
  MPI_CK(MPI_Finalize());
  set_msg_id(&G);
  S.sbuf = FREEX(S.sbuf);
  S.rbuf = FREEX(S.rbuf);
  S.buf_len = 0;
}

//----------------------------------------------------------------------------
// xexec_mpi - init foundational commands
//----------------------------------------------------------------------------
MODULE_INSTALL(xexec_mpi_install) {
  struct xexec_act_parse parse[] = {
  // Command   V0    V1    V2    V3    V4     Check          Run
    #ifdef MPI
    {"mi",    {UINT, NONE, NONE, NONE, NONE}, NULL,          mi_run      },
    {"msr",   {PINT, PINT, NONE, NONE, NONE}, NULL,          msr_run     },
    {"mb",    {NONE, NONE, NONE, NONE, NONE}, NULL,          mb_run      },
    {"mgf",   {NONE, NONE, NONE, NONE, NONE}, NULL,          mgf_run     },
    {"mf",    {NONE, NONE, NONE, NONE, NONE}, NULL,          mf_run      },
    #endif
  };

  xexec_act_add(&G, parse, DIM1(parse), xexec_mpi_init, &mpi_state, xexec_mpi_help);

  return 0;
}

#endif  // MPI
