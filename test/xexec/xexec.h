
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

#include <regex.h>
#ifdef MPI
  #include <mpi.h>
#endif // MPI
//----------------------------------------------------------------------------
// DBGMAXLEV controls which debug messages are compiled into the program.
// Set via compile option -DDBGMAXLEV=<n>, where <n> is 0 through 5.
// DBGMAXLEV is used by cw_misc.h to control the expansion of DBGx macros.
// Even when compiled in, debug messages are only issued if the current
// debug message level is set via the "d <n>" action to the level of the
// individual debug message.  Compiling in all debug messages via -DDBGLEV=5
// will noticeably impact the performance of high speed loops such as
// "vt" or "nr" due to the time required to repeatedly test the debug level.
// You have been warned !
//----------------------------------------------------------------------------
#ifndef DBGMAXLEV
  #define DBGMAXLEV 4
#endif
#include "cw_misc.h"

//----------------------------------------------------------------------------
// xexec global
//----------------------------------------------------------------------------
enum options {
  OPT_ROF      =  1,  // Run on Failure
  OPT_RCHK     =  2,  // Read Data Check
  OPT_XPERF    =  4,  // Extended Performance Messages
  OPT_PERFXCHK =  8,  // Exclude check time from Performance messages
  OPT_SMSGV1   = 16,  // Print success message v 1 so it can be suppressed with v 0 
  OPT_PAVM     = 32   // Pavilion Messages
};

#define XEXEC_VERSION "1.1.0"
typedef struct xexec_global {
  char id_string[256];
  int id_string_len;
  MSG_CONTEXT xexec_msg_context;
  char * test_name;
  int local_fails;   // Count of fails on this rank
  int global_fails;  // Count of fails on all ranks
  int gather_fails;  // local fails have been gathered into global fails
  #ifdef MPI
  // If MPI not init or finalized, myrank and size = 0
    int myrank;
    int mpi_size;
    int my_prior_rank; // MPI rank, even after MPI finalized
    MPI_Comm mpi_comm;
  #else
    const int myrank;  
    const int mpi_size;
    const int my_prior_rank; // MPI rank, even after MPI finalized
  #endif
  enum options options;
  // Common read / write buffer pointers, etc.  Set by dbuf action.
  void * wbuf_ptr;
  void * rbuf_ptr;
  size_t rwbuf_len;              // length not counting pattern overrun area
  U64 wbuf_data_object_hash_mod; // data_object hash modulus
  U64 wbuf_bdy;                  // wbuf address boundary
  U64 wbuf_repeat_len;           // repeat length of wbuf pattern
} GLOBAL;

//----------------------------------------------------------------------------
// Common macros - assume xexec global access vi G.<zzz>
//----------------------------------------------------------------------------
#define VERB_LEV_MULTI 2

#ifdef MPI
  #define MPI_CK(API) {                              \
    int mpi_rc;                                      \
    DBG2("Calling " #API);                           \
    mpi_rc = (API);                                  \
    VERB3(#API " rc: %d", mpi_rc);                   \
    if (mpi_rc != MPI_SUCCESS) {                     \
      char str[256];                                 \
      int len;                                       \
      MPI_Error_string(mpi_rc, str, &len);           \
      ERRX("%s rc:%d [%s]", (#API), mpi_rc, str);    \
    }                                                \
  }

  #define IF_MPI(stmt)     \
     if (mpi_active())     \
       { stmt; }           

  #define ELSE_MPI(stmt)   \
     else { stmt; }


#else
  #define IF_MPI(stmt)
  #define ELSE_MPI(stmt)   \
     { stmt; }
  #define MPI_CK(API) 
#endif

// Serialize execution of all MPI ranks
#define RANK_SERIALIZE_START {                         \
  DBG4("RANK_SERIALIZE_START");                        \
  IF_MPI(                                              \
    MPI_CK(MPI_Barrier(G.mpi_comm))                    \
    if (G.mpi_size > 0 && G.myrank != 0) {             \
      char buf;                                        \
      MPI_Status status;                               \
      MPI_CK(MPI_Recv(&buf, 1, MPI_CHAR, G.myrank - 1, \
             MPI_ANY_TAG, G.mpi_comm, &status));       \
    }                                                  \
  )                                                    \
}

#define RANK_SERIALIZE_END {                             \
  DBG4("RANK_SERIALIZE_END");                            \
  IF_MPI(                                                \
    if (G.mpi_size > 0 && G.myrank != G.mpi_size - 1) {  \
      char buf;                                          \
      MPI_CK(MPI_Send(&buf, 1, MPI_CHAR, G.myrank + 1,   \
             0, G.mpi_comm));                            \
    }                                                    \
    MPI_CK(MPI_Barrier(G.mpi_comm));                     \
  )                                                      \
}

#define R0_OR_VERB_START                                                                  \
  if ( (MY_MSG_CTX)->verbose_level >= VERB_LEV_MULTI ) RANK_SERIALIZE_START;              \
  if ( G.mpi_size == 0 || G.myrank == 0 || (MY_MSG_CTX)->verbose_level >= VERB_LEV_MULTI ) {

#define R0_OR_VERB_END                                                                    \
  }                                                                                       \
  if ( (MY_MSG_CTX)->verbose_level >= VERB_LEV_MULTI ) RANK_SERIALIZE_END;

//----------------------------------------------------------------------------
// Action handler interface
//----------------------------------------------------------------------------
typedef struct pval {
  U64 u;
  char * s;
  int i;  // Use for enums
  int c;  // Use for disabling enum bits
  double d;
  regex_t * rxp;  // Compiled regular expressions
} PVAL;

#define MAX_PARAM 5

typedef struct action ACTION;

// Many action handlers, use macro to keep function defs in sync
#define ACTION_CHECK(name) void (name)(GLOBAL *gptr, struct action *actionp)
typedef ACTION_CHECK(action_check);

#define ACTION_RUN(name) void (name)(GLOBAL *gptr, struct action *actionp)
typedef ACTION_RUN(action_run);

struct action {
  int tokn;             // Index of first token for action
  int actn;             // Index of this action element
  char * action;        // Action name
  char * desc;          // Action description
  action_check * checker;
  action_run * runner;
  PVAL v[MAX_PARAM];    // Action values
  void * state;
};

#define A (*actionp)
#define V0 (actionp->v[0])
#define V1 (actionp->v[1])
#define V2 (actionp->v[2])
#define V3 (actionp->v[3])
#define V4 (actionp->v[4])

enum ptype {
  SINT = CVT_SINT,
  UINT = CVT_NNINT,
  PINT = CVT_PINT,
  DOUB = CVT_DOUB,
  STR, REGX, DBUF, HFLG, HDSM, HERR, HULM, HDSI, HCPR, DWST, ONFF, NONE };

struct xexec_act_parse {
  char * cmd;
  enum ptype param[MAX_PARAM];
  action_check * checker;
  action_run * runner;
  void * state;
};

#define MODULE_INSTALL(name) int (name) (GLOBAL *gptr)
typedef MODULE_INSTALL(module_install); 

#define MODULE_INIT(name) int (name) (GLOBAL *gptr, void * state)
typedef MODULE_INIT(module_init);

#define MODULE_HELP(name) int (name) (GLOBAL *gptr, void * state, FILE * file)
typedef MODULE_HELP(module_help);

MODULE_INSTALL(xexec_base_install);   
MODULE_INSTALL(xexec_mpi_install);   
MODULE_INSTALL(xexec_fio_install);   
MODULE_INSTALL(xexec_hio_install);   

int xexec_act_add(GLOBAL *gptr, struct xexec_act_parse * parse_add, int n_add, 
                  module_init * init, void * state, module_help * help);

//----------------------------------------------------------------------------
// Common services
//----------------------------------------------------------------------------
int mpi_active(void);
void set_msg_id(GLOBAL * gptr);
enum dbuf_type {RAND22P, RAND22, OFS20};
void dbuf_init(GLOBAL * gptr, enum dbuf_type type, U64 size);
U64 get_data_object_hash(GLOBAL * gptr, char * obj_desc_string);
void * get_wbuf_ptr(GLOBAL * gptr, char * ctx, U64 offset, U64 data_object_hash);
int check_read_data(GLOBAL * gptr, char * ctx, void * buf, size_t len, U64 offset, U64 data_object_hash);
void prt_mmmst(GLOBAL * gptr, double val, char * desc, char * unit);
int rx_run(GLOBAL * gptr, int n, struct action * actionp, char * line);
ssize_t fwrite_rd (FILE *file, const void *ptr, size_t count);
ssize_t fread_rd (FILE *file, void *ptr, size_t count);

