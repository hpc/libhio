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

//----------------------------------------------------------------------------
// xexec.c - xexec is a multi-purpose HPC system testing tool.  See the help
// text a few lines below for a description of its capabilities.
//----------------------------------------------------------------------------
#define _GNU_SOURCE 1
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <execinfo.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <regex.h>
#ifdef DLFCN
#include <dlfcn.h>
#endif  // DLFCN
#ifdef MPI
#include <mpi.h>
#endif  // MPI
#ifdef HIO
#include "hio.h"
#include "hio_config.h"
#if HIO_USE_DATAWARP
#include <datawarp.h>
#ifndef DW_PH_2
  #define DW_PH_2 0  // Phase 2 APIs not yet available
#endif // DW_PH_2
#endif // HIO_USE_DATAWARP
#endif // HIO
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

#define VERB_LEV_MULTI 2

//----------------------------------------------------------------------------
// To build:    cc -O3       xexec.c cw_misc.c -I. -o xexec.x
//       or: mpicc -O3 -DMPI xexec.c cw_misc.c -I. -o xexec.x
//  on Cray:    cc -O3 -DMPI xexec.c cw_misc.c -I. -o xexec.x -dynamic
//
// Optionally add: -DDBGLEV=3 (see discussion below)
//                 -DMPI      to enable MPI functions
//                 -DDLFCN    to enable dlopen and related calls
//                 -DHIO      to enable hio functions
//
// Also builds under hio autoconf / automake system.
//----------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Features to add: mpi sr swap, file write & read,
// flap seq validation, more flexible nr striding
//----------------------------------------------------------------------------

char * help =
  "xexec - multi-pupose HPC exercise and testing tool.  Processes command\n"
  "        line arguments and file input in sequence to control actions.\n"
  "        Version 1.0.0 " __DATE__ " " __TIME__ "\n"
  "\n"
  "  Syntax:  xexec -h | [ action [param ...] ] ...\n"
  "\n"
  "  Where valid actions and their parameters are:"
  "\n"
  "  v <level>     set verbosity level\n"
  "                0 = program start and end, failures\n"
  "                1 = 0 + summary performance messages\n"
  "                2 = 1 + detailed performance messages\n"
  "                3 = 2 + API result messages\n"
  "  d <level>     set debug message level\n"
  "                0 = no debug messages\n"
  "                1 = Action start messages\n"
  "                2 = 1 + API pre-call messages\n"
  "                3 = 2 + action parsing messages\n"
  "                4 = 3 + detailed action progress messages\n"
  "                5 = 4 + detailed repetitive action progress messages - if\n"
  "                enabled at compile time which will impact performance.\n"
  "  opt +-<option> ...  Set (+) or unset (-) options. Valid options are:\n"
  "                %s\n"
  "                Initial value is: %s plus env XEXEC_OPT\n"
  "                ? displays current set options\n"
  "  name <test name> Set test name for final success / fail message\n"
  "  im <file>     imbed a file of actions at this point, - means stdin\n"
  "  srr <seed>    seed random rank - seed RNG with <seed> mixed with rank (if MPI)\n"
  "  lc <count>    loop start; repeat the following actions (up to the matching\n"
  "                loop end) <count> times\n"
  "  lcr <min> <max>  like lc, but count random within inclusive range\n"
  "  lt <seconds>  loop start; repeat the following actions (up to the matching\n"
  "                loop end) for at least <seconds>\n"
  #ifdef MPI
  "  ls <seconds>  like lt, but synchronized via MPI_Bcast from rank 0\n"
  #endif
  "  le            loop end; loops may be nested up to 16 deep\n"
  "  ifr <rank>    execute following if rank matches, always exec if mpi not init'd\n"
  "                <rank> = -1 tests last rank, -2, -3, etc. similarly\n"
  "  eif           terminates ifr conditional, may not be nested\n"
  "  o <count>     write <count> lines to stdout\n"
  "  e <count>     write <count> lines to stderr\n"
  "  s <seconds>   sleep for <seconds>\n"
  "  va <bytes>    malloc <bytes> of memory\n"
  "  vt <stride>   touch most recently allocated memory every <stride> bytes\n"
  "  vf            free most recently allocated memory\n"
  "  imd <path> <mode> Issue mkdir(path, mode)\n"
  #ifdef __linux__
  "  dca           Display CPU Affinity\n"
  #endif
  #ifdef MPI
  "  mi <shift>    issue MPI_Init(), shift ranks by <shift> from original assignment\n"
  "  msr <size> <stride>\n"
  "                issue MPI_Sendreceive with specified buffer <size> to and\n"
  "                from ranks <stride> above and below this rank\n"
  "  mb            issue MPI_Barrier()\n"
  "  mgf           gather failures - send fails to and only report success from rank 0\n"
  "  mf            issue MPI_Finalize()\n"
  "  fo <file> <mode> Issue fopen for a file, %%r or %%p in path will be expanded to MPI\n"
  "                rank or PID. <mode> is fopen mode string.\n"
  "  fw <offset> <size>  Write relative to current file offset\n"
  "  fr <offset> <size>  Read relative to current file offset\n"
  "  fc            Issue fclose for file\n"  
  "  fctw <dir> <num files> <file size> <block size> File coherency test - write files\n"
  "                from rank 0\n"
  "  fctr <dir> <num files> <file size> <block size> File coherency test - read files\n"
  "                from all non-zero ranks \n"
  "  fget <file>   fopen(), fgets() to eof, fclose()\n"
  #endif
  "  ni <size> <count>\n"
  "                Creates <count> blocks of <size> doubles each.  All\n"
  "                but one double in each block is populated with sequential\n"
  "                values starting with 1.0.\n"
  "  nr <rep> <stride>\n"
  "                The values in each block are added and written to the\n"
  "                remaining double in the block. The summing of the block is\n"
  "                repeated <rep> times.  All <count> blocks are processed in\n"
  "                sequence offset by <stride>. The sum of all blocks is\n"
  "                computed and compared with an expected value.\n"
  "                <size> must be 2 or greater, <count> must be 1 greater than\n"
  "                a multiple of <stride>.\n"
  "  nf            Free allocated blocks\n"
  "  hx <min> <max> <blocks> <limit> <count>\n"
  "                Perform <count> malloc/touch/free cycles on memory blocks ranging\n"
  "                in size from <min> to <max>.  Allocate no more than <limit> bytes\n"
  "                in <blocks> separate allocations.  Sequence and sizes of\n"
  "                allocations are randomized.\n"
  #ifdef DLFCN
  "  dlo <name>    Issue dlopen for specified file name\n"
  "  dls <symbol>  Issue dlsym for specified symbol in most recently opened library\n"
  "  dlc           Issue dlclose for most recently opened library\n"
  #endif // DLFCN
  "  dbuf RAND22 | RAND22P | OFS20  <size>  Allocates and initializes data buffer\n"
  "                with pattern used for read data check.  Write and read patterns\n"
  "                must match.\n"
  #ifdef HIO
  "  hi  <name> <data_root>  Init hio context\n"
  "  hda <name> <id> <flags> <mode> Dataset allocate\n"
  "  hdo           Dataset open\n"
  "  heo <name> <flags> Element open\n"
  "  hso <offset> Set element offset.  Also set to 0 by heo, incremented by hew, her\n"
  "  hew <offset> <size> Element write, offset relative to current element offset\n"
  "  her <offset> <size> Element read, offset relative to current element offset\n"
  "  hewr <offset> <min> <max> <align> Element write random size, offset relative to current element offset\n"
  "  herr <offset> <min> <max> <align> Element read random size, offset relative to current element offset\n"
  "  hsega <start> <size_per_rank> <rank_shift> Activate absolute segmented\n"
  "                addressing. Actual offset now ofs + <start> + rank*size\n"
  "  hsegr <start> <size_per_rank> <rank_shift> Activate relative segmented\n"
  "                addressing. Start is relative to end of previous segment\n"
  "  hec <name>    Element close\n"
  "  hdc           Dataset close\n"
  "  hdf           Dataset free\n"
  "  hdu <name> <id> CURRENT|FIRST|ALL  Dataset unlink\n"
  "  hf            Fini\n"
  "  hxrc <rc_name|ANY> Expect non-SUCCESS rc on next HIO action\n"
  "  hxct <count>  Expect count != request on next R/W.  -999 = any count\n"
  "  hxdi <id> Expect dataset ID on next hdo\n"
  "  hvp <type regex> <name regex> Prints config and performance variables that match\n"
  "                the type and name regex's [1][2].  Types are two letter codes {c|p} {c|d|e}\n"
  "                where c|p is config or perf and c|d|e is context or dataset or element\n"
  "                \"hvp . .\" will print everything\n"
  "                \"hvp p total\" will print all perf vars containing \"total\" in the name\n"
  "  hvsc <name> <value>  Set hio context variable\n"
  "  hvsd <name> <value>  Set hio dataset variable\n"
  "  hvse <name> <value>  Set hio element variable\n"
  #if HIO_USE_DATAWARP
  "  dwds <directory> Issue dw_wait_directory_stage\n"
  "  dsdo <dw_dir> <pfs_dir> <type> Issue dw_stage_directory_out\n"
  #if DW_PH_2
  "  dwws <file>   Issue dw_wait_sync_complete\n"
  #endif // DW_PH_2
  #endif // HIO_USE_DATAWARP
  #endif // HIO
  "  k <signal>    raise <signal> (number)\n"
  "  x <status>    exit with <status>\n"
  "  segv          force SIGSEGV\n" 
  "  grep <regex> <file>  Search <file> and print (verbose 1) matching lines [1]\n"
  "                <file> = \"@ENV\" searches environment\n"
  "\n"
  "Notes:\n"
  " Numbers can be specified with suffixes %s\n"
  "\n"
  " Comments are delimited with /@, /@@ and @/, however those must stand alone in the \n"
  " actions as separate tokens.  Also, /@ is only recognized if it is the first\n"
  " token of an action. Comments starting with /@@ are printed. Comments may be nested.\n"
  "\n"
  " [1] <regex> parameters are POSIX extended regular expressions. See man 7 regex\n"
  "\n"
  " [2] Output only if MPI not active or if rank 0 or if verbose >= " STRINGIFY(VERB_LEV_MULTI) ".\n"
  "\n"
  " Example action sequences:\n"
  "    v 1 d 1\n"
  "    opt +ROF-RCHK\n"
  "    lc 3 s 0 le\n"
  "    lt 3 s 1 le\n"
  "    o 3 e 2\n"
  "    va 1M vt 4K vf\n"
  #ifdef MPI
  "    mi mb mf\n"
  #endif // MPI
  "    ni 32 1M nr 8 1 nf\n"
  "    x 99\n"
  "\n"
  #ifndef MPI
  " MPI actions can be enabled by building with -DMPI.  See comments in source.\n"
  "\n"
  #endif // MPI
  #ifndef DLFCN
  " dlopen and related actions can be enabled by building with -DDLFCN.  See comments in source.\n"
  "\n"
  #endif // DLFCN
;

//----------------------------------------------------------------------------
// Global variables
//----------------------------------------------------------------------------
char id_string[256] = "";
int id_string_len = 0;
int quit_on_fail = 1;  // Quit after this many or more fails (0 = never quit)
int local_fails = 0;   // Count of fails on this rank
int global_fails = 0;  // Count of fails on all ranks
int gather_fails = 0;  // local fails have been gathered into global fails
char * test_name = "<unnamed>";
#ifdef MPI
// If MPI not init or finalized, myrank and size = 0
int myrank = 0, mpi_size = 0;
static MPI_Comm mpi_comm;
#else
const int myrank = 0;
#endif
int tokc = 0;
char * * tokv = NULL;

enum options {
  OPT_ROF      =  1,  // Run on Failure
  OPT_RCHK     =  2,  // Read Data Check
  OPT_XPERF    =  4,  // Extended Performance Messages
  OPT_PERFXCHK =  8,  // Exclude check time from Performance messages
  OPT_SMSGV1   = 16,  // Print success message v 1 so it can be suppressed with v 0 
  OPT_PAVM     = 32   // Pavilion Messages
};

static enum options options = 0;

ENUM_START(etab_opt)
ENUM_NAMP(OPT_, ROF)
ENUM_NAMP(OPT_, RCHK)
ENUM_NAMP(OPT_, XPERF)
ENUM_NAMP(OPT_, PERFXCHK)
ENUM_NAMP(OPT_, SMSGV1)
ENUM_NAMP(OPT_, PAVM)
ENUM_END(etab_opt, 1, "+")

static char * options_init = "-ROF+RCHK+XPERF-PERFXCHK-SMSGV1-PAVM"; 

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
#define ACTION_CHECK(name) void (name)(struct action *actionp, int tokn)
typedef ACTION_CHECK(action_check);

#define ACTION_RUN(name) void (name)(struct action *actionp, int *pactn)
typedef ACTION_RUN(action_run);

struct action {
  int tokn;             // Index of first token for action
  int actn;             // Index of this action element
  char * action;        // Action name
  char * desc;          // Action description
  action_check * checker;
  action_run * runner;
  PVAL v[MAX_PARAM];    // Action values
};
#define A (*actionp)
#define V0 (actionp->v[0])
#define V1 (actionp->v[1])
#define V2 (actionp->v[2])
#define V3 (actionp->v[3])
#define V4 (actionp->v[4])

int actc = 0;
ACTION * actv;

MSG_CONTEXT my_msg_context;
#define MY_MSG_CTX (&my_msg_context)

// Common read / write buffer pointers, etc.  Set by dbuf action.
static void * wbuf_ptr = NULL;
static void * rbuf_ptr = NULL;
static size_t rwbuf_len = 0;          // length not counting pattern overrun area
static U64 wbuf_data_object_hash_mod; // data_object hash modulus
static U64 wbuf_bdy;                  // wbuf address boundary
static U64 wbuf_repeat_len;           // repeat length of wbuf pattern

static ETIMER local_tmr;
//----------------------------------------------------------------------------
// Common subroutines and macros
//----------------------------------------------------------------------------
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

  // Serialize execution of all MPI ranks
  #define RANK_SERIALIZE_START                           \
    DBG4("RANK_SERIALIZE_START");                        \
    MPI_CK(MPI_Barrier(mpi_comm));                       \
    if (mpi_size > 0 && myrank != 0) {                   \
      char buf;                                          \
      MPI_Status status;                                 \
      MPI_CK(MPI_Recv(&buf, 1, MPI_CHAR, myrank - 1,     \
             MPI_ANY_TAG, mpi_comm, &status));           \
    }
  #define RANK_SERIALIZE_END                             \
    DBG4("RANK_SERIALIZE_END");                          \
    if (mpi_size > 0 && myrank != mpi_size - 1) {        \
      char buf;                                          \
      MPI_CK(MPI_Send(&buf, 1, MPI_CHAR, myrank + 1,     \
             0, mpi_comm));                              \
    }                                                    \
    MPI_CK(MPI_Barrier(mpi_comm));

#else
  #define RANK_SERIALIZE_START
  #define RANK_SERIALIZE_END
#endif

int mpi_active(void) {
  #ifdef MPI
    int mpi_init_flag, mpi_final_flag;
    MPI_Initialized(&mpi_init_flag);
    if (mpi_init_flag) {
      MPI_Finalized(&mpi_final_flag);
      if (! mpi_final_flag) {
        return 1;
      } 
    }
  #endif 
  return 0;
}

#define R0_OR_VERB_START                                                                  \
  if ( (MY_MSG_CTX)->verbose_level >= VERB_LEV_MULTI ) RANK_SERIALIZE_START;              \
  if ( mpi_size == 0 || myrank == 0 || (MY_MSG_CTX)->verbose_level >= VERB_LEV_MULTI ) {

#define R0_OR_VERB_END                                                                    \
  }                                                                                       \
  if ( (MY_MSG_CTX)->verbose_level >= VERB_LEV_MULTI ) RANK_SERIALIZE_END;

void get_id() {
  char * p;
  char tmp_id[sizeof(id_string)];
  int rc;

  rc = gethostname(tmp_id, sizeof(tmp_id));
  if (rc != 0) ERRX("gethostname rc: %d %s", rc, strerror(errno));
  p = strchr(tmp_id, '.');
  if (p) *p = '\0';

  #ifdef MPI
  if (mpi_active()) {  
    MPI_CK(MPI_Comm_rank(mpi_comm, &myrank));
    sprintf(tmp_id+strlen(tmp_id), ".%d", myrank);
    MPI_CK(MPI_Comm_size(mpi_comm, &mpi_size));
    sprintf(tmp_id+strlen(tmp_id), "/%d", mpi_size);
  } else {
  mpi_size = 0;
  }
  #endif // MPI
  strcat(tmp_id, " ");
  strcpy(id_string, tmp_id);
  id_string_len = strlen(id_string);
  MY_MSG_CTX->id_string=id_string;
}

//----------------------------------------------------------------------------
// lfsr_22_byte - bytewise 22 bit linear feedback shift register.
// Taps at bits 21 & 22 (origin 1) to provide 2^22-1 repeat cycle.
//----------------------------------------------------------------------------
static unsigned char lfsr_state[23]; // One extra byte to hold shift out

#define LFSR_22_CYCLE (4 * 1024 * 1024 - 1)

void lfsr_22_byte(unsigned char * p, U64 len) {
  while (len--) {
    memmove(lfsr_state+1, lfsr_state, sizeof(lfsr_state) - 1);
    lfsr_state[0] = lfsr_state[21] ^ lfsr_state[22] ^ 0xFF;
    *p++ = lfsr_state[22];
  }
}

void lfsr_22_byte_init(void) {
  srandom(15485863); // The 1 millionth prime
  for (int i = 0; i<sizeof(lfsr_state); ++i) {
    lfsr_state[i] = random() % 256;
  }
}

void lfsr_22_byte_init_p(void) {
  // Use a very simple PRNG to initialize lfsr_state
  int prime = 15485863; // The 1 millionth prime
  lfsr_state[0] = 0xA5;
  for (int i = 1; i<sizeof(lfsr_state); ++i) {
    lfsr_state[i] = (lfsr_state[i-1] * prime) % 256;
  }
  // Cycle a few times to mix things up
  unsigned char t[1000];
  lfsr_22_byte(t, sizeof(t));
}

# if 1
void lfsr_test(void) {
  // A few tests for lfsr properties
  U64 size = 8 * 1024 * 1024;
  unsigned char * buf = MALLOCX(size);

  lfsr_22_byte_init();

  printf("lfsr_state:\n");
  hex_dump(lfsr_state, sizeof(lfsr_state));

  lfsr_22_byte(buf, size);

  printf("buf:\n");
  hex_dump(buf, 256);

  printf("buf + %d:\n", LFSR_22_CYCLE);
  hex_dump(buf+LFSR_22_CYCLE, 256);

  int b[8] = {0,0,0,0,0,0,0,0};
  int j = 0;
  for (int i = 0; i<256; ++i) {
    int n = ((unsigned char *)buf)[i];
    j += n;
    for (int k=0; k<8; ++k) {
      unsigned char m = 1 << k;
      if (m & n) b[k]++;
    }
       
  }
  VERB0("j: %d", j);
  for (int k=0; k<8; ++k) {
    VERB0("b[%d]: %d", k, b[k]);
  } 


  for (int j=0; j<100; ++j) {
    int sum = 0;
    for (int i=0; i<100; ++i) {
      sum += buf[j*1000+i];
    }
    printf("sum: %d sum/100: %f\n", sum, sum/100.0);
  }

  for (int i = 0; i<4*1024*1024; ++i) {
    if (0 == i%1000) printf("i=%d\n", i);
    for (int j = i+1; j<4*1024*1024; ++j) {
      if (!memcmp(buf+i, buf+j, 8)) {
        printf("match at %d, %d\n", i, j);
      }
     }
  }
}
#endif

//----------------------------------------------------------------------------
// fread and fwrite with redrive
//----------------------------------------------------------------------------
ssize_t fwrite_rd (FILE *file, const void *ptr, size_t count) {
  ssize_t actual, total = 0;

  do {
    actual = fwrite (ptr, 1, count, file);

    if (actual > 0) {
      total += actual;
      count -= actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    }
  } while (count > 0 && (actual > 0 || (-1 == actual && EINTR == errno)) );

  return (actual < 0) ? actual: total;
}

ssize_t fread_rd (FILE *file, void *ptr, size_t count) {
  ssize_t actual, total = 0;

  do {
    actual = fread (ptr, 1, count, file);

    if (actual > 0) {
      total += actual;
      count -= actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    }
  } while (count > 0 && (actual > 0 || (-1 == actual && EINTR == errno)) );

  return (actual < 0) ? actual: total;
}

//----------------------------------------------------------------------------
// If MPI & Rank 0, VERB1 message with collective min/mean/max/stddev/total
// Always VERB2 local value message
//----------------------------------------------------------------------------
#ifdef MPI
struct mm_val_s {
  double val;
  char id[24];
};

struct min_max_s {
  struct mm_val_s min;
  struct mm_val_s max;
};

void min_max_who(void *invec, void *inoutvec, int *len, MPI_Datatype *datatype) {
  for (int j=0; j<*len; ++j) {
    struct min_max_s *vi = ((struct min_max_s *)invec) + j;
    struct min_max_s *vo = ((struct min_max_s *)inoutvec) + j;

    if ( (*vi).min.val < (*vo).min.val ) {
      (*vo).min = (*vi).min;
    }
    if ( (*vi).max.val > (*vo).max.val ) {
      (*vo).max = (*vi).max;
    }
  }
}
#endif // MPI

void prt_mmmst(double val, char * desc, char * unit) {
  #ifdef MPI
    if (mpi_size > 0) {
      double sum, mean, diff, sumdiff, sd;
      struct min_max_s mm, mmr;
      MPI_Datatype mmtype;
      MPI_Op mmw_op;

      // Set up MPI data type and op
      MPI_CK( MPI_Type_contiguous(sizeof(struct min_max_s), MPI_BYTE, &mmtype) );
      MPI_CK( MPI_Type_commit(&mmtype) );
      MPI_CK( MPI_Op_create(min_max_who, true, & mmw_op) );

      // Use two pass method to calculate variance
      MPI_CK( MPI_Allreduce(&val, &sum, 1, MPI_DOUBLE, MPI_SUM, mpi_comm) );
      mean = sum/(double)mpi_size;
      diff = (val - mean) * (val - mean);
      MPI_CK( MPI_Reduce(&diff, &sumdiff, 1, MPI_DOUBLE, MPI_SUM, 0, mpi_comm) );
      sd = sqrt(sumdiff / mpi_size);

      // Get min, max
      mm.min.val = mm.max.val = val;
      strncpy(mm.min.id, id_string, sizeof(mm.min.id)); 
      mm.min.id[sizeof(mm.min.id)-1] = '\0';
      char * p = strchr(mm.min.id, ' ');
      if (p) *p = '\0';
      strncpy(mm.max.id, mm.min.id, sizeof(mm.max.id)); 
      MPI_CK( MPI_Reduce(&mm, &mmr, 1, mmtype, mmw_op, 0, mpi_comm) );
      MPI_CK( MPI_Type_free(&mmtype) );
      MPI_CK( MPI_Op_free(&mmw_op) );

      char b1[32], b2[32], b3[32], b4[32], b5[32];
      if (0 == myrank) VERB1("%s mean/min/max/s/tot: %s %s %s %s %s %s   min/max: %s %s", desc,
                             eng_not(b1, sizeof(b1), mean, "D6.4", ""),
                             eng_not(b2, sizeof(b2), mmr.min.val,  "D6.4", ""), 
                             eng_not(b3, sizeof(b3), mmr.max.val,  "D6.4", ""), 
                             eng_not(b4, sizeof(b4), sd,   "D6.4", ""),
                             eng_not(b5, sizeof(b5), sum,  "D6.4", ""), unit,
                             mmr.min.id, mmr.max.id);
    } else {
  #endif
    char b1[32];  
    VERB1("%s: %s", desc, eng_not(b1, sizeof(b1), val, "D6.4", unit));
  } 
}

//----------------------------------------------------------------------------
// Action handler routines
//----------------------------------------------------------------------------
//----------------------------------------------------------------------------
// ztest action handler - for misc testing, modify as needed
//----------------------------------------------------------------------------
ACTION_RUN(ztest_run) {
  I64 i0 = V0.u;
  double d1 = V1.d;
  char * s2 = V2.s; 
  char * s3 = V3.s; 

  VERB0("Nothing to see here, move along", i0, d1, s2, s3); // Vars passed to avoid warning

  //char buf[32];
  //printf("eng_not(%lg, %s, %s) --> \"%s\"\n", d1, s2, s3, eng_not(buf, sizeof(buf), d1, s2, s3));

  //lfsr_test();

  //prt_mmmst((double) myrank, "rank", "");
 
}

//----------------------------------------------------------------------------
// v, d (verbose, debug) action handlers
//----------------------------------------------------------------------------
ACTION_CHECK(verbose_check) {
  int verbose = V0.u;
  if (verbose > MAX_VERBOSE) ERRX("%s: Verbosity level %d > maximum %d", A.desc, verbose, MAX_VERBOSE);
}

ACTION_RUN(verbose_run) {
  int verbose = V0.u;
  msg_context_set_verbose(MY_MSG_CTX, verbose);
  VERB2("Verbosity level set to %d", verbose);
}

ACTION_CHECK(debug_check) {
  int debug = V0.u;
  if (debug > DBGMAXLEV) ERRX("%s: requested debug level %d > maximum %d."
                              " Rebuild with -DDBGMAXLEV=<n> to increase"
                              " (see comments in source.)", A.desc, debug, DBGMAXLEV);
  if (A.actn == 0) {
    msg_context_set_debug(MY_MSG_CTX, debug);
    VERB2("Parse debug level set to %d", debug);
  }
}

ACTION_RUN(debug_run) {
  int debug = V0.u;
  if (A.actn != 0) {
    msg_context_set_debug(MY_MSG_CTX, debug);
    VERB2("Run debug level set to %d", debug);
  }
}

//----------------------------------------------------------------------------
// opt (set option flag) action handler
//----------------------------------------------------------------------------

void parse_opt(char *ctx, char *flags, int *set, int *clear) {
  int rc = flag2enum(MY_MSG_CTX, &etab_opt, flags, set, clear);
  if (rc) ERRX("%s; invalid option. Valid values are +/- %s", ctx, enum_list(MY_MSG_CTX, &etab_opt));
  DBG4("%s: flags: %s set: 0x%X  clear: 0x%X", ctx, flags, *set, *clear);
}

void show_opt(char *ctx, int val) {
  char * opt_desc;
  enum2str(MY_MSG_CTX, &etab_opt, val, &opt_desc);
  VERB1("%s options: %s", ctx, opt_desc);
  FREEX(opt_desc);
}

ACTION_CHECK(opt_check) {
  char * flags = V0.s;
  if (0 == strcmp("?", flags)) V0.c = ~(V0.i = 0);
  else parse_opt(A.desc, flags, &V0.i, &V0.c);
  DBG4("opt: flags: %s set: 0x%X  clear: 0x%X \n", flags, V0.i, V0.c);
}

ACTION_RUN(opt_run) {
  if (0 == V0.i && ~0 == V0.c) show_opt(A.desc, options);
  else options = V0.c & (V0.i | options); 
}

//----------------------------------------------------------------------------
// name action handler
//----------------------------------------------------------------------------
ACTION_RUN(name_run) {
  test_name = V0.s;
}

//----------------------------------------------------------------------------
// im (imbed) action handler
//----------------------------------------------------------------------------
void add2actv(ACTION * newact) {
  actv = REALLOCX(actv, (actc + 1) * sizeof(ACTION));
  memcpy(actv+actc, newact, sizeof(ACTION));
  actc++;
}

void add2tokv(int n, char * * newtok) {
  if (n == 0) return;
  tokv = REALLOCX(tokv, (tokc + n) * sizeof(char *));
  memcpy(tokv+tokc, newtok, n * sizeof(char *));
  tokc += n;
}

ACTION_CHECK(imbed_check) {
    // Open file and read into buffer
    FILE * file;
    char * fn = V0.s;
    if (!strcmp(fn, "-")) file = stdin;
    else file = fopen(fn, "r");
    if (!file) ERRX("%s: unable to open file %s: %s", A.desc, fn, strerror(errno));
    #define BUFSZ 1024*1024
    void * p = MALLOCX(BUFSZ);
    size_t size;
    size = fread(p, 1, BUFSZ, file);
    DBG4("fread %s returns %d", fn, size);
    if (ferror(file)) ERRX("%s: error reading file %s %d %s", A.desc, fn, ferror(file), strerror(ferror(file)));
    if (!feof(file)) ERRX("%s: imbed file %s larger than buffer (%d bytes)", A.desc, fn, BUFSZ);
    fclose(file);
    p = REALLOCX(p, size);

    // Save old tokc / tokv, copy up through current action into new tokc / tokv
    int old_tokc = tokc;
    char * * old_tokv = tokv;
    tokc = 0;
    tokv = NULL;
    add2tokv(tokn+1, old_tokv);

    // tokenize buffer, append to tokc / tokv
    char * sep = " \t\n\f\r";
    char * a = strtok(p, sep);
    while (a) {
      DBG4("imbed_hand add tok: \"%s\" tokc: %d", a, tokc);
      add2tokv(1, &a);
      a = strtok(NULL, sep);
    }

    // append remainder of old tokc / tokv to new
    add2tokv(old_tokc - tokn - 1, &old_tokv[tokn + 1]);
    FREEX(old_tokv);
}

//----------------------------------------------------------------------------
// srr (seed random rank) action handler
//----------------------------------------------------------------------------
ACTION_RUN(srr_run) {
  I64 seed = V0.u;
  #define PR231_100 (U64)2147481317              // Prime close to but < 2^31
  #define PR231_200 (U64)2147479259              // Prime close to but < 2^31
  unsigned short seed16v[3];

  // Multiply by prime so seed48(rank+1, seed) != seed(48(rank, seed+1)
  seed *= PR231_100;
  #ifdef MPI
    if (mpi_size > 0) seed += (myrank * PR231_200);
  #endif
  seed16v[0] = seed & 0xFFFF;
  seed16v[1] = seed >> 16;
  seed16v[2] = 0;
  DBG3("srr seed: %lld; calling seed48(0x%04hX 0x%04hX 0x%04hX)", seed, seed16v[2], seed16v[1], seed16v[0]);
  seed48(seed16v);
}

//----------------------------------------------------------------------------
// lc, lt, ls, le (looping) action handlers
//----------------------------------------------------------------------------
#define MAX_LOOP 16
enum looptype {COUNT, TIME, SYNC};
struct loop_ctl {
  enum looptype type;
  int count;
  double ltime;
  int top;
  ETIMER tmr;
} lctl[MAX_LOOP+1];
struct loop_ctl * lcur = &lctl[0];

ACTION_CHECK(loop_check) {
  if ( !strcmp(A.action, "lc")  ||
       !strcmp(A.action, "lcr") ||
    #ifdef MPI
       !strcmp(A.action, "ls")  ||
    #endif
       !strcmp(A.action, "lt") ) {
    if (++lcur - lctl >= MAX_LOOP) ERRX("%s: Maximum nested loop depth of %d exceeded", A.desc, MAX_LOOP);
  } else if (!strcmp(A.action, "le")) {
    if (lcur <= lctl) ERRX("%s: loop end when no loop active - more loop ends than loop starts", A.desc);
    lcur--;
  } else ERRX("%s: internal error loop_hand invalid action: %s", A.desc, A.action);
}

ACTION_RUN(lc_run) {
  lcur++;
  DBG4("loop count start; depth: %d top actn: %d count: %d", lcur-lctl, *pactn, V0.u);
  lcur->type = COUNT;
  lcur->count = V0.u;
  lcur->top = *pactn;
}

ACTION_RUN(lcr_run) {
  int count = rand_range(V0.u, V1.u, 1);
  lcur++;
  DBG4("loop count rand start; depth: %d top actn: %d count: %d", lcur-lctl, *pactn, count);
  lcur->type = COUNT;
  lcur->count = count;
  lcur->top = *pactn;
}

ACTION_RUN(lt_run) {
  lcur++;
  DBG4("loop time start; depth: %d top actn: %d time: %d", lcur - lctl, *pactn, V0.u);
  lcur->type = TIME;
  lcur->top = *pactn;
  lcur->ltime = V0.d;
  ETIMER_START(&lcur->tmr);
}

#ifdef MPI
ACTION_RUN(ls_run) {
  lcur++;
  DBG4("loop sync start; depth: %d top actn: %d time: %d", lcur - lctl, *pactn, V0.u);
  lcur->type = SYNC;
  lcur->top = *pactn;
  lcur->ltime = V0.d;
  if (myrank == 0) {
    ETIMER_START(&lcur->tmr);
  }
}
#endif // MPI

ACTION_RUN(le_run) {
  int time2stop = 0;
  switch (lcur->type) {
    case COUNT:;
      if (--lcur->count > 0) {
        *pactn = lcur->top;
        DBG4("loop count end, not done; depth: %d top actn: %d count: %d", lcur-lctl, lcur->top, lcur->count);
      } else {
        DBG4("loop count end, done; depth: %d top actn: %d count: %d", lcur-lctl, lcur->top, lcur->count);
        lcur--;
      }
      break;
    case TIME:;
      if (lcur->ltime <= ETIMER_ELAPSED(&lcur->tmr)) {
        DBG4("loop time end, done; depth: %d top actn: %d", lcur-lctl, lcur->top);
        lcur--;
      } else {
        *pactn = lcur->top;
        DBG4("loop time end, not done; depth: %d top actn: %d", lcur-lctl, lcur->top);
      }
      break;
    #ifdef MPI
    case SYNC:;
      if (myrank == 0) {
        if (lcur->ltime <= ETIMER_ELAPSED(&lcur->tmr)) {
          DBG4("loop sync rank 0 end, done; depth: %d top actn: %d", lcur-lctl, lcur->top);
          time2stop = 1;
        } else {
          DBG4("loop sync rank 0 end, not done; depth: %d top actn: %d", lcur-lctl, lcur->top);
        }
      }
      MPI_CK(MPI_Bcast(&time2stop, 1, MPI_INT, 0, mpi_comm));
      if (time2stop) {
        VERB1("loop sync end, done; depth: %d top actn: %d", lcur-lctl, lcur->top);
        lcur--;
      } else {
        *pactn = lcur->top;
        DBG4("loop sync end, not done; depth: %d top actn: %d", lcur-lctl, lcur->top);
      }
      break;
    #endif // MPI
    default:
      ERRX("%s: internal error le_run invalid looptype %d", A.desc, lcur->type);
  }
}

//----------------------------------------------------------------------------
// ifr, eif action handler
//----------------------------------------------------------------------------
int ifr_depth = 0;
int ife_num;

ACTION_CHECK(ifr_check) {
  if (ifr_depth > 0) 
    ERRX("%s: nested ifr", A.desc);
  ifr_depth++; 
}

ACTION_CHECK(eif_check) {
  if (ifr_depth != 1) 
    ERRX("%s: ife without preceeding ifr", A.desc);
  ifr_depth--;
  ife_num = A.actn - 1;
}

ACTION_RUN(ifr_run) {
  int req_rank = V0.u;
  int cond = false;  // false --> don't run body of ifr
  
  if (mpi_active()) {
    if (req_rank >= 0) {
      if (myrank == req_rank) cond = true;
    } else {
      if ( myrank == mpi_size + req_rank) cond = true;
    } 
  } else {
    cond = true;
  }
  
  if (!cond) *pactn = ife_num;

  DBG4("ifr done req_rank: %d cond: %d actn: %d", req_rank, cond, *pactn);
}

ACTION_RUN(eif_run) {
  DBG4("ife done actn: %d", *pactn);
}

//----------------------------------------------------------------------------
// dbuf action handler and related data validation routines
//----------------------------------------------------------------------------
enum dbuf_type {RAND22P, RAND22, OFS20};
ENUM_START(etab_dbuf)  // Write buffer type
ENUM_NAME("RAND22P", RAND22P)
ENUM_NAME("RAND22", RAND22)
ENUM_NAME("OFS20",  OFS20)
ENUM_END(etab_dbuf, 0, NULL)

#define PRIME_LT_64KI    65521 // A prime a little less than 2**16
#define PRIME_75PCT_2MI 786431 // A prime about 75% of 2**20
#define PRIME_LT_2MI   1048573 // A prime a little less that 2**20

void dbuf_init(enum dbuf_type type, U64 size) {
  rwbuf_len = size;
  int rc;

  FREEX(wbuf_ptr);
  FREEX(rbuf_ptr);

  switch (type) {
    case RAND22P:
      wbuf_bdy = 1;
      wbuf_repeat_len = LFSR_22_CYCLE;
      wbuf_data_object_hash_mod = PRIME_LT_64KI;
      rc = posix_memalign((void * *)&wbuf_ptr, 4096, rwbuf_len + wbuf_repeat_len);
      if (rc) ERRX("wbuf posix_memalign %d bytes failed: %s", rwbuf_len + wbuf_repeat_len, strerror(rc));
      lfsr_22_byte_init_p();
      lfsr_22_byte(wbuf_ptr, rwbuf_len + wbuf_repeat_len);
      break;
    case RAND22:
      wbuf_bdy = 1;
      wbuf_repeat_len = LFSR_22_CYCLE;
      wbuf_data_object_hash_mod = PRIME_LT_2MI;
      rc = posix_memalign((void * *)&wbuf_ptr, 4096, rwbuf_len + wbuf_repeat_len);
      if (rc) ERRX("wbuf posix_memalign %d bytes failed: %s", rwbuf_len + wbuf_repeat_len, strerror(rc));
      lfsr_22_byte_init();
      lfsr_22_byte(wbuf_ptr, rwbuf_len + wbuf_repeat_len);
      break;
    case OFS20:
      wbuf_bdy = sizeof(uint32_t); 
      wbuf_repeat_len = 1024 * 1024;
      wbuf_data_object_hash_mod = 1024 * 1024;
      rc = posix_memalign((void * *)&wbuf_ptr, 4096, rwbuf_len + wbuf_repeat_len);
      if (rc) ERRX("wbuf posix_memalign %d bytes failed: %s", rwbuf_len + wbuf_repeat_len, strerror(rc));
      for (long i = 0; i<(rwbuf_len+wbuf_repeat_len)/sizeof(uint32_t); ++i) {
        ((uint32_t *)wbuf_ptr)[i] = i * sizeof(uint32_t);
      }
      break;
  }

  rbuf_ptr = MALLOCX(rwbuf_len);

  DBG3("dbuf_init type: %s size: %lld wbuf_ptr: 0x%lX",              \
        enum_name(MY_MSG_CTX, &etab_dbuf, type), size, wbuf_ptr);  
  IFDBG3( hex_dump(wbuf_ptr, 32) );                                             

}

ACTION_CHECK(dbuf_check) {
  rwbuf_len = V1.u;
}

ACTION_RUN(dbuf_run) {
  dbuf_init(V0.i, V1.u);
}

// Returns a pointer into wbuf based on offset and data object hash
void * get_wbuf_ptr(char * ctx, U64 offset, U64 data_object_hash) {
  void * expected = wbuf_ptr + ( (offset + data_object_hash) % wbuf_repeat_len);
  DBG2("%s: object_hash: 0x%lX  wbuf_ptr: 0x%lX expected-wbuf_ptr: 0x%lX", ctx, data_object_hash, wbuf_ptr, expected - wbuf_ptr);  
  return expected;
}

// Convert string that uniquely identifies a data object into a hash
U64 get_data_object_hash(char * id_string) {
  U64 obj_hash = BDYDN(crc32(0, id_string, strlen(id_string)) % wbuf_data_object_hash_mod, wbuf_bdy);
  DBG4("data_object_hash: \"%s\" 0x%lX", id_string, obj_hash);
  return obj_hash;
}

int check_read_data(char * ctx, void * buf, size_t len, U64 offset, U64 data_object_hash) {
  int rc;
  void * expected = get_wbuf_ptr("hew", offset, data_object_hash);
  void * mis_comp;
  if ( ! (mis_comp = memdiff(buf, expected, len)) ) {
    rc = 0;
    VERB3("hio_element_read data check successful");
  } else {
    // Read data miscompare - dump lots of data about it
    rc = 1;
    I64 misc_ofs = (char *)mis_comp - (char *)buf;
    I64 dump_start = MAX(0, misc_ofs - 16) & (~15);
    VERB0("Error: %s data check miscompare", ctx);
    VERB0("       Data offset: 0x%llX  %lld", offset, offset); 
    VERB0("       Data length: 0x%llX  %lld", len, len); 
    VERB0("      Data address: 0x%llX", buf); 
    VERB0("Miscompare address: 0x%llX", mis_comp); 
    VERB0(" Miscompare offset: 0x%llX  %lld", mis_comp-buf, mis_comp-buf); 

    VERB0("Debug: wbuf_ptr addr: 0x%lX:", wbuf_ptr); hex_dump(wbuf_ptr, 32);

    VERB0("Miscompare expected data at offset 0x%llX %lld follows:", dump_start, dump_start);
    hex_dump( expected + dump_start, 96);

    VERB0("Miscompare actual data at offset 0x%llX %lld follows:", dump_start, dump_start);
    hex_dump( buf + dump_start, 96);

    VERB0("XOR of Expected addr: 0x%lX  Miscomp addr: 0x%lX", expected, mis_comp);
    char * xorbuf_ptr = MALLOCX(len);
    for (int i=0; i<len; i++) {
      ((char *)xorbuf_ptr)[i] = ((char *)buf)[i] ^ ((char *)expected)[i];
    }
    hex_dump( xorbuf_ptr, len);
    FREEX(xorbuf_ptr);

  }
  return rc;
}
//----------------------------------------------------------------------------
// o, e (stdout, stderr) action handlers
//----------------------------------------------------------------------------
ACTION_RUN(stdout_run) {
  U64 line;
  for (line = 1; line <= V0.u; line++) {
    // Message padded to exactly 100 bytes long.
    MSG("action %-4u stdout line %-8lu of %-8lu %*s", A.actn + 1, line, V0.u, 34 - id_string_len, "");
  }
}

ACTION_RUN(stderr_run) {
  U64 line;
  for (line = 1; line <= V0.u; line++) {
    // Message padded to exactly 100 bytes long.
    MSGE("action %-4u stderr line %-8lu of %-8lu %*s", A.actn + 1, line, V0.u, 34 - id_string_len, "");
  }
}

//----------------------------------------------------------------------------
// s (sleep) action handler
//----------------------------------------------------------------------------

ACTION_CHECK(sleep_check) {
  if (V0.d < 0) ERRX("%s; negative sleep seconds", A.desc);
}

ACTION_RUN(sleep_run) {
  fsleep(V0.d);
}

//----------------------------------------------------------------------------
// va, vt, vf (memory allocate, touch, free) action handlers
//----------------------------------------------------------------------------
struct memblk {
  size_t size;
  struct memblk * prev;
};
static struct memblk * memptr;
static int memcount;

ACTION_CHECK(va_check) {
  memcount++;
}

ACTION_CHECK(vt_check) {
  if (memcount <= 0) ERRX("%s: touch without corresponding allocate", A.desc);
}

ACTION_CHECK(vf_check) {
  if (memcount-- <= 0) ERRX("%s: free without corresponding allocate", A.desc);
}


ACTION_RUN(va_run) {
  memcount++;
  size_t len = V0.u;
  struct memblk * p;
  DBG2("Calling malloc(%lld)", len);
  p = (struct memblk *)malloc(len);
  VERB3("malloc returns %p", p);
  if (p) {
    p->size = len;
    p->prev = memptr;
    memptr = p;
  } else {
    VERB0("mem_hand - Warning: malloc returned NULL");
    memcount--;
  }
}


ACTION_RUN(vt_run) {
  U64 stride = V0.u;
  char *p, *end_p1;
  ETIMER tmr;
  if (memcount > 0) {
    p = (char*)memptr + sizeof(struct memblk);
    end_p1 = p + memptr->size;
    DBG4("Touching memory at %p, length 0x%llx, stride: %lld", p, memptr->size, stride);
    ETIMER_START(&tmr);
    while (p < end_p1) {
      DBG5("touch memptr: %p memlen: 0x%llx: end_p1: %p p: %p", memptr, memptr->size, end_p1, p);
      *p = 'x';
      p += stride;
    }
    double delta_t = ETIMER_ELAPSED(&tmr);
    U64 count = memptr->size / stride;
    prt_mmmst((double)count/delta_t, "vt rate", "TpS");

  } else {
    VERB0("mem_hand - Warning: no memory allocation to touch");
  }
}

ACTION_RUN(vf_run) {
  if (memcount > 0) {
    struct memblk * p;
    p = memptr->prev;
    DBG2("Calling free(%p)", memptr);
    free(memptr);
    memptr = p;
    memcount--;
  } else {
    VERB0("mem_hand - Warning: no memory allocation to free");
  }
}

#ifdef __linux__
//----------------------------------------------------------------------------
// dca action handlers
//----------------------------------------------------------------------------
ACTION_RUN(dca_run) {
  I64 aff = GetCPUaffinity();
  long np = sysconf(_SC_NPROCESSORS_CONF);
  RANK_SERIALIZE_START
  if (aff >= 0) {
    VERB0("_SC_NPROCESSORS_CONF: %d  CPU Affinity: %ld", np, aff);
  } else if ( aff < -1 ) {
    VERB0("_SC_NPROCESSORS_CONF: %d  CPU Affinity Mask: 0x%lX", np, -aff);
  } else {
    VERB0("_SC_NPROCESSORS_CONF: %d  CPU Affinity: None", np);
  }
  RANK_SERIALIZE_END
}

#endif // __linux__

//----------------------------------------------------------------------------
// mi, mb, mf (MPI init, barrier, finalize) mgf action handlers
//----------------------------------------------------------------------------
#ifdef MPI
static void *mpi_sbuf = NULL, *mpi_rbuf = NULL;
static size_t mpi_buf_len = 0;

ACTION_RUN(mi_run) {
  MPI_CK(MPI_Init(NULL, NULL));
  int shift = V0.u;
  mpi_comm = MPI_COMM_WORLD;
  get_id();
  if (shift > 0) {
    MPI_Group oldgroup, newgroup;
    int ranks[mpi_size];

    for (int i=0; i<mpi_size; ++i) {
      ranks[i] = (i + shift) % mpi_size;
      if (myrank == 0) VERB3("New rank %d is old rank %d", i, ranks[i]);
    }

    MPI_CK(MPI_Comm_group(MPI_COMM_WORLD, &oldgroup));
    MPI_CK(MPI_Group_incl(oldgroup, mpi_size, ranks, &newgroup));
    MPI_CK(MPI_Comm_create(MPI_COMM_WORLD, newgroup, &mpi_comm));

    get_id();
  }
}

ACTION_RUN(msr_run) {
  int len = V0.u;
  int stride = V1.u;
  MPI_Status status;
  if (mpi_buf_len != len) {
    mpi_sbuf = REALLOCX(mpi_sbuf, len);
    mpi_rbuf = REALLOCX(mpi_rbuf, len);
    mpi_buf_len = len;
  }
  int dest = (myrank + stride) % mpi_size;
  int source = (myrank - stride + mpi_size) % mpi_size;
  DBG2("msr len: %d dest: %d source: %d", len, dest, source);
  MPI_CK(MPI_Sendrecv(mpi_sbuf, len, MPI_BYTE, dest, 0,
                      mpi_rbuf, len, MPI_BYTE, source, 0,
                      mpi_comm, &status));
}

ACTION_RUN(mb_run) {
  MPI_CK(MPI_Barrier(mpi_comm));
}

ACTION_RUN(mgf_run) {
  int new_global_fails = 0;
  MPI_CK(MPI_Reduce(&local_fails, &new_global_fails, 1, MPI_INT, MPI_SUM, 0, mpi_comm));
  DBG4("mgf: old local fails: %d new global fails: %d", local_fails, new_global_fails);
  if (myrank == 0) global_fails += new_global_fails;
  local_fails = 0;
  gather_fails = 1;
}

ACTION_RUN(mf_run) {
  MPI_CK(MPI_Finalize());
  get_id();
  mpi_sbuf = FREEX(mpi_sbuf);
  mpi_rbuf = FREEX(mpi_rbuf);
  mpi_buf_len = 0;
}

//----------------------------------------------------------------------------
// fctw, fctr (file coherency test) action handlers
//----------------------------------------------------------------------------
ACTION_CHECK(fct_check) {
}

ACTION_RUN(fctw_run) {
  char * dir = V0.s;
  U64 file_num = V1.u;
  U64 file_sz = V2.u;
  U64 blk_sz = V3.u;

  void * buf = MALLOCX(blk_sz);

  if (0 == myrank) {
    for (int i=0; i<file_num; ++i) {
      char * fname = ALLOC_PRINTF("%s/test_%d", dir, i);
      FILE *f = fopen(fname, "w");
      if (!f) ERRX("%s: error opening \"%s\" %s", A.desc, fname, strerror(errno));
      I64 remain = file_sz;
      while (remain > 0) {
        I64 req_len = MIN(blk_sz, remain);
        DBG4("fctw: write %lld bytes to %s", req_len, fname);
        I64 act_len = fwrite(buf, 1, req_len, f);
        if (act_len != req_len) ERRX("%s error writing \"%s\", act_len: %lld req_len: %lld",
          A.desc, fname, act_len, req_len);
        remain -= act_len;
      }
      fclose(f);
      FREEX(fname);
    }
  }
  FREEX(buf);
}

ACTION_RUN(fctr_run) {
  char * dir = V0.s;
  U64 file_num = V1.u;
  U64 file_sz = V2.u;
  U64 blk_sz = V3.u;

  void * buf = MALLOCX(blk_sz);

  if (0 != myrank) {
    for (int i=file_num-1; i>=0; --i) {
      char * fname = ALLOC_PRINTF("%s/test_%d", dir, i);
      FILE *f = fopen(fname, "r");
      if (!f) ERRX("%s: error opening \"%s\" %s", A.desc, fname, strerror(errno));
      I64 remain = file_sz;
      while (remain > 0) {
        I64 req_len = MIN(blk_sz, remain);
        I64 act_len = fread(buf, 1, req_len, f);
        DBG4("fctr: read %lld bytes from %s", req_len, fname);
        if (act_len != req_len) ERRX("%s error reading \"%s\", act_len: %lld req_len: %lld",
          A.desc, fname, act_len, req_len);
        remain -= act_len;
      }
      fclose(f);
      FREEX(fname);
    }
  }
  FREEX(buf);
}

#endif // MPI

//----------------------------------------------------------------------------
// imd action handler
//----------------------------------------------------------------------------
ACTION_RUN(imd_run) {
  char * path = V0.s;
  I64 mode = V1.u;
  int rc;

  errno = 0;
  rc = mkdir(path, mode);
  if (rc || MY_MSG_CTX->verbose_level >= 3) {                                          
    MSG("mkdir(%s, 0%llo) rc: %d errno: %d(%s)", path, mode, rc, errno, strerror(errno));
  }
}

//----------------------------------------------------------------------------
// fget action handler
//----------------------------------------------------------------------------
ACTION_RUN(fget_run) {
  char * fn = V0.s;
  double delta_t;
  ETIMER tmr;
  FILE * f;
  U64 len = 0;

  #define BUFSIZE (1024*1024)
  char * buf = MALLOCX(BUFSIZE);

  ETIMER_START(&tmr);
  if (! (f = fopen(fn, "r+")) ) ERRX("fopen(%s, \"r\") errno: %d(%s)", fn, errno, strerror(errno));
  while ( fgets(buf, BUFSIZE, f) ) len += strlen(buf);
  if (! feof(f)) ERRX("fgets(%s) errn ERRX(fgets(%s) errno: %d(%s)", fn, errno, strerror(errno));
  if (fclose(f)) ERRX("fclose(%s) errno: %d(%s)", fn, errno, strerror(errno));
  delta_t = ETIMER_ELAPSED(&tmr);
  FREEX(buf);

  VERB1("fget %s done;  len: %llu  time: %f Seconds", fn, len, delta_t);
}

//----------------------------------------------------------------------------
// fo, fw, fr, gc file access action handler
//----------------------------------------------------------------------------

// Substitute rank & PID for %r and %p in string.  Must be free'd by caller
char * str_sub(char * string) {
  size_t needed = strlen(string) + 1;
  size_t dlen = needed + 16; // A little extra so hopefully realloc not needed
  char * dest = MALLOCX(dlen);
  char *s = string;
  char *d = dest;
  
  while (*s) {
    if ('%' == *s) {
      s++;
      switch (*s) {
        case 'p':
        case 'r':
          needed += 8;
          if (dlen < needed) {
            size_t pos = d - dest;
            dlen = needed;
            dest = REALLOCX(dest, dlen);
            d = dest + pos; 
          }
          d += sprintf(d, "%0.8d", (*s == 'p') ? getpid(): (mpi_active() ? myrank: 0) );
          s++;
          break;
        case '%': 
          *d++ = '%';
          break;
        default: 
          *d++ = *s++;
      }
    } else {
      *d++ = *s++;
    }      
  }
  *d = '\0';
  return dest;
}

static int fio_o_count = 0;
static char * fio_path = NULL;
static FILE * fio_file;
static U64 fio_ofs;
static U64 fio_id_hash;
static U64 fio_rw_count[2]; // read, write byte counts
ETIMER fio_foc_tmr;
double fio_o_time, fio_w_time, fio_r_time, fio_exc_time, fio_c_time;

#define IF_ERRNO(COND, FMT, ...)                                                 \
  if ( (COND) ) {                                                                \
    VERB0(FMT " failed.  errno: %d (%s)", __VA_ARGS__, errno, strerror(errno));  \
    local_fails++;                                                               \
  }


ACTION_CHECK(fo_check) {
  if (fio_o_count++ != 0) ERRX("nested fo");
  if (rwbuf_len == 0) rwbuf_len = 20 * 1024 * 1024;
} 

ACTION_RUN(fo_run) {
  fio_path = str_sub(V0.s);
  char * mode = V1.s;

  fio_o_time = fio_w_time = fio_r_time = fio_exc_time = fio_c_time = 0;
  fio_ofs = 0;

  if (! wbuf_ptr ) dbuf_init(RAND22, 20 * 1024 * 1024); 
  fio_id_hash = get_data_object_hash(fio_path);
  if (mpi_active()) MPI_CK(MPI_Barrier(mpi_comm));

  ETIMER_START(&fio_foc_tmr);
  ETIMER_START(&local_tmr);
  fio_file = fopen(fio_path, mode);
  IF_ERRNO( !fio_file, "fo: fopen(%s, %s)", fio_path, mode );
  fio_rw_count[0] = fio_rw_count[1] = 0;
} 

ACTION_CHECK(fw_check) {
  U64 size = V1.u;
  if (fio_o_count != 1) ERRX("fw without preceeding fo");
  if (size > rwbuf_len) ERRX("%s; size > rwbuf_len", A.desc);
} 
  

ACTION_RUN(fw_run) {
  ssize_t len_act;
  I64 ofs_param = V0.u;
  U64 len_req = V1.u;
  U64 ofs_abs;

  ofs_abs = fio_ofs + ofs_param;
  DBG2("fw ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", fio_ofs, ofs_param, ofs_abs, len_req);
  void * expected = get_wbuf_ptr("fw", ofs_abs, fio_id_hash);
  ETIMER_START(&local_tmr);

  if (fio_ofs != ofs_abs) {
    int rc = fseeko(fio_file, ofs_abs, SEEK_SET); 
    IF_ERRNO( rc, "fw: fseek(%s, %lld, SEEK_SET)", fio_path, ofs_abs);
    fio_ofs = ofs_abs;
  }

  len_act = fwrite_rd(fio_file, expected, len_req);
  fio_w_time += ETIMER_ELAPSED(&local_tmr);
  fio_ofs += len_act;
  IF_ERRNO( len_act != len_req, "fw: fwrite(%s, , %lld) len_act: %lld", fio_path, len_req, len_act);
  fio_rw_count[1] += len_act;
}

ACTION_CHECK(fr_check) {
  U64 size = V1.u;
  if (fio_o_count != 1) ERRX("fr without preceeding fo");
  if (size > rwbuf_len) ERRX("%s; size > rwbuf_len", A.desc);
}
 
ACTION_RUN(fr_run) {
  ssize_t len_act;
  I64 ofs_param = V0.u;
  U64 len_req = V1.u;
  U64 ofs_abs;

  ofs_abs = fio_ofs + ofs_param;
  DBG2("fr ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", fio_ofs, ofs_param, ofs_abs, len_req);

  ETIMER_START(&local_tmr);

  if (fio_ofs != ofs_abs) {
    int rc = fseeko(fio_file, ofs_abs, SEEK_SET); 
    IF_ERRNO( rc, "fr: fseek(%s, %lld, SEEK_SET)", fio_path, ofs_abs);
    fio_ofs = ofs_abs;
  }

  len_act = fread_rd(fio_file, rbuf_ptr, len_req);
  fio_r_time += ETIMER_ELAPSED(&local_tmr);
  fio_ofs += len_act;
  IF_ERRNO( len_act != len_req, "fr: fread(%s, , %lld) len_act: %lld", fio_path, len_req, len_act);
  
  if (options & OPT_RCHK) {
    ETIMER_START(&local_tmr);
    // Force error for unit test
    //*(char *)(rbuf_ptr+16) = '\0';
    int rc = check_read_data("fread", rbuf_ptr, len_req, ofs_abs, fio_id_hash);
    if (rc) { 
      local_fails++;
    }
    fio_exc_time += ETIMER_ELAPSED(&local_tmr);
  }
  fio_rw_count[0] += len_act;
}

ACTION_CHECK(fc_check) {
  if (fio_o_count != 1) ERRX("fc without preceeding fo");
  fio_o_count--;
}

ACTION_RUN(fc_run) {
  int rc;

  ETIMER_START(&local_tmr);
  rc = fclose(fio_file);
  fio_c_time += ETIMER_ELAPSED(&local_tmr);

  ETIMER_START(&local_tmr);
  if (mpi_active()) MPI_CK(MPI_Barrier(mpi_comm));
  double bar_time = ETIMER_ELAPSED(&local_tmr);
  double foc_time = ETIMER_ELAPSED(&fio_foc_tmr);

  IF_ERRNO( rc, "fc: fclose(%s)", fio_path);

  double una_time = foc_time - (fio_o_time + fio_w_time + fio_r_time +
                                fio_c_time + bar_time + fio_exc_time);

  char * desc = "         data check time"; 
  if (options & OPT_PERFXCHK) desc = "excluded data check time"; 

  if (options & OPT_XPERF) {  
    prt_mmmst(fio_o_time,   "              fopen time", "S");
    prt_mmmst(fio_w_time,   "             fwrite time", "S");
    prt_mmmst(fio_r_time,   "              fread time", "S");
    prt_mmmst(fio_c_time,   "             fclose time", "S");
    prt_mmmst(bar_time,     "post fclose barrier time", "S");
    prt_mmmst(fio_exc_time, desc,                       "S");
    prt_mmmst(foc_time,     " fopen-fclose total time", "S");
    prt_mmmst(una_time,     "    unaccounted for time", "S");
  }

  U64 rw_count_sum[2];
  if (mpi_active()) {
    #ifdef MPI
      MPI_CK(MPI_Reduce(fio_rw_count, rw_count_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, mpi_comm));
    #endif  
  } else {
    rw_count_sum[0] = fio_rw_count[0];
    rw_count_sum[1] = fio_rw_count[1];
  }
  
  if (0 == myrank) {
    if (options & OPT_PERFXCHK) foc_time -= fio_exc_time;
    char b1[32], b2[32], b3[32], b4[32], b5[32];
    VERB1("fo-fc R/W Size: %s %s  Time: %s  R/W Speed: %s %s",
          eng_not(b1, sizeof(b1), (double)rw_count_sum[0],    "B6.4", "B"),
          eng_not(b2, sizeof(b2), (double)rw_count_sum[1],    "B6.4", "B"),
          eng_not(b3, sizeof(b3), (double)foc_time,           "D6.4", "S"),
          eng_not(b4, sizeof(b4), rw_count_sum[0] / foc_time, "B6.4", "B/S"),
          eng_not(b5, sizeof(b5), rw_count_sum[1] / foc_time, "B6.4", "B/S"));
  }        
  fio_path = FREEX(fio_path);
}

//----------------------------------------------------------------------------
// ni, nr, nf (floating point addition init, run, free) action handlers
//----------------------------------------------------------------------------
static double * nums;
static U64 flap_size = 0, count;

ACTION_CHECK(ni_check) {
  flap_size = V0.u;
  count = V1.u;
  if (flap_size < 2) ERRX("%s; size must be at least 2", A.desc);
}

ACTION_CHECK(nr_check) {
  U64 rep = V0.u;
  U64 stride = V1.u;

  if (!flap_size) ERRX("%s; nr without prior ni", A.desc);
  if ((count-1)%stride != 0) ERRX("%s; count-1 must equal a multiple of stride", A.desc);
  if (rep<1) ERRX("%s; rep must be at least 1", A.desc);
}

ACTION_CHECK(nf_check) {
  if (!flap_size) ERRX("%s; nf without prior ni", A.desc);
  flap_size = 0;
}

ACTION_RUN(ni_run) {
  flap_size = V0.u;
  count = V1.u;
  U64 N = flap_size * count;

  int rc = posix_memalign((void * *)&nums, 4096, N * sizeof(double));
  if (rc) ERRX("%s; posix_memalign %d doubles failed: %s", A.desc, N, strerror(rc));

  U64 iv = 0;
  for (int i=0; i<N; ++i) {
    if (i%flap_size != 0) {
      nums[i] = (double) ++iv;
      DBG4("%s; nums[%d] = %d", A.desc, i, iv);
    }
  }
}

ACTION_RUN(nr_run) {
  double sum, delta_t, predicted;
  U64 b, ba, r, d, fp_add_ct, max_val;
  U64 N = flap_size * count;
  U64 rep = V0.u;
  U64 stride = V1.u;
  ETIMER tmr;

  max_val = (flap_size-1) * count;
  predicted = (pow((double) max_val, 2.0) + (double) max_val ) / 2 * (double)rep;
  DBG1("%s; v: %d predicted: %f", A.desc, max_val, predicted);
  fp_add_ct = (max_val * rep) + count;

  for (int i=0; i<N; i+=flap_size) {
    nums[i] = 0.0;
      DBG3("%s; nums[%d] = %d", A.desc, i, 0);
  }

  DBG1("flapper starting; size: %llu count: %llu rep: %llu stride: %llu", flap_size, count, rep, stride);
  ETIMER_START(&tmr);

  for (b=0; b<count; ++b) {
    ba = b * stride % count;
    U64 d_sum = ba*flap_size;
    U64 d_first = d_sum + 1;
    U64 d_lastp1 = (ba+1)*flap_size;
    DBG3("b: %llu ba:%llu", b, ba);
    for (r=0; r<rep; ++r) {
      sum = nums[d_sum];
      for (d=d_first; d<d_lastp1; ++d) {
        sum += nums[d];
        DBG3("%s; val: %f sum: %f", A.desc, nums[d], sum)
      }
      nums[d_sum] = sum;
    }
  }

  sum = 0.0;
  for (d=0; d<count*flap_size; d+=flap_size) {
    sum += nums[d];
  }

  delta_t = ETIMER_ELAPSED(&tmr);

  VERB2("flapper done; FP Adds %llu, predicted: %e sum: %e delta: %e", fp_add_ct, predicted, sum, sum - predicted);
  VERB2("FP Adds: %llu, time: %f Seconds, MFLAPS: %e", fp_add_ct, delta_t, (double)fp_add_ct / delta_t / 1000000.0);
  prt_mmmst(delta_t, " Add time", "S");
  prt_mmmst((double)fp_add_ct / delta_t, "FLAP rate", "F/S" );
}

ACTION_RUN(nf_run) {
  flap_size = 0;
  FREEX(nums);
}


//----------------------------------------------------------------------------
// hx (heap exerciser) action handler
//----------------------------------------------------------------------------
ACTION_CHECK(hx_check) {
  U64 min = V0.u;
  U64 max = V1.u;
  U64 limit = V3.u;

  if (min < 1) ERRX("%s; min < 1", A.desc);
  if (min > max) ERRX("%s; min > max", A.desc);
  if (max > limit) ERRX("%s; max > limit", A.desc);
}

ACTION_RUN(hx_run) {
  U64 min = V0.u;
  U64 max = V1.u;
  U64 blocks = V2.u;
  U64 limit = V3.u;
  U64 count = V4.u;

  double min_l2 = log2(min), max_l2 = log2(max);
  double range_l2 = max_l2 - min_l2;
  U64 i, n, k, total = 0;
  int b;
  ETIMER tmr;

  struct {
    void * ptr;
    size_t size;
  } blk [ blocks ];

  struct stat {
    U64 count;
    double atime;
    double ftime;
  } stat [ 1 + (int)log2(max) ];

  // Set up
  VERB1("heapx starting; min: %llu max: %llu blocks: %llu limit: %llu count: %llu", min, max, blocks, limit, count);

  for (n=0; n<blocks; ++n) {
    blk[n].ptr = NULL;
    blk[n].size = 0;
  }

  for (b=0; b<sizeof(stat)/sizeof(struct stat); ++b) {
    stat[b].count = 0;
    stat[b].atime = 0.0;
    stat[b].ftime = 0.0;
  }

  // Do allocations
  for (i=0; i<count; ++i) {

    n = random()%blocks;
    if (blk[n].ptr) {
      DBG4("heapx: total: %llu; free %td bytes", total, blk[n].size);
      b = (int) log2(blk[n].size);
      ETIMER_START(&tmr);
      free(blk[n].ptr);
      stat[b].ftime += ETIMER_ELAPSED(&tmr);
      total -= blk[n].size;
      blk[n].size = 0;
      blk[n].ptr = 0;
    }

    // blk[n].size = random()%(max - min + 1) + min;

    blk[n].size = (size_t)exp2( ((double)random() / (double)RAND_MAX * range_l2 ) + min_l2 );

    // Make sure limit will not be exceeded
    while (blk[n].size + total > limit) {
      k = random()%blocks;
      if (blk[k].ptr) {
        DBG4("heapx: total: %llu; free %td bytes", total, blk[k].size);
        b = (int) log2(blk[k].size);
        ETIMER_START(&tmr);
        free(blk[k].ptr);
        stat[b].ftime += ETIMER_ELAPSED(&tmr);
        total -= blk[k].size;
        blk[k].size = 0;
        blk[k].ptr = 0;
      }
    }

    VERB2("heapx: total: %llu; malloc and touch %td bytes", total, blk[n].size);
    b = (int) log2(blk[n].size);
    ETIMER_START(&tmr);
    blk[n].ptr = malloc(blk[n].size);
    stat[b].atime += ETIMER_ELAPSED(&tmr);
    if (!blk[n].ptr) ERRX("%s: heapx: malloc %td bytes failed", A.desc, blk[n].size);
    total += blk[n].size;
    stat[b].count++;
    memset(blk[n].ptr, 0xA5, blk[n].size);
  }

   // Clean up remainder
  for (n=0; n<blocks; ++n) {
    if (blk[n].ptr) {
      DBG4("heapx: total: %llu; free %td bytes", total, blk[n].size);
      b = (int) log2(blk[n].size);
      ETIMER_START(&tmr);
      free(blk[n].ptr);
      stat[b].ftime += ETIMER_ELAPSED(&tmr);
      total -= blk[n].size;
      blk[n].size = 0;
      blk[n].ptr = 0;
    }
  }

  // Reporting
  RANK_SERIALIZE_START
  for (b=0; b<sizeof(stat)/sizeof(struct stat); ++b) {
    if (stat[b].count > 0) {
      VERB2("heapx: bucket start: %lld count: %lld alloc_time: %.3f uS free_time %.3f uS", (long)exp2(b),
      stat[b].count, stat[b].atime*1e6/(double)stat[b].count, stat[b].ftime*1e6/(double)stat[b].count);
    }
  }
  RANK_SERIALIZE_END

}


//----------------------------------------------------------------------------
// dlo, dls, dlc (dl open, sym, close) action handlers
//----------------------------------------------------------------------------
#ifdef DLFCN
static int dl_num = -1;
static void * dl_handle[100];

ACTION_CHECK(dlo_check) {
  if (++dl_num >= DIM1(dl_handle)) ERRX("%s; too many dlo commands, limit is %d", A.desc, DIM1(dl_handle));
}

ACTION_CHECK(dls_check) {
  if (dl_num < 0) ERRX("%s; o currently open dynamic library", A.desc);
}

ACTION_CHECK(dlc_check) {
  if (dl_num-- < 0) ERRX("%s; no currently open dynamic library", A.desc);
}

ACTION_RUN(dlo_run) {
  char * name = V0.s;
  dl_handle[dl_num] = dlopen(name, RTLD_NOW);
  VERB3("%s; dlopen(%s) returns %p", A.desc, name, dl_handle[dl_num]);
  if (!dl_handle[dl_num]) {
    VERB0("%s; dlopen failed: %s", A.desc, dlerror());
    dl_num--;
  }
}

ACTION_RUN(dls_run) {
  char * symbol = V0.s;
  char * error = dlerror();
  void * sym = dlsym(dl_handle[dl_num], symbol);
  VERB3("%s; dlsym(%s) returns %p", A.desc, symbol, sym);
  error = dlerror();
  if (error) VERB0("%s; dlsym error: %s", A.desc, error);
}

ACTION_RUN(dlc_run) {
  int rc = dlclose(dl_handle[dl_num--]);
  VERB3("%s; dlclose returns %d", A.desc, rc);
  if (rc) VERB0("%s; dlclose error: %s", A.desc, dlerror());
}
#endif // DLFCN

//----------------------------------------------------------------------------
// Compile regex, handle errors
//----------------------------------------------------------------------------
void rx_comp(int n, struct action * actionp) {
  A.v[n].rxp = MALLOCX(sizeof(regex_t));

  int rc = regcomp(A.v[n].rxp, A.v[n].s, REG_EXTENDED | REG_NOSUB);
  if (rc) {
    char buf[512];
    regerror(rc, NULL, buf, sizeof(buf));
    ERRX("%s; regex: %s", A.desc, buf);
  }
}
//----------------------------------------------------------------------------
// Compare regex, handle errors. Returns 0 on match or REG_NOMATCH
//----------------------------------------------------------------------------
int rx_run(int n, struct action * actionp, char * line) {
  int rc = regexec(A.v[n].rxp, line, 0, NULL, 0);
  if (rc != 0 && rc != REG_NOMATCH) {
    char buf[512];
    regerror(rc, A.v[n].rxp, buf, sizeof(buf));
    ERRX("%s; regex: %s", A.desc, buf);
  }
  return rc;
}

//----------------------------------------------------------------------------
// hi, hda, hdo, heo, hew, her, hec, hdc, hdf, hf (HIO) action handlers
//----------------------------------------------------------------------------
//----------------------------------------------------------------------------
// Enum conversion tables
//----------------------------------------------------------------------------
ENUM_START(etab_onff)  // On, Off + case variants
ENUM_NAME("OFF", 0)
ENUM_NAME("ON",  1)
ENUM_NAME("off", 0)
ENUM_NAME("on",  1)
ENUM_NAME("Off", 0)
ENUM_NAME("On",  1)
ENUM_END(etab_onff, 0, NULL)

#ifdef HIO
ENUM_START(etab_hflg)
ENUM_NAMP(HIO_FLAG_, READ)
ENUM_NAMP(HIO_FLAG_, WRITE)
ENUM_NAMP(HIO_FLAG_, CREAT)
ENUM_NAMP(HIO_FLAG_, TRUNC)
ENUM_NAMP(HIO_FLAG_, APPEND)
ENUM_END(etab_hflg, 1, ",")

ENUM_START(etab_hdsm)  // hio dataset mode
ENUM_NAMP(HIO_SET_ELEMENT_, UNIQUE)
ENUM_NAMP(HIO_SET_ELEMENT_, SHARED)
ENUM_END(etab_hdsm, 0, NULL)

#define HIO_ANY 999    // "special" rc value, means any rc OK
ENUM_START(etab_herr)  // hio error codes
ENUM_NAMP(HIO_, SUCCESS)
ENUM_NAMP(HIO_, ERROR)
ENUM_NAMP(HIO_, ERR_PERM)
ENUM_NAMP(HIO_, ERR_TRUNCATE)
ENUM_NAMP(HIO_, ERR_OUT_OF_RESOURCE)
ENUM_NAMP(HIO_, ERR_NOT_FOUND)
ENUM_NAMP(HIO_, ERR_NOT_AVAILABLE)
ENUM_NAMP(HIO_, ERR_BAD_PARAM)
ENUM_NAMP(HIO_, ERR_EXISTS)
ENUM_NAMP(HIO_, ERR_IO_TEMPORARY)
ENUM_NAMP(HIO_, ERR_IO_PERMANENT)
ENUM_NAMP(HIO_, ANY)
ENUM_END(etab_herr, 0, NULL)

#define HIO_CNT_REQ -998
#define HIO_CNT_ANY -999

ENUM_START(etab_hcfg)  // hio_config_type_t
ENUM_NAMP(HIO_CONFIG_TYPE_, BOOL)
ENUM_NAMP(HIO_CONFIG_TYPE_, STRING)
ENUM_NAMP(HIO_CONFIG_TYPE_, INT32)
ENUM_NAMP(HIO_CONFIG_TYPE_, UINT32)
ENUM_NAMP(HIO_CONFIG_TYPE_, INT64)
ENUM_NAMP(HIO_CONFIG_TYPE_, UINT64)
ENUM_NAMP(HIO_CONFIG_TYPE_, FLOAT)
ENUM_NAMP(HIO_CONFIG_TYPE_, DOUBLE)
ENUM_END(etab_hcfg, 0, NULL)

ENUM_START(etab_hulm) // hio_unlink_mode_t
ENUM_NAMP(HIO_UNLINK_MODE_, CURRENT)
ENUM_NAMP(HIO_UNLINK_MODE_, FIRST)
ENUM_NAMP(HIO_UNLINK_MODE_, ALL)
ENUM_END(etab_hulm, 0, NULL)

ENUM_START(etab_hdsi) // hio_dataset_id
ENUM_NAMP(HIO_DATASET_, ID_NEWEST)
ENUM_NAMP(HIO_DATASET_, ID_HIGHEST)
ENUM_END(etab_hdsi, 0, NULL)

static hio_context_t context = NULL;
static hio_dataset_t dataset = NULL;
static hio_element_t element = NULL;
static char * hio_context_name;
static char * hio_dataset_name;
static I64 hio_ds_id_req;
static I64 hio_ds_id_act;
static hio_flags_t hio_dataset_flags;
static hio_dataset_mode_t hio_dataset_mode;
static char * hio_element_name;
static U64 hio_element_hash;
#define EL_HASH_MODULUS 65521     // A prime a little less than 2**16
static hio_return_t hio_rc_exp = HIO_SUCCESS;
static I64 hio_cnt_exp = HIO_CNT_REQ;
static I64 hio_dsid_exp = -999;
static int hio_dsid_exp_set = 0;
static int hio_fail = 0;
static U64 hio_e_ofs;
static I64 hseg_start = 0;
static U64 hio_rw_count[2];
static ETIMER hio_hdaf_tmr;
double hio_hda_time, hio_hdo_time, hio_heo_time, hio_hew_time, hio_her_time,
       hio_hec_time, hio_hdc_time, hio_hdf_time, hio_exc_time;

#define HRC_TEST(API_NAME)  {                                                                \
  local_fails += (hio_fail = (hrc != hio_rc_exp && hio_rc_exp != HIO_ANY) ? 1: 0);           \
  if (hio_fail || MY_MSG_CTX->verbose_level >= 3) {                                          \
    MSG("%s: " #API_NAME " %s; rc: %s exp: %s errno: %d(%s)", A.desc,                        \
         hio_fail ? "FAIL": "OK", enum_name(MY_MSG_CTX, &etab_herr, hrc),                    \
         enum_name(MY_MSG_CTX, &etab_herr, hio_rc_exp), errno, strerror(errno));             \
  }                                                                                          \
  if (hrc != HIO_SUCCESS) hio_err_print_all(context, stderr, "[" #API_NAME " error]");       \
  hio_rc_exp = HIO_SUCCESS;                                                                  \
}

#define HCNT_TEST(API_NAME)  {                                                               \
  if (HIO_CNT_REQ == hio_cnt_exp) hio_cnt_exp = hreq;                                        \
  local_fails += (hio_fail = ( hcnt != hio_cnt_exp && hio_cnt_exp != HIO_CNT_ANY ) ? 1: 0);  \
  if (hio_fail || MY_MSG_CTX->verbose_level >= 3) {                                          \
    MSG("%s: " #API_NAME " %s; cnt: %d exp: %d errno: %d(%s)", A.desc,                       \
         hio_fail ? "FAIL": "OK", hcnt, hio_cnt_exp, errno, strerror(errno));                \
  }                                                                                          \
  hio_err_print_all(context, stderr, "[" #API_NAME " error]");                               \
  hio_cnt_exp = HIO_CNT_REQ;                                                                 \
}

ACTION_RUN(hi_run) {
  hio_return_t hrc;
  hio_context_name = V0.s;
  char * data_root = V1.s;
  char * root_var_name = "data_roots";

  DBG2("Calling hio_init_mpi(&context, &mpi_comm, NULL, NULL, \"%s\")", hio_context_name);
  hrc = hio_init_mpi(&context, &mpi_comm, NULL, NULL, hio_context_name);
  HRC_TEST(hio_init_mpi)

  if (HIO_SUCCESS == hrc) {
    DBG2("Calling hio_config_set(context, \"%s\", \"%s\")", root_var_name, data_root);
    hrc = hio_config_set_value((hio_object_t)context, root_var_name, data_root);
    HRC_TEST(hio_config_set_value)
  }

  if (HIO_SUCCESS == hrc) {
    char * tmp_str = NULL;
    hrc = hio_config_get_value((hio_object_t)context, root_var_name, &tmp_str);
    HRC_TEST(hio_config_get_value)
    if (HIO_SUCCESS == hrc) {
      VERB3("hio_config_get_value var:%s value=\"%s\"", root_var_name, tmp_str);
    }
  }
}

ACTION_RUN(hda_run) {
  hio_return_t hrc = 0;
  hio_dataset_name = V0.s;
  hio_ds_id_req = V1.u;
  hio_dataset_flags = V2.i;
  hio_dataset_mode = V3.i;
  hio_rw_count[0] = hio_rw_count[1] = 0;
  MPI_CK(MPI_Barrier(mpi_comm));
  hio_hda_time = hio_hdo_time = hio_heo_time = hio_hew_time = hio_her_time =
                 hio_hec_time = hio_hdc_time = hio_hdf_time = hio_exc_time = 0.0;
  DBG2("hda_run: dataset: %p", dataset);
  DBG2("Calling hio_datset_alloc(context, &dataset, %s, %lld, %d(%s), %d(%s))", hio_dataset_name, hio_ds_id_req,
        hio_dataset_flags, V2.s, hio_dataset_mode, V3.s);
  ETIMER_START(&hio_hdaf_tmr);
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_alloc (context, &dataset, hio_dataset_name, hio_ds_id_req, hio_dataset_flags, hio_dataset_mode);
  hio_hda_time += ETIMER_ELAPSED(&local_tmr);
  DBG2("hda_run: dataset: %p", dataset);
  HRC_TEST(hio_dataset_alloc);
}

#include <inttypes.h>

ACTION_RUN(hdo_run) {
  hio_return_t hrc;
  DBG2("calling hio_dataset_open(%p)", dataset);
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_open (dataset);
  hio_hdo_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_dataset_open);
  if (HIO_SUCCESS == hrc) {
    hrc = hio_dataset_get_id(dataset, &hio_ds_id_act);
    HRC_TEST(hio_dataset_get_id);
    local_fails += hio_fail = (hio_dsid_exp_set && hio_dsid_exp != hio_ds_id_act);
    if (hio_fail || MY_MSG_CTX->verbose_level >= 3) {
      if (hio_dsid_exp_set) {
        MSG("%s: hio_dataset_get_id %s actual: %" PRIi64 " exp: %" PRIi64, A.desc, hio_fail ? "FAIL": "OK",
            hio_ds_id_act, hio_dsid_exp);
      } else {
        MSG("%s: hio_dataset_get_id actual %"PRIi64, A.desc, hio_ds_id_act);
      }
    }
  }
  hio_dsid_exp = -999;
  hio_dsid_exp_set = 0;
}

ACTION_CHECK(heo_check) {
  if (rwbuf_len == 0) rwbuf_len = 20 * 1024 * 1024;
}

ACTION_RUN(heo_run) {
  hio_return_t hrc;
  hio_element_name = V0.s;
  int flag_i = V1.i;
  ETIMER_START(&local_tmr);
  hrc = hio_element_open (dataset, &element, hio_element_name, flag_i);
  hio_heo_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_element_open)

  ETIMER_START(&local_tmr);
  if (! wbuf_ptr ) dbuf_init(RAND22P, 20 * 1024 * 1024); 

  char * element_id = ALLOC_PRINTF("%s %s %d %s %d", hio_context_name, hio_dataset_name,
                                   hio_ds_id_act, hio_element_name,
                                   (HIO_SET_ELEMENT_UNIQUE == hio_dataset_mode) ? myrank: 0);
  hio_element_hash = get_data_object_hash(element_id);
  //hio_element_hash = BDYDN(crc32(0, element_id, strlen(element_id)) % wbuf_data_object_hash_mod, wbuf_bdy);
  FREEX(element_id);

  hio_e_ofs = 0;
  hio_exc_time += ETIMER_ELAPSED(&local_tmr);
}

ACTION_RUN(hso_run) {
  hio_e_ofs = V0.u;
}

ACTION_CHECK(hew_check) {
  U64 size = V1.u;
  if (size > rwbuf_len) ERRX("%s; size > rwbuf_len", A.desc);
}

ACTION_RUN(hsega_run) {
  U64 start = V0.u;
  U64 size_per_rank = V1.u;
  U64 rank_shift = V2.u;
  hseg_start = start;
  hio_e_ofs = hseg_start + size_per_rank * ((myrank + rank_shift) % mpi_size);
  hseg_start += size_per_rank * mpi_size;
}

ACTION_RUN(hsegr_run) {
  U64 start = V0.u;
  U64 size_per_rank = V1.u;
  U64 rank_shift = V2.u;
  hseg_start += start;
  hio_e_ofs = hseg_start + size_per_rank * ((myrank + rank_shift) % mpi_size);
  hseg_start += size_per_rank * mpi_size;
}

ACTION_RUN(hew_run) {
  ssize_t hcnt;
  I64 ofs_param = V0.u;
  U64 hreq = V1.u;
  U64 ofs_abs;

  ofs_abs = hio_e_ofs + ofs_param;
  DBG2("hew el_ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", hio_e_ofs, ofs_param, ofs_abs, hreq);
  hio_e_ofs = ofs_abs + hreq;
  void * expected = get_wbuf_ptr("hew", ofs_abs, hio_element_hash);
  ETIMER_START(&local_tmr);
  hcnt = hio_element_write (element, ofs_abs, 0, expected, 1, hreq);
  hio_hew_time += ETIMER_ELAPSED(&local_tmr);
  HCNT_TEST(hio_element_write)
  hio_rw_count[1] += hcnt;
}

// Randomize length, then call hew action handler
ACTION_RUN(hewr_run) {
  struct action new = *actionp;
  new.v[1].u = rand_range(V1.u, V2.u, V3.u);
  hew_run(&new, pactn);
}

ACTION_CHECK(her_check) {
  U64 size = V1.u;
  if (size > rwbuf_len) ERRX("%s; size > rwbuf_len", A.desc);
}

ACTION_RUN(her_run) {
  ssize_t hcnt;
  I64 ofs_param = V0.u;
  U64 hreq = V1.u;
  U64 ofs_abs;

  ofs_abs = hio_e_ofs + ofs_param;
  DBG2("her el_ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", hio_e_ofs, ofs_param, ofs_abs, hreq);
  hio_e_ofs = ofs_abs + hreq;
  ETIMER_START(&local_tmr);
  hcnt = hio_element_read (element, ofs_abs, 0, rbuf_ptr, 1, hreq);
  hio_her_time += ETIMER_ELAPSED(&local_tmr);
  HCNT_TEST(hio_element_read)
  hio_rw_count[0] += hcnt;

  if (options & OPT_RCHK) {
    ETIMER_START(&local_tmr);
    // Force error for unit test
    // *(char *)(rbuf_ptr+16) = '\0';
    int rc = check_read_data("hio_element_read", rbuf_ptr, hreq, ofs_abs, hio_element_hash);
    if (rc) { 
      local_fails++;
      char * dw_path = getenv("DW_JOB_STRIPED");
      if (dw_path) {
        // Attempt to dump the datawarp physical file data directly (without HIO) 
        // This works for the current version of HIO in basic mode, no guarantees
        // going forward.
        char path[512];
        //                            root
        //                               ctx    dsn
        //                                         id              el_name
        int len = snprintf(path, sizeof(path), "%s/%s.hio/%s/%"PRIi64"/element_data.%s",
                           dw_path, hio_context_name, hio_dataset_name,
                           hio_ds_id_act, hio_element_name);
        if (HIO_SET_ELEMENT_UNIQUE == hio_dataset_mode) {
          snprintf(path+len, sizeof(path)-len, ".%05d", myrank);
        }

        FILE * elf = fopen(path, "r");
        if (!elf) {
          VERB0("fopen(\"%s\", \"r\") failed, errno: %d", path, errno);
        } else {
          int rc = fseeko(elf, ofs_abs, SEEK_SET);
          if (rc != 0 ) {
            VERB0("fseek( , %llu, SEEK_SET) failed, errno: %d", ofs_abs, errno);
          } else {
            char * fbuf = MALLOCX(hreq);
            ssize_t count = fread_rd(elf, fbuf, hreq);
            if (count != hreq) VERB0("Warning: fread_rd count: %ld, len_req: %ld", count, hreq);
            VERB0("File %s at offset 0x%lX:", path, ofs_abs);
            hex_dump(fbuf, hreq);  
            FREEX(fbuf);
            rc = fclose(elf); 
            if (rc != 0) VERB0("fclose failed rc: %d errno: %d", rc, errno);
          }
        } 
      }
    }
    hio_exc_time += ETIMER_ELAPSED(&local_tmr);
  }

}

// Randomize length, then call her action handler
ACTION_RUN(herr_run) {
  struct action new = *actionp;
  new.v[1].u = rand_range(V1.u, V2.u, V3.u);
  her_run(&new, pactn);
}

ACTION_RUN(hec_run) {
  hio_return_t hrc;
  ETIMER_START(&local_tmr);
  hrc = hio_element_close(&element);
  hio_hec_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_element_close)
}

#define GIGBIN (1024.0 * 1024.0 * 1024.0)

ACTION_RUN(hdc_run) {
  hio_return_t hrc;
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_close(dataset);
  hio_hdc_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_dataset_close)
}

ACTION_RUN(hdf_run) {
  hio_return_t hrc;
  DBG3("Calling hio_dataset_free(%p); dataset: %p", &dataset, dataset);
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_free(&dataset);
  hio_hdf_time += ETIMER_ELAPSED(&local_tmr);
  ETIMER_START(&local_tmr);
  MPI_CK(MPI_Barrier(mpi_comm));
  double bar_time = ETIMER_ELAPSED(&local_tmr);
  double hdaf_time = ETIMER_ELAPSED(&hio_hdaf_tmr);
  HRC_TEST(hio_dataset_close)
  U64 hio_rw_count_sum[2];
  MPI_CK(MPI_Reduce(hio_rw_count, hio_rw_count_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, mpi_comm));
  DBG3("After hio_dataset_free(); dataset: %p", dataset);
  IFDBG3( hex_dump(&dataset, sizeof(dataset)) );
  double una_time = hdaf_time - (hio_hda_time + hio_hdo_time + hio_heo_time + hio_hew_time + hio_her_time
                                 + hio_hec_time + hio_hdc_time + hio_hdf_time + bar_time + hio_exc_time);
 
  char * desc = "           data check time"; 
  if (options & OPT_PERFXCHK) desc = "  excluded data check time";
 
  if (options & OPT_XPERF) {  
    prt_mmmst(hio_hda_time, " hio_dataset_allocate time", "S");
    prt_mmmst(hio_hdo_time, "     hio_dataset_open time", "S");
    prt_mmmst(hio_heo_time, "     hio_element_open time", "S");
    prt_mmmst(hio_hew_time, "    hio_element_write time", "S");
    prt_mmmst(hio_her_time, "     hio_element_read time", "S");
    prt_mmmst(hio_hec_time, "    hio_element_close time", "S");
    prt_mmmst(hio_hdc_time, "    hio_dataset_close time", "S");
    prt_mmmst(hio_hdf_time, "     hio_dataset_free time", "S");
    prt_mmmst(bar_time,     "    post free barrier time", "S");
    prt_mmmst(hio_exc_time, desc ,                        "S");
    prt_mmmst(hdaf_time,    "        hda-hdf total time", "S");
    prt_mmmst(una_time,     "      unaccounted for time", "S");
  }

  if (myrank == 0) {
    if (options & OPT_PERFXCHK) hdaf_time -= hio_exc_time;
    char b1[32], b2[32], b3[32], b4[32], b5[32];
    VERB1("hda-hdf R/W Size: %s %s  Time: %s  R/W Speed: %s %s",
          eng_not(b1, sizeof(b1), (double)hio_rw_count_sum[0],     "B6.4", "B"),
          eng_not(b2, sizeof(b2), (double)hio_rw_count_sum[1],     "B6.4", "B"),
          eng_not(b3, sizeof(b3), (double)hdaf_time,           "D6.4", "S"),
          eng_not(b4, sizeof(b4), hio_rw_count_sum[0] / hdaf_time, "B6.4", "B/S"),
          eng_not(b5, sizeof(b5), hio_rw_count_sum[1] / hdaf_time, "B6.4", "B/S"));
    
    if (options & OPT_PAVM) {
      printf("<td> Read_speed %f GiB/S\n", hio_rw_count_sum[0] / hdaf_time / GIGBIN );
      printf("<td> Write_speed %f GiB/S\n", hio_rw_count_sum[1] / hdaf_time / GIGBIN );
    }
  }
}

ACTION_RUN(hdu_run) {
  hio_return_t hrc;
  char * name = V0.s;
  U64 id = V1.u;
  hio_unlink_mode_t ulm = V2.i;
  hrc = hio_dataset_unlink(context, name, id, ulm);
  HRC_TEST(hio_dataset_unlink)
}

ACTION_RUN(hf_run) {
  hio_return_t hrc;
  hrc = hio_fini(&context);
  HRC_TEST(hio_fini);
}

ACTION_RUN(hxrc_run) {
  hio_rc_exp = V0.i;
  VERB3("%s; HIO expected rc now %s(%d)", A.desc, V0.s, V0.i);
}

ACTION_CHECK(hxct_check) {
  I64 count = V0.u;
  if (count < 0 && count != HIO_CNT_ANY && count != HIO_CNT_REQ)
    ERRX("%s; count negative and not %d (ANY) or %d (REQ)", A.desc, HIO_CNT_ANY, HIO_CNT_REQ);
}

ACTION_RUN(hxct_run) {
  hio_cnt_exp = V0.u;
  VERB3("%s; HIO expected count now %lld", A.desc, V0.u);
}

ACTION_RUN(hxdi_run) {
  hio_dsid_exp = V0.u;
  hio_dsid_exp_set = 1;
  VERB3("%s; HIO expected dataset id now %lld", A.desc, hio_dsid_exp);
}

//----------------------------------------------------------------------------
// hvp action handlers
//----------------------------------------------------------------------------
void pr_cfg(hio_object_t object, char * obj_name, struct action * actionp) {
  hio_return_t hrc;
  int count;
  hrc = hio_config_get_count((hio_object_t) object, &count);
  HRC_TEST("hio_config_get_count");
  DBG3("hio_config_get_count %s returns count: %d", obj_name, count);

  for (int i = 0; i< count; i++) {
    char * name;
    hio_config_type_t type;
    bool ro;
    char * value = NULL;

    hrc = hio_config_get_info((hio_object_t) object, i, &name, &type, &ro);
    HRC_TEST("hio_config_get_info");
    if (HIO_SUCCESS == hrc) {
      if (!rx_run(1, actionp, name)) {
        hrc = hio_config_get_value((hio_object_t) object, name, &value);
        HRC_TEST("hio_config_get_value");
        if (HIO_SUCCESS == hrc) {
          VERB1("%s Config (%6s, %s) %30s = %s", obj_name,
                enum_name(MY_MSG_CTX, &etab_hcfg, type), ro ? "RO": "RW", name, value);
        }
        value = FREEX(value);
      }
    }
  }
}

void pr_perf(hio_object_t object, char * obj_name, struct action * actionp) {
  hio_return_t hrc;
  int count;
  hrc = hio_perf_get_count((hio_object_t) object, &count);
  HRC_TEST("hio_perf_get_count");
  DBG3("hio_perf_get_count %s returns count: %d", obj_name, count);
  for (int i = 0; i< count; i++) {
    char * name;
    hio_config_type_t type;

    union {                // Union member names match hio_config_type_t names
      bool BOOL;
      char STRING[512];
      I32 INT32;
      U32 UINT32;
      I64 INT64;
      U64 UINT64;
      float FLOAT;
      double DOUBLE;
    } value;

    hrc = hio_perf_get_info((hio_object_t) object, i, &name, &type);
    HRC_TEST("hio_perf_get_info");
    if (HIO_SUCCESS == hrc) {
      if (!rx_run(1, actionp, name)) {
        hrc = hio_perf_get_value((hio_object_t) object, name, &value, sizeof(value));
        HRC_TEST("hio_perf_get_value");
        if (HIO_SUCCESS == hrc) {
          #define PM2(FMT, VAR)                                         \
            VERB1("%s Perf   (%6s) %30s = " #FMT, obj_name,             \
                  enum_name(MY_MSG_CTX, &etab_hcfg, type), name, VAR);

          switch (type) {
            case HIO_CONFIG_TYPE_BOOL:   PM2(%d,   value.BOOL  ); break;
            case HIO_CONFIG_TYPE_STRING: PM2(%s,   value.STRING); break;
            case HIO_CONFIG_TYPE_INT32:  PM2(%ld,  value.INT32 ); break;
            case HIO_CONFIG_TYPE_UINT32: PM2(%lu,  value.UINT32); break;
            case HIO_CONFIG_TYPE_INT64:  PM2(%lld, value.INT64 ); break;
            case HIO_CONFIG_TYPE_UINT64: PM2(%llu, value.UINT64); break;
            case HIO_CONFIG_TYPE_FLOAT:  PM2(%f,   value.FLOAT) ; break;
            case HIO_CONFIG_TYPE_DOUBLE: PM2(%f,   value.DOUBLE); break;
            default: ERRX("%s: invalid hio_config_type_t: %d", A.desc, type);
          }
        }
      }
    }
  }
}

ACTION_CHECK(hvp_check) {
  rx_comp(0, actionp);
  rx_comp(1, actionp);
}


ACTION_RUN(hvp_run) {
  R0_OR_VERB_START
    if (!rx_run(0, actionp, "cc") && context) pr_cfg((hio_object_t)context, "Context", actionp);
    if (!rx_run(0, actionp, "cd") && dataset) pr_cfg((hio_object_t)dataset, "Dataset", actionp);
    if (!rx_run(0, actionp, "ce") && element) pr_cfg((hio_object_t)element, "Element", actionp);
    if (!rx_run(0, actionp, "pc") && context) pr_perf((hio_object_t)context, "Context", actionp);
    if (!rx_run(0, actionp, "pd") && dataset) pr_perf((hio_object_t)dataset, "Dataset", actionp);
    if (!rx_run(0, actionp, "pe") && element) pr_perf((hio_object_t)element, "Element", actionp);
  R0_OR_VERB_END
}

ACTION_RUN(hvsc_run) {
  hio_return_t hrc;
  if (!context) ERRX("%s: hio context not established", A.desc);
  hrc = hio_config_set_value((hio_object_t) context, V0.s, V1.s);
  HRC_TEST("hio_config_set_value");
}

ACTION_RUN(hvsd_run) {
  hio_return_t hrc;
  if (!dataset) ERRX("%s: hio dataset not open", A.desc);
  hrc = hio_config_set_value((hio_object_t) dataset, V0.s, V1.s);
  HRC_TEST("hio_config_set_value");
}

ACTION_RUN(hvse_run) {
  hio_return_t hrc;
  if (!context) ERRX("%s: hio element not open", A.desc);
  hrc = hio_config_set_value((hio_object_t) element, V0.s, V1.s);
  HRC_TEST("hio_config_set_value");
}

#if HIO_USE_DATAWARP
ENUM_START(etab_dwst)  // DataWarp stage type
ENUM_NAME("IMMEDIATE", DW_STAGE_IMMEDIATE)
ENUM_NAME("JOB_END",   DW_STAGE_AT_JOB_END)
ENUM_NAME("REVOKE",    DW_REVOKE_STAGE_AT_JOB_END)
ENUM_NAME("ACTIVATE",  DW_ACTIVATE_DEFERRED_STAGE) 
ENUM_END(etab_dwst, 0, NULL)

ACTION_RUN(dsdo_run) {
  char * dw_dir = V0.s;
  char * pfs_dir = V1.s;
  enum dw_stage_type type = V2.i;
  int rc = dw_stage_directory_out(dw_dir, pfs_dir, type);
  if (rc || MY_MSG_CTX->verbose_level >= 3) {                                          
    MSG("dw_stage_directory_out(%s, %s, %s) rc: %d", dw_dir, pfs_dir, enum_name(MY_MSG_CTX, &etab_dwst, type), rc);
  }
}

ACTION_RUN(dwds_run) {
  ETIMER tmr;
  ETIMER_START(&tmr);
  int rc = dw_wait_directory_stage(V0.s);
  if (rc) local_fails++;
  VERB1("dw_wait_directory_stage(%s) rc: %d (%s)  time: %f Sec", V0.s, rc, strerror(abs(rc)), ETIMER_ELAPSED(&tmr));
}


#if DW_PH_2
ACTION_RUN(dwws_run) {
  char * filename = V0.s;
  ETIMER tmr;
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    MSG("dwws: open(%s) fails with errno:%d (%s)", filename, errno, strerror(errno));
    local_fails++;
  } else {
    ETIMER_START(&tmr);
    int rc = dw_wait_sync_complete(fd);
    if (rc != 0) {
      MSG("dwws dw_wait_sync_complete(%s) fails with rc: %d (%s)", filename, rc, strerror(abs(rc)));
      local_fails++;
    }
    VERB1("dw_wait_sync_complete(%s) rc: %d (%s)  time: %f Sec", filename, rc, strerror(abs(rc)), ETIMER_ELAPSED(&tmr));
    rc = close(fd);
    if (rc != 0) {
      MSG("dwws: close(%s) fails with errno:%d (%s)", filename, errno, strerror(errno));
      local_fails++;
    }
  }
}
#endif // DW_PH_2
#endif // HIO_USE_DATAWARP

#endif // HIO

// Special action runner for printing out /@@ comments
ACTION_RUN(cmsg_run) {
  R0_OR_VERB_START
    VERB1("%s", V0.s);
  R0_OR_VERB_END
}

//----------------------------------------------------------------------------
// k, x (signal, exit) action handlers
//----------------------------------------------------------------------------
ACTION_RUN(raise_run) {
  VERB0("Raising signal %d", V0.u);
  raise(V0.u);
}

ACTION_RUN(exit_run) {
  VERB0("Exiting with status %d", V0.u);
  exit(V0.u);
}

ACTION_RUN(segv_run) {
  VERB0("Forcing SIGSEGV");
  * (volatile char *) 0 = '\0';
}

//----------------------------------------------------------------------------
// grep action handler
//----------------------------------------------------------------------------

ACTION_CHECK(grep_check) {
  rx_comp(0, actionp);
}

extern char * * environ;

ACTION_RUN(grep_run) {
  char * fname = V1.s;

  if (0 == strcmp(fname, "@ENV")) {
    for (char ** eptr = environ; *eptr; eptr++) {
      if (!rx_run(0, actionp, *eptr)) {
        VERB1("grep: %s", *eptr);
      }
    }
  } else {
    char line[512];
    FILE * f = fopen(fname, "r");
    if (!f) ERRX("%s: error opening \"%s\" %s", A.desc, fname, strerror(errno));
    while (fgets(line, sizeof(line), f)) {
      if (!rx_run(0, actionp, line)) {
        char * last = line + strlen(line) - 1;
        if ('\n' == *last) *last = '\0';
        VERB1("grep: %s", line);
      }
    }
    fclose(f);
  }
}

//----------------------------------------------------------------------------
// Argument string parsing table
//----------------------------------------------------------------------------
enum ptype {
  SINT = CVT_SINT,
  UINT = CVT_NNINT,
  PINT = CVT_PINT,
  DOUB = CVT_DOUB,
  STR, DBUF, HFLG, HDSM, HERR, HULM, HDSI, DWST, ONFF, NONE };

struct parse {
  char * cmd;
  enum ptype param[MAX_PARAM];
  action_check * checker;
  action_run * runner;
} parse[] = {
// Command  V0    V1    V2    V3    V4    Check          Run
  {"ztest", {SINT, DOUB, STR,  STR,  NONE}, NULL,          ztest_run   },   // For testing code fragments  
  {"v",     {UINT, NONE, NONE, NONE, NONE}, verbose_check, verbose_run },
  {"d",     {UINT, NONE, NONE, NONE, NONE}, debug_check,   debug_run   },
  {"opt",   {STR,  NONE, NONE, NONE, NONE}, opt_check,     opt_run     },
  {"name",  {STR,  NONE, NONE, NONE, NONE}, NULL,          name_run    },
  {"im",    {STR,  NONE, NONE, NONE, NONE}, imbed_check,   NULL        },
  {"srr",   {SINT, NONE, NONE, NONE, NONE}, NULL,          srr_run     },
  {"lc",    {UINT, NONE, NONE, NONE, NONE}, loop_check,    lc_run      },
  {"lcr",   {UINT, UINT, NONE, NONE, NONE}, loop_check,    lcr_run     },
  {"lt",    {DOUB, NONE, NONE, NONE, NONE}, loop_check,    lt_run      },
  {"ifr",   {SINT, NONE, NONE, NONE, NONE}, ifr_check,     ifr_run     },
  {"eif",   {NONE, NONE, NONE, NONE, NONE}, eif_check,     eif_run     },
  #ifdef MPI
  {"ls",    {DOUB, NONE, NONE, NONE, NONE}, loop_check,    ls_run      },
  #endif
  {"le",    {NONE, NONE, NONE, NONE, NONE}, loop_check,    le_run      },
  {"dbuf",  {DBUF, UINT, NONE, NONE, NONE}, dbuf_check,    dbuf_run    },
  {"o",     {UINT, NONE, NONE, NONE, NONE}, NULL,          stdout_run  },
  {"e",     {UINT, NONE, NONE, NONE, NONE}, NULL,          stderr_run  },
  {"s",     {DOUB, NONE, NONE, NONE, NONE}, sleep_check,   sleep_run   },
  {"va",    {UINT, NONE, NONE, NONE, NONE}, va_check,      va_run      },
  {"vt",    {PINT, NONE, NONE, NONE, NONE}, vt_check,      vt_run      },
  {"vf",    {NONE, NONE, NONE, NONE, NONE}, vf_check,      vf_run      },
  {"imd",   {STR,  SINT, NONE, NONE, NONE}, NULL,          imd_run     },

  #ifdef __linux__
  {"dca",   {NONE, NONE, NONE, NONE, NONE}, NULL,          dca_run     },
  #endif
  #ifdef MPI
  {"mi",    {UINT, NONE, NONE, NONE, NONE}, NULL,          mi_run      },
  {"msr",   {PINT, PINT, NONE, NONE, NONE}, NULL,          msr_run     },
  {"mb",    {NONE, NONE, NONE, NONE, NONE}, NULL,          mb_run      },
  {"mb",    {NONE, NONE, NONE, NONE, NONE}, NULL,          mb_run      },
  {"mgf",   {NONE, NONE, NONE, NONE, NONE}, NULL,          mgf_run     },
  {"mf",    {NONE, NONE, NONE, NONE, NONE}, NULL,          mf_run      },
  {"fctw",  {STR,  UINT, UINT, UINT, NONE}, fct_check,     fctw_run    },
  {"fctr",  {STR,  UINT, UINT, UINT, NONE}, fct_check,     fctr_run    },
  {"fget",  {STR,  NONE, NONE, NONE, NONE}, NULL,          fget_run    },
  {"fo",    {STR,  STR,  NONE, NONE, NONE}, fo_check,      fo_run      },
  {"fw",    {SINT, UINT, NONE, NONE, NONE}, fw_check,      fw_run      },
  {"fr",    {SINT, UINT, NONE, NONE, NONE}, fr_check,      fr_run      },
  {"fc",    {NONE, NONE, NONE, NONE, NONE}, fc_check,      fc_run      },
  #endif
  {"ni",    {UINT, PINT, NONE, NONE, NONE}, ni_check,      ni_run      },
  {"nr",    {PINT, PINT, NONE, NONE, NONE}, nr_check,      nr_run      },
  {"nf",    {NONE, NONE, NONE, NONE, NONE}, nf_check,      nf_run      },
  {"hx",    {UINT, UINT, UINT, UINT, UINT}, hx_check,      hx_run      },
  #ifdef DLFCN
  {"dlo",   {STR,  NONE, NONE, NONE, NONE}, dlo_check,     dlo_run     },
  {"dls",   {STR,  NONE, NONE, NONE, NONE}, dls_check,     dls_run     },
  {"dlc",   {NONE, NONE, NONE, NONE, NONE}, dlc_check,     dlc_run     },
  #endif
  #ifdef HIO
  {"hi",    {STR,  STR,  NONE, NONE, NONE}, NULL,          hi_run      },
  {"hda",   {STR,  HDSI, HFLG, HDSM, NONE}, NULL,          hda_run     },
  {"hdo",   {NONE, NONE, NONE, NONE, NONE}, NULL,          hdo_run     },
  {"heo",   {STR,  HFLG, NONE, NONE, NONE}, heo_check,     heo_run     },
  {"hso",   {UINT, NONE, NONE, NONE, NONE}, NULL,          hso_run     },
  {"hsega", {SINT, SINT, SINT, NONE, NONE}, NULL,          hsega_run   },
  {"hsegr", {SINT, SINT, SINT, NONE, NONE}, NULL,          hsegr_run   },
  {"hew",   {SINT, UINT, NONE, NONE, NONE}, hew_check,     hew_run     },
  {"her",   {SINT, UINT, NONE, NONE, NONE}, her_check,     her_run     },
  {"hewr",  {SINT, UINT, UINT, UINT, NONE}, hew_check,     hewr_run    },
  {"herr",  {SINT, UINT, UINT, UINT, NONE}, her_check,     herr_run    },
  {"hec",   {NONE, NONE, NONE, NONE, NONE}, NULL,          hec_run     },
  {"hdc",   {NONE, NONE, NONE, NONE, NONE}, NULL,          hdc_run     },
  {"hdf",   {NONE, NONE, NONE, NONE, NONE}, NULL,          hdf_run     },
  {"hdu",   {STR,  UINT, HULM, NONE, NONE}, NULL,          hdu_run     },
  {"hf",    {NONE, NONE, NONE, NONE, NONE}, NULL,          hf_run      },
  {"hxrc",  {HERR, NONE, NONE, NONE, NONE}, NULL,          hxrc_run    },
  {"hxct",  {SINT, NONE, NONE, NONE, NONE}, hxct_check,    hxct_run    },
  {"hxdi",  {HDSI, NONE, NONE, NONE, NONE}, NULL,          hxdi_run    },
  {"hvp",   {STR,  STR,  NONE, NONE, NONE}, hvp_check,     hvp_run     },
  {"hvsc",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvsc_run    },
  {"hvsd",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvsd_run    },
  {"hvse",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvse_run    },
  #if HIO_USE_DATAWARP
  {"dsdo",  {STR,  STR,  DWST, NONE, NONE}, NULL,          dsdo_run    },
  {"dwds",  {STR,  NONE, NONE, NONE, NONE}, NULL,          dwds_run    },
  #if DW_PH_2
  {"dwws",  {STR,  NONE, NONE, NONE, NONE}, NULL,          dwws_run    },
  #endif  // DW_PH_2
  #endif  // HIO_USE_DATAWARP
  #endif  // HIO
  {"k",     {UINT, NONE, NONE, NONE, NONE}, NULL,          raise_run   },
  {"x",     {UINT, NONE, NONE, NONE, NONE}, NULL,          exit_run    },
  {"grep",  {STR,  STR,  NONE, NONE, NONE}, grep_check,    grep_run    },
  {"segv",  {NONE, NONE, NONE, NONE, NONE}, NULL,          segv_run    },
};

//----------------------------------------------------------------------------
// Argument string parser - call check routines, build action vector
//----------------------------------------------------------------------------
void decode(ENUM_TABLE * etptr, char * tok, char * name, char * desc, PVAL * val) {
  int rc = str2enum(MY_MSG_CTX, etptr, tok, &val->i);
  if (rc) ERRX("%s ...; invalid %s \"%s\". Valid values are %s",
               desc, name, tok, enum_list(MY_MSG_CTX, etptr));
  rc = enum2str(MY_MSG_CTX, etptr, val->i, &val->s);
  if (rc) ERRX("%s ...; invalid %s \"%s\"", desc, name, tok);
}

void decode_int(ENUM_TABLE * etptr, char * tok, char * name, char * desc, PVAL * val, ACTION * actionp) {
  int i, rc;
  char buf[128];

  // Try enum
  rc = str2enum(MY_MSG_CTX, etptr, tok, &i);

  if (rc == 0) {
    val->u = (U64)i;
  } else {
    // Not valid enum, try integer
    rc = cvt_num(CVT_SINT, tok, &(val->u), buf, sizeof(buf));
    // Neither, issue error message
    if (rc) ERRX("%s and not a valid %s, recognized values are %s",
                 buf, name, enum_list(MY_MSG_CTX, etptr));
  }
}

void parse_action() {
  int t = -1, i, j, rc;
  ACTION nact;
  char buf[128];

  msg_context_set_verbose(MY_MSG_CTX, 0);
  msg_context_set_debug(MY_MSG_CTX, 0);

  #ifdef DLFCN
    dl_num = -1;
  #endif

  int comment_depth=0;
  char * comment_msg = NULL;

  while ( ++t < tokc ) {
    if (0 == strcmp(tokv[t], "/@")) {
      comment_depth++;
      DBG3("comment start: tokv[%d]: %s depth: %d", t, tokv[t], comment_depth);
    } else if (0 == strcmp(tokv[t], "/@@")) {
      comment_depth++;
      comment_msg = STRDUPX("***");
      DBG3("comment start: tokv[%d]: %s depth: %d", t, tokv[t], comment_depth);
    } else if (0 == strcmp(tokv[t], "@/")) {
      comment_depth = MAX(0, comment_depth - 1);
      DBG3("comment end: tokv[%d]: %s depth: %d", t, tokv[t], comment_depth);
      if (comment_msg) {
        nact.tokn = t;
        nact.actn = actc;
        nact.action = tokv[t];
        nact.desc = ALLOC_PRINTF("action %d: /@@ %s", actc+1, comment_msg);
        nact.runner = cmsg_run;
        nact.v[0].s = comment_msg;
        add2actv(&nact);
        comment_msg = NULL;
      }
    } else if (comment_depth > 0) {
      if (comment_msg) comment_msg = STRCATRX(STRCATRX(comment_msg, " "), tokv[t]);
      DBG3("Token in comment skipped: tokv[%d]: %s depth: %d", t, tokv[t], comment_depth);
    } else {
      for (i = 0; i < DIM1(parse); ++i) { // loop over parse table
        if (0 == strcmp(tokv[t], parse[i].cmd)) {
          DBG3("match: tokv[%d]: %s parse[%d].cmd: %s", t, tokv[t], i, parse[i].cmd);
          nact.tokn = t;
          nact.actn = actc;
          nact.action = tokv[t];
          nact.desc = ALLOC_PRINTF("action %d: %s", actc+1, tokv[t]);

          for (j = 0; j < MAX_PARAM; ++j) { // loop over params
            if (parse[i].param[j] == NONE) break; // for j loop over params
            t++;
            if (t >= tokc) ERRX("action %d \"%s\" missing param %d", nact.tokn, nact.action, j+1);
            nact.desc = STRCATRX(STRCATRX(nact.desc, " "), tokv[t]);
            DBG5("%s ...; parse[%d].param[%d]: %d", nact.desc, i, j, parse[i].param[j]);
            switch (parse[i].param[j]) {
              case SINT:
              case UINT:
              case PINT:
                rc = cvt_num((enum cvt_num_type)parse[i].param[j], tokv[t], &nact.v[j].u, buf, sizeof(buf));
                if (rc) ERRX("%s ...; %s", nact.desc, buf);
                break;
              case DOUB:
                rc = cvt_num((enum cvt_num_type)parse[i].param[j], tokv[t], &nact.v[j].d, buf, sizeof(buf));
                if (rc) ERRX("%s ...; %s", nact.desc, buf);
                break;
              case STR:
                nact.v[j].s = tokv[t];
                break;
              case DBUF:
                decode(&etab_dbuf, tokv[t], "Data buffer pattern type", nact.desc, &nact.v[j]);
                break;
              #ifdef HIO
              case HFLG:
                decode(&etab_hflg, tokv[t], "hio flag", nact.desc, &nact.v[j]);
                break;
              case HDSM:
                decode(&etab_hdsm, tokv[t], "hio mode", nact.desc, &nact.v[j]);
                break;
              case HERR:
                decode(&etab_herr, tokv[t], "hio return", nact.desc, &nact.v[j]);
                break;
              case HULM:
                decode(&etab_hulm, tokv[t], "hio unlink mode", nact.desc, &nact.v[j]);
                break;
              case HDSI:
                decode_int(&etab_hdsi, tokv[t], "hio dataset ID", nact.desc, &nact.v[j], &nact);
                break;
              #if HIO_USE_DATAWARP
              case DWST:
                decode(&etab_dwst, tokv[t], "DataWarp stage type", nact.desc, &nact.v[j]);
                break;
              #endif
              #endif
              case ONFF:
                decode(&etab_onff, tokv[t], "ON / OFF value", nact.desc, &nact.v[j]);
                break;
              default:
                ERRX("%s ...; internal parse error parse[%d].param[%d]: %d", nact.desc, i, j, parse[i].param[j]);
            }
          }
          nact.runner = parse[i].runner;
          add2actv(&nact);
          DBG2("Checking %s action.actn: %d", nact.desc, nact.actn);
          if (parse[i].checker) parse[i].checker(&actv[actc-1], t);
          break; // break for i loop over parse table
        }
      }
      if (i >= DIM1(parse)) ERRX("action %d: \"%s\" not recognized.", t, tokv[t]);
    }
  }
  if (lcur-lctl > 0) ERRX("Unterminated loop - more loop starts than loop ends");
  if (ifr_depth != 0) ERRX("Unterminated ifr - ifr without eif");
  if (comment_depth > 0) ERRX("Unterminated comment - more comment starts than comment ends");
  IFDBG4( for (int a=0; a<actc; a++) DBG0("actv[%d].desc: %s", a, actv[a].desc) );
  DBG1("Parse complete actc: %d", actc);
}

//----------------------------------------------------------------------------
// Action runner - call run routines for action vector entries
//----------------------------------------------------------------------------
void run_action() {
  int a = -1;

  msg_context_set_verbose(MY_MSG_CTX, 1);
  msg_context_set_debug(MY_MSG_CTX, 0);

  #ifdef DLFCN
    dl_num = -1;
  #endif

  while ( ++a < actc ) {
    VERB2("--- Running %s", actv[a].desc);
    // Runner routine may modify variable a for looping or conditional
    int old_a = a;
    errno = 0;
    if (actv[a].runner) actv[a].runner(&actv[a], &a);
    DBG3("Done %s; fails: %d ROF: %d actn: %d", actv[old_a].desc, local_fails, (options & OPT_ROF) ? 1: 0, a);
    if (local_fails > 0 && !(options & OPT_ROF)) {
      VERB0("Quiting due to fails: %d and ROF not set", local_fails);
      break;
    }
  }
  VERB2("Action execution ended, Fails: %d", local_fails);

}

//----------------------------------------------------------------------------
// Signal handler - init and run
//----------------------------------------------------------------------------

void xexec_signal_handler(int signum, siginfo_t *siginfo  , void * ptr) {
  // Verbose name for this routine to better show up on backtrace
  MSGE("xexec received signal: %d (%s) from pid: %d", signum, sys_siglist[signum], siginfo->si_pid);

  #ifdef __linux__
    psiginfo(siginfo, "xexec received signal:");
  #endif

  // Print backtrace on signals potentially caused by program error
  if ( SIGILL  == signum ||
       SIGABRT == signum ||
       SIGFPE  == signum ||
       SIGBUS  == signum ||
       SIGSEGV == signum ) {
    void * buf[128];
    int n = backtrace(buf, DIM1(buf));
    backtrace_symbols_fd(buf, n, STDERR_FILENO);
    fflush(stderr);
  }

}

void sig_add(int signum) {
  struct sigaction sa;
   
  sa.sa_sigaction = xexec_signal_handler;
  // Provide siginfo structure and reset signal handler to default
  sa.sa_flags = SA_SIGINFO | SA_RESETHAND;
  sigaction(signum, &sa, NULL);
}

void sig_init(void) {
  // Intercept every known signal
  sig_add(SIGHUP);
  sig_add(SIGINT);
  sig_add(SIGQUIT);
  sig_add(SIGILL);
  sig_add(SIGABRT);
  sig_add(SIGKILL);
  sig_add(SIGBUS);
  sig_add(SIGFPE);
  sig_add(SIGSEGV);
  sig_add(SIGSYS);
  sig_add(SIGPIPE);
  sig_add(SIGALRM);
  sig_add(SIGTERM);
  sig_add(SIGSTOP);
  sig_add(SIGTSTP);
  sig_add(SIGCONT);
  sig_add(SIGCHLD);
  sig_add(SIGTTIN);
  sig_add(SIGTTOU);
  sig_add(SIGIO);
  sig_add(SIGXCPU);
  sig_add(SIGXFSZ);
  sig_add(SIGVTALRM);
  sig_add(SIGPROF);
  sig_add(SIGWINCH);
  sig_add(SIGUSR1);
  sig_add(SIGUSR2);
}

//----------------------------------------------------------------------------
// Main - write help, call parser / dispatcher
//----------------------------------------------------------------------------
int main(int argc, char * * argv) {


  msg_context_init(MY_MSG_CTX, 0, 0);
  get_id();

  if (argc <= 1 || 0 == strncmp("-h", argv[1], 2)) {
    fprintf(stdout, help, enum_list(MY_MSG_CTX, &etab_opt),
            options_init, cvt_num_suffix());
    return 1;
  }

  sig_init();
  add2tokv(argc-1, argv+1); // Make initial copy of argv so im works

  // Parse init and env option values
  int set, clear, newopt, rc;
  char * xexec_opt = getenv("XEXEC_OPT");

  parse_opt("internal options_init", options_init, &set, &clear);
  newopt = clear & set;

  if (xexec_opt) { 
    parse_opt("XEXEC_OPT env var", xexec_opt, &set, &clear);
    newopt = clear & (set | newopt);
  }   
 
  // Make two passes through args, first to check, second to run.
  options = newopt;
  parse_action();

  options = newopt;
  run_action();

  // Suppress SUCCESS result message from all but rank 0
  // Suppress  if gather_fails, not rank 0 and local fails = 0
  if (gather_fails && myrank != 0 && local_fails == 0) {
    // do nothing
  } else {
    if (local_fails + global_fails == 0) { 

      if (options & OPT_SMSGV1) { // SMSGV1 option allows really quiet output on V0
        VERB1("xexec done.  Result: %s  Fails: %d  Test name: %s",
              "SUCCESS", local_fails + global_fails, test_name);
      } else {
        VERB0("xexec done.  Result: %s  Fails: %d  Test name: %s",
              "SUCCESS", local_fails + global_fails, test_name);
      }

    } else { // But failure always prints message
      VERB0("xexec done.  Result: %s  Fails: %d  Test name: %s",
            "FAILURE", local_fails + global_fails, test_name);
    }

    // Pavilion message
    if (options & OPT_PAVM) {
      printf("<result> %s <<< xexec done.  Test name: %s  Fails: %d >>>\n",
             (local_fails + global_fails) ? "fail" : "pass",
             test_name, local_fails + global_fails);
    }
  }

  rc = (local_fails + global_fails) ? EXIT_FAILURE : EXIT_SUCCESS;

  #ifdef MPI
  if (rc != EXIT_SUCCESS && mpi_active()) {
    VERB0("MPI_abort due to error exit from main with MPI active rc: %d", rc);
    MPI_CK(MPI_Abort(mpi_comm, rc));
  }
  #endif // MPI

  return rc;
}
// --- end of xexec.c ---
