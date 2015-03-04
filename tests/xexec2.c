#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#ifdef DLFCN
#include <dlfcn.h>
#endif
#ifdef MPI
#include <mpi.h>
#endif
#ifdef HIO
#include "hio.h"
#endif
//----------------------------------------------------------------------------
// DBGMAXLEV controls which debug messages are compiled into the program.
// Set via compile option -DDBGMAXLEV=<n>, where <n> is 0 through 5.
// DBGMAXLEV is used by cw_misc.h to control the expansion of DBGx macros.
// Even when compiled in, debug messages are only issued if the current
// debug message level is set via the "d <n>" action to the level of the
// individual debug message.  Compiling in all debug messages via -DDBGLEV=5
// will noticeably impact the performance of high speed loops such as
// "vt" or "fr" due to the time required to repeatedly test the debug level.
// You have been warned !
//----------------------------------------------------------------------------
#ifndef DBGMAXLEV
  #define DBGMAXLEV 4
#endif
#include "cw_misc.h"

//----------------------------------------------------------------------------
// To build:    cc -O3       xexec.c -o xexec
//       or: mpicc -O3 -DMPI xexec.c -o xexec
//  on Cray:    cc -O3 -DMPI xexec.c -o xexec -dynamic
//
// Optionally add: -DDBGLEV=3 (see discussion below)
//                 -DMPI      to enable MPI functions
//                 -DDLFCN    to enable dlopen and related calls
//----------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Features to add: mpi sr swap, file write & read,
// flap seq validation, more flexible fr striding
//----------------------------------------------------------------------------

char * help =
  "xexec - universal testing executable.  Processes command line arguments\n"
  "        in sequence to control actions.\n"
  "        Version 0.9.3 " __DATE__ " " __TIME__ "\n"
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
  "  im <file>     imbed a file of actions at this point, - means stdin\n"
  "  lc <count>    loop start; repeat the following actions (up to the matching\n"
  "                loop end) <count> times\n"
  "  lt <seconds>  loop start; repeat the following actions (up to the matching\n"
  "                loop end) for at least <seconds>\n"
  #ifdef MPI
  "  ls <seconds>  like lt, but synchronized via MPI_Bcast from rank 0\n"
  #endif
  "  le            loop end; loops may be nested up to 16 deep\n"
  "  o <count>     write <count> lines to stdout\n"
  "  e <count>     write <count> lines to stderr\n"
  "  s <seconds>   sleep for <seconds>\n"
  "  va <bytes>    malloc <bytes> of memory\n"
  "  vt <stride>   touch most recently allocated memory every <stride> bytes\n"
  "  vf            free most recently allocated memory\n"
  #ifdef MPI
  "  mi            issue MPI_Init()\n"
  "  msr <size> <stride>\n"
  "                issue MPI_Sendreceive with specified buffer <size> to and\n"
  "                from ranks <stride> above and below this rank\n"
  "  mb            issue MPI_Barrier()\n"
  "  mf            issue MPI_Finalize()\n"
  #endif
  "  fi <size> <count>\n"
  "                Creates <count> blocks of <size> doubles each.  All\n"
  "                but one double in each block is populated with sequential\n"
  "                values starting with 1.0.\n"
  "  fr <rep> <stride>\n"
  "                The values in each block are added and written to the\n"
  "                remaining double in the block. The summing of the block is\n"
  "                repeated <rep> times.  All <count> blocks are processed in\n"
  "                sequence offset by <stride>. The sum of all blocks is\n"
  "                computed and compared with an expected value.\n"
  "                <size> must be 2 or greater, <count> must be 1 greater than\n"
  "                a multiple of <stride>.\n"
  "  ff            Free allocated blocks\n"
  "  hx <min> <max> <blocks> <limit> <count>\n"
  "                Perform <count> malloc/touch/free cycles on memory blocks ranging\n"
  "                in size from <min> to <max>.  Alocate no more than <limit> bytes\n"
  "                in <blocks> separate allocations.  Sequence and sizes of\n"
  "                allocations are randomized.\n"
  #ifdef DLFCN
  "  dlo <name>    Issue dlopen for specified file name\n"
  "  dls <symbol>  Issue dlsym for specified symbol in most recently opened library\n"
  "  dlc           Issue dlclose for most recently opened library\n"
  #endif
  #ifdef HIO
  "  hi  <name> <data_root>  Init hio context\n"
  "  hdo <name> <id> <flags> <mode> Dataset open\n"
  "  heo <name> <flags> Element open\n"
  "  hew <offset> <size> Element write - if offset negative, auto increment\n"
  "  her <offset> <size> Element read - if offset negative, auto increment\n"
  "  hec <name>    Element close\n"
  "  hdc           Dataset close\n"
  "  hf            Fini\n"
  "  hck <ON|OFF>  Enable read data checking\n"
  "  hxrc <rc_name|ANY> Expect non-SUCCESS rc on next HIO action\n"
  #endif
  "  k <signal>    raise <signal> (number)\n"
  "  x <status>    exit with <status>\n"
  "\n"
  "  Numbers can be specified with suffixes k, ki, M, Mi, G, Gi, etc.\n"
  "\n"
  "  Example action sequences:\n"
  "    v 1 d 1\n"
  "    lc 3 s 0 le\n"
  "    lt 3 s 1 le\n"
  "    o 3 e 2\n"
  "    va 1M vt 4K vf\n"
  #ifdef MPI
  "    mi mb mf\n"
  #endif
  "    fi 32 1M fr 8 1 ff\n"
  "    x 99\n"
  "\n"
  #ifndef MPI
  " MPI actions can be enabled by building with -DMPI.  See comments in source.\n"
  "\n"
  #endif
  #ifndef DLFCN
  " dlopen and related actions can be enabled by building with -DDLFCN.  See comments in source.\n"
  "\n"
  #endif
;

//----------------------------------------------------------------------------
// Global variables
//----------------------------------------------------------------------------
char id_string[256] = "";
int id_string_len = 0;
int quit_on_fail = 1; // Quit after this many or more fails (0 = nevr quit)
int fail_count = 0;   // Count of fails
#ifdef MPI
int myrank, mpi_size = 0;
#endif
int tokc = 0;
char * * tokv = NULL;

typedef struct pval {
  U64 u;
  char * s;
  int i;  // Use for enums
  double d; 
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
#endif

// Serialize execution of all MPI ranks
#ifdef MPI
  #define RANK_SERIALIZE_START                           \
    MPI_CK(MPI_Barrier(MPI_COMM_WORLD));                 \
    if (mpi_size > 0 && myrank != 0) {                   \
      char buf;                                          \
      MPI_Status status;                                 \
      MPI_CK(MPI_Recv(&buf, 1, MPI_CHAR, myrank - 1,     \
             MPI_ANY_TAG, MPI_COMM_WORLD, &status));     \
    }
  #define RANK_SERIALIZE_END                             \
    if (mpi_size > 0 && myrank != mpi_size - 1) {        \
      char buf;                                          \
      MPI_CK(MPI_Send(&buf, 1, MPI_CHAR, myrank + 1,     \
             0, MPI_COMM_WORLD));                        \
    }
#else
  #define RANK_SERIALIZE_START
  #define RANK_SERIALIZE_END
#endif

void get_id() {
  char * p;
  char tmp_id[sizeof(id_string)];
  int rc;

  rc = gethostname(tmp_id, sizeof(tmp_id));
  if (rc != 0) ERRX("gethostname rc: %d %s", rc, strerror(errno));
  p = strchr(tmp_id, '.');
  if (p) *p = '\0';

  # ifdef MPI
  mpi_size = 0;
  { int mpi_init_flag, mpi_final_flag;
    MPI_Initialized(&mpi_init_flag);
    if (mpi_init_flag) {
      MPI_Finalized(&mpi_final_flag);
      if (! mpi_final_flag) {
        MPI_CK(MPI_Comm_rank(MPI_COMM_WORLD, &myrank));
        sprintf(tmp_id+strlen(tmp_id), ".%d", myrank);
        MPI_CK(MPI_Comm_size(MPI_COMM_WORLD, &mpi_size));
        sprintf(tmp_id+strlen(tmp_id), "/%d", mpi_size);
      }
    }
  }
  #endif
  strcat(tmp_id, " ");
  strcpy(id_string, tmp_id);
  id_string_len = strlen(id_string);
  MY_MSG_CTX->id_string=id_string;
}

enum ptype { SINT, UINT, PINT, DOUB, STR, HFLG, HDSM, HERR, ONFF, NONE };

U64 getI64(char * num, enum ptype type, ACTION *actionp) {
  long long int n;
  char * endptr;
  DBG3("getI64 num: %s", num);
  errno = 0;
  n = strtoll(num, &endptr, 0);
  if (errno != 0) ERRX("%s ...; invalid integer \"%s\"", A.desc, num);
  if (type == UINT && n < 0) ERRX("%s ...; negative integer \"%s\"", A.desc, num);
  if (type == PINT && n <= 0) ERRX("%s ...; non-positive integer \"%s\"", A.desc, num);
  if (*endptr == '\0');
  else if (!strcmp("k",  endptr)) n *= 1000;
  else if (!strcmp("ki", endptr)) n *= 1024;
  else if (!strcmp("M",  endptr)) n *= (1000 * 1000);
  else if (!strcmp("Mi", endptr)) n *= (1024 * 1024);
  else if (!strcmp("G",  endptr)) n *= (1000 * 1000 * 1000);
  else if (!strcmp("Gi", endptr)) n *= (1024 * 1024 * 1024);
  else if (!strcmp("T",  endptr)) n *= (1ll * 1000 * 1000 * 1000 * 1000);
  else if (!strcmp("Ti", endptr)) n *= (1ll * 1024 * 1024 * 1024 * 1024);
  else if (!strcmp("P",  endptr)) n *= (1ll * 1000 * 1000 * 1000 * 1000 * 1000);
  else if (!strcmp("Pi", endptr)) n *= (1ll * 1024 * 1024 * 1024 * 1024 * 1024);
  else ERRX("%s ...; invalid integer \"%s\"", A.desc, num);
  return n;
}

double getDoub(char * num, enum ptype type, ACTION *actionp) {
  double n;
  char * endptr;
  DBG3("getDoub num: %s", num);
  errno = 0;
  n = strtod(num, &endptr);
  if (errno != 0) ERRX("%s ...; invalid double \"%s\"", A.desc, num);
  if (*endptr == '\0');
  else if (!strcmp("k",  endptr)) n *= 1000;
  else if (!strcmp("ki", endptr)) n *= 1024;
  else if (!strcmp("M",  endptr)) n *= (1000 * 1000);
  else if (!strcmp("Mi", endptr)) n *= (1024 * 1024);
  else if (!strcmp("G",  endptr)) n *= (1000 * 1000 * 1000);
  else if (!strcmp("Gi", endptr)) n *= (1024 * 1024 * 1024);
  else if (!strcmp("T",  endptr)) n *= (1ll * 1000 * 1000 * 1000 * 1000);
  else if (!strcmp("Ti", endptr)) n *= (1ll * 1024 * 1024 * 1024 * 1024);
  else if (!strcmp("P",  endptr)) n *= (1ll * 1000 * 1000 * 1000 * 1000 * 1000);
  else if (!strcmp("Pi", endptr)) n *= (1ll * 1024 * 1024 * 1024 * 1024 * 1024);
  else ERRX("%s ...; invalid double \"%s\"", A.desc, num);
  return n;
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
  // Use a very simp PRNG to initialize lfsr_state
  int prime = 15485863; // The 1 millionth prime
  lfsr_state[0] = 0xA5;
  for (int i = 1; i<sizeof(lfsr_state); ++i) {
    lfsr_state[i] = (lfsr_state[i-1] * prime) % 256;
  } 

  // Cycle a few times to mix things up
  unsigned char t[1000];
  lfsr_22_byte(t, sizeof(t)); 
}

void lfsr_test(void) {
  // A few tests for lfsr properties
  U64 size = 8 * 1024 * 1024;
  unsigned char * buf = MALLOCX(size);

  lfsr_22_byte_init();

  printf("lfsr_state:\n");
  hex_dump(lfsr_state, sizeof(lfsr_state));

  lfsr_22_byte(buf, size);

  printf("buf:\n");
  hex_dump(buf, 64);

  printf("buf + %d:\n", LFSR_22_CYCLE);
  hex_dump(buf+LFSR_22_CYCLE, 64);

  #if 0
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
  #endif
}


//----------------------------------------------------------------------------
// Action handler routines
//----------------------------------------------------------------------------
//----------------------------------------------------------------------------
// v, d (verbose, debug) action handlers
//----------------------------------------------------------------------------
ACTION_CHECK(verbose_check) {
  int verbose = V0.u;
  if (verbose > MAX_VERBOSE) ERRX("Verbosity level %d > maximum %d", verbose, MAX_VERBOSE);
}

ACTION_RUN(verbose_run) {
  int verbose = V0.u;
  msg_context_set_verbose(MY_MSG_CTX, verbose);
  VERB0("Verbosity level set to %d", verbose);
}

ACTION_CHECK(debug_check) {
  int debug = V0.u;
  if (debug > DBGMAXLEV) ERRX("requested debug level %d > maximum %d."
                              " Rebuild with -DDBGMAXLEV=<n> to increase"
                              " (see comments in source.)", debug, DBGMAXLEV);
  if (A.actn == 0) {
    msg_context_set_debug(MY_MSG_CTX, debug);
    VERB0("Parse debug level set to %d", debug);
  }
}

ACTION_RUN(debug_run) {
  int debug = V0.u;
  if (A.actn != 0) {
    msg_context_set_debug(MY_MSG_CTX, debug);
    VERB0("Run debug level set to %d", debug);
  }
}

//----------------------------------------------------------------------------
// qof (quit on fail) action handlers
//----------------------------------------------------------------------------
ACTION_RUN(qof_run) {
  quit_on_fail = V0.u;
  VERB0("Quit on fail count set to %d", quit_on_fail);
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
    if (!file) ERRX("unable to open file %s: %s", fn, strerror(errno));
    #define BUFSZ 1024*1024
    void * p = MALLOCX(BUFSZ);
    size_t size;
    size = fread(p, 1, BUFSZ, file);
    DBG4("fread %s returns %d", fn, size);
    if (ferror(file)) ERRX("error reading file %s %d %s", fn, ferror(file), strerror(ferror(file)));
    if (!feof(file)) ERRX("imbed file %s larger than buffer (%d bytes)", fn, BUFSZ);
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
    free(old_tokv);
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
  if ( !strcmp(A.action, "lc") || 
    #ifdef MPI
       !strcmp(A.action, "ls") ||
    #endif
       !strcmp(A.action, "lt") ) {
    if (++lcur - lctl >= MAX_LOOP) ERRX("Maximum nested loop depth of %d exceeded", MAX_LOOP);
  } else if (!strcmp(A.action, "le")) {
    if (lcur <= lctl) ERRX("loop end when no loop active - more loop ends than loop starts");
    lcur--;
  } else ERRX("internal error loop_hand invalid action: %s", A.action);
}

ACTION_RUN(lc_run) {
  lcur++;
  DBG4("loop count start; depth: %d top actn: %d count: %d", lcur-lctl, *pactn, V0.u);
  lcur->type = COUNT;
  lcur->count = V0.u;
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
#endif

ACTION_RUN(le_run) {
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
      int time2stop = 0;
      if (myrank == 0) {
        if (lcur->ltime <= ETIMER_ELAPSED(&lcur->tmr)) {
          DBG4("loop sync rank 0 end, done; depth: %d top actn: %d", lcur-lctl, lcur->top);
          time2stop = 1;
        } else {
          DBG4("loop sync rank 0 end, not done; depth: %d top actn: %d", lcur-lctl, lcur->top);
        }
      }
      MPI_CK(MPI_Bcast(&time2stop, 1, MPI_INT, 0, MPI_COMM_WORLD));
      if (time2stop) {
        VERB1("loop sync end, done; depth: %d top actn: %d", lcur-lctl, lcur->top);
        lcur--;
      } else {
        *pactn = lcur->top;
        DBG4("loop sync end, not done; depth: %d top actn: %d", lcur-lctl, lcur->top);
      }
      break;
    #endif
    default:
      ERRX("internal error le_run invalid looptype %d", lcur->type);
  }
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
  if (memcount <= 0) ERRX("Touch without cooresponding allocate");
}

ACTION_CHECK(vf_check) {
  if (memcount-- <= 0) ERRX("Free without cooresponding allocate");
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
  if (memcount > 0) {
    p = (char*)memptr;
    end_p1 = p + memptr->size;
    DBG4("Touching memory at %p, length 0x%llx, stride: %lld", p, memptr->size, stride);
    while (p < end_p1) {
      if (p - (char *)memptr >= sizeof(struct memblk)) {
        DBG5("touch memptr: %p memlen: 0x%llx: end_p1: %p p: %p", memptr, memptr->size, end_p1, p);
        *p = 'x';
      }
      p += stride;
    }
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

//----------------------------------------------------------------------------
// mi, mb, mf (MPI init, barrier, finalize) action handlers
//----------------------------------------------------------------------------
#ifdef MPI
static void *mpi_sbuf = NULL, *mpi_rbuf = NULL;
static size_t mpi_buf_len = 0;

ACTION_RUN(mi_run) {
  MPI_CK(MPI_Init(NULL, NULL));
  get_id();
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
                      MPI_COMM_WORLD, &status));
}

ACTION_RUN(mb_run) {
  MPI_CK(MPI_Barrier(MPI_COMM_WORLD));
}

ACTION_RUN(mf_run) {
  MPI_CK(MPI_Finalize());
  get_id();
  mpi_sbuf = FREEX(mpi_sbuf);
  mpi_rbuf = FREEX(mpi_rbuf);
  mpi_buf_len = 0;
}
#endif


//----------------------------------------------------------------------------
// fi, fr, ff (floating point addition init, run, free) action handlers
//----------------------------------------------------------------------------
static double * nums;
static U64 flap_size = 0, count;

ACTION_CHECK(fi_check) {
  flap_size = V0.u;
  count = V1.u;
  if (flap_size < 2) ERRX("%s; size must be at least 2", A.desc);
}

ACTION_CHECK(fr_check) {
  U64 rep = V0.u;
  U64 stride = V1.u;

  if (!flap_size) ERRX("%s; fr without prior fi", A.desc);
  if ((count-1)%stride != 0) ERRX("%s; count-1 must equal a multiple of stride", A.desc);
  if (rep<1) ERRX("%s; rep must be at least 1", A.desc);
}

ACTION_CHECK(ff_check) {
  if (!flap_size) ERRX("%s; ff without prior fi", A.desc);
  flap_size = 0;
}

ACTION_RUN(fi_run) {
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

ACTION_RUN(fr_run) {
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

  VERB2("flapper done; predicted: %e sum: %e delta: %e", predicted, sum, sum - predicted);
  VERB2("FP Adds: %llu, time: %f Seconds, MFLAPS: %e", fp_add_ct, delta_t, (double)fp_add_ct / delta_t / 1000000.0);
}

ACTION_RUN(ff_run) {
  flap_size = 0;
  free(nums);
}


//----------------------------------------------------------------------------
// hx (heap exercisor) action handler
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
    if (!blk[n].ptr) ERRX("heapx: malloc %td bytes failed", blk[n].size);
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
#endif

//----------------------------------------------------------------------------
// hi, hdo, heo, hew, her, hec, hdc, hf (HIO) action handlers
//----------------------------------------------------------------------------
#ifdef HIO
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
ENUM_NAMP(HIO_, ERR_IO_TEMPORARY)
ENUM_NAMP(HIO_, ERR_IO_PERMANENT)
ENUM_NAMP(HIO_, ANY)
ENUM_END(etab_herr, 0, NULL)

static hio_context_t context = NULL;
static hio_dataset_t dataset = NULL;
static hio_element_t element = NULL;
static hio_return_t hio_exp = HIO_SUCCESS;
static void * wbuf = NULL, *rbuf = NULL;
static U64 bufsz = 0;
static int hio_check = 0;
static U64 hew_ofs, her_ofs;
static U64 rw_count[2];
ETIMER hio_tmr;

ACTION_RUN(hi_run) {
  hio_return_t hrc;
  char * context_name = V0.s;
  char * data_root = V1.s;
  char * tmp_str;
  char * root_var = "data_roots";
  DBG4("HIO_SET_ELEMENT_UNIQUE: %lld", HIO_SET_ELEMENT_UNIQUE);
  DBG4("HIO_SET_ELEMENT_SHARED: %lld", HIO_SET_ELEMENT_SHARED);
  MPI_Comm myworld = MPI_COMM_WORLD;
  // Workaround for read only context_data_root and no default context
  {
    int rc;
    char ename[255];
    snprintf(ename, sizeof(ename), "HIO_context_%s_%s", context_name, root_var); 
    DBG4("setenv %s=%s", ename, data_root);
    rc = setenv(ename, data_root, 1);
    DBG4("setenv %s=%s rc:%d", ename, data_root, rc);
    DBG4("getenv(%s) returns \"%s\"", ename, getenv(ename));
  }
  // end workaround
  DBG1("Invoking hio_init_mpi context:%s root:%s", context_name, data_root);
  hrc = hio_init_mpi(&context, &myworld, NULL, NULL, context_name);
  VERB3("hio_init_mpi rc:%d context_name:%s", hrc, context_name);
  if (HIO_SUCCESS != hrc) {
    VERB0("hio_init_mpi failed rc:%d", hrc);
    hio_err_print_all(context, stderr, "hio_init_mpi error: ");
  } else {
    //DBG1("Invoking hio_config_set_value var:%s val:%s", root_var, data_root);
    //hrc = hio_config_set_value((hio_object_t)context, root_var, data_root);
    //VERB2("hio_config_set_value rc:%d var:%s val:%s", hrc, root_var, data_root);
    DBG1("Invoking hio_config_get_value var:%s", root_var);
    hrc = hio_config_get_value((hio_object_t)context, root_var, &tmp_str);
    VERB3("hio_config_get_value rc:%d, var:%s value=\"%s\"", hrc, root_var, tmp_str);
    if (HIO_SUCCESS == hrc) {
      free(tmp_str);
    } else {
      VERB0("hio_config_get_value failed, rc:%d var:%s", hrc, root_var);
      hio_err_print_all(context, stderr, "hio_config_get_value error: ");
    }
  }
}

ACTION_RUN(hdo_run) {
  hio_return_t hrc;
  int hio_fail;
  char * ds_name = V0.s;
  U64 ds_id = V1.u;
  int flag_i = V2.i;
  int mode_i = V3.i;
  rw_count[0] = rw_count[1] = 0;
  MPI_CK(MPI_Barrier(MPI_COMM_WORLD));
  ETIMER_START(&hio_tmr);
  hrc = hio_dataset_open (context, &dataset, ds_name, ds_id, flag_i, mode_i);

  fail_count += (hio_fail = (hrc != hio_exp && hio_exp != HIO_ANY) ? 1: 0);
  if (hio_fail || MY_MSG_CTX->verbose_level >= 3) {
    MSG("%s: hio_dataset_open %s rc: %s exp: %s", A.desc, hio_fail ? "FAIL": "OK", enum_name(MY_MSG_CTX, &etab_herr, hrc), enum_name(MY_MSG_CTX, &etab_herr, hio_exp));
    if (hio_fail) hio_err_print_all(context, stderr, "hio_dataset_open error: "); 
  }

  // VERB3("%s; hio_dataset_open rc:%d(%s)", A.desc, hrc,enum_name(MY_MSG_CTX, &etab_herr, hrc));
  // if (HIO_SUCCESS != hrc) {
  //   VERB0("%s; hio_dataset_open failed rc:%d(%s)", A.desc, hrc,enum_name(MY_MSG_CTX, &etab_herr, hrc));
  //   hio_err_print_all(context, stderr, "hio_dataset_open error: ");
  // }

}

ACTION_CHECK(heo_check) {
  bufsz = V2.u;
}

ACTION_RUN(heo_run) {
  hio_return_t hrc;
  char * el_name = V0.s;
  int flag_i = V1.i;
  char * flag_s = V1.s;
  bufsz = V2.u;
  hrc = hio_element_open (dataset, &element, el_name, flag_i);
  VERB3("hio_element_open rc:%d name:%s flags:%s", hrc, el_name, flag_s);
  if (HIO_SUCCESS != hrc) {
    VERB0("hio_elememt_open failed rc:%d name:%s flags:%s", hrc, el_name, flag_s);
    hio_err_print_all(context, stderr, "hio_element_open error: ");
  }

  wbuf = MALLOCX(bufsz+LFSR_22_CYCLE);
  lfsr_22_byte_init();
  lfsr_22_byte(wbuf, bufsz+LFSR_22_CYCLE); 

  rbuf = MALLOCX(bufsz);
  hew_ofs = her_ofs = 0;
}

ACTION_RUN(hck_run) {
  hio_check = V0.u;
  VERB0("HIO read data checking is now %s", V0.s); 
}

ACTION_CHECK(hew_check) {
  U64 size = V1.u;
  if (size > bufsz) ERRX("%s; size > bufsz", A.desc);
}

ACTION_RUN(hew_run) {
  ssize_t retsize;
  I64 p_ofs = V0.u;
  U64 size = V1.u;
  U64 a_ofs;
  if (p_ofs < 0) { 
    a_ofs = hew_ofs;
    hew_ofs += -p_ofs;
  } else {
    a_ofs = p_ofs;
  }
  retsize = hio_element_write (element, a_ofs, 0, wbuf + (a_ofs%LFSR_22_CYCLE), 1, size);
  VERB3("hio_element_write retsize:%d ofs:%lld size:%lld", retsize, a_ofs, size);
  if (size != retsize) {
    VERB0("hio_element_write failed retsize:%d ofs:%lld size:%lld", retsize, a_ofs, size);
    hio_err_print_all(context, stderr, "hio_element_write error: ");
  } else {
    rw_count[1] += size;
  }
}

ACTION_CHECK(her_check) {
  U64 size = V1.u;
  if (size > bufsz) ERRX("%s; size > bufsz", A.desc);
}

ACTION_RUN(her_run) {
  ssize_t retsize;
  I64 p_ofs = V0.u;
  U64 size = V1.u;
  U64 a_ofs;
  if (p_ofs < 0) { 
    a_ofs = her_ofs;
    her_ofs += -p_ofs;
  } else {
    a_ofs = p_ofs;
  }
  retsize = hio_element_read (element, a_ofs, 0, rbuf, 1, size);
  VERB3("hio_element_read retsize:%d ofs:%lld size:%lld", retsize, a_ofs, size);
  if (size != retsize) {
    VERB0("hio_element_read failed retsize:%d ofs:%lld size:%lld", retsize, a_ofs, size);
    hio_err_print_all(context, stderr, "hio_element_read error: ");
  } else {
    rw_count[0] += size;
  }
  if (hio_check) {
    if (memcmp(rbuf, wbuf + (a_ofs%LFSR_22_CYCLE), size)) {
      VERB0("Error: hio_element_read data miscompare ofs:%lld size:%lld", a_ofs, size);
    } else {
      VERB3("hio data read check successful");
    }
  }
}

ACTION_RUN(hec_run) {
  hio_return_t hrc;
  hrc = hio_element_close(&element);
  VERB3("hio_element_close rc:%d", hrc);
  if (HIO_SUCCESS != hrc) {
    VERB0("hio_close element_failed rc:%d", hrc);
    hio_err_print_all(context, stderr, "hio_element_close error: ");
  }
  DBG1("Invoking free(%p)", wbuf);
  free(wbuf);
  wbuf = NULL;
  DBG1("Invoking free(%p)", rbuf);
  free(rbuf);
  rbuf = NULL;
  bufsz = 0;
}

ACTION_RUN(hdc_run) {
  hio_return_t hrc;
  hrc = hio_dataset_close(&dataset);
  MPI_CK(MPI_Barrier(MPI_COMM_WORLD));
  double time = ETIMER_ELAPSED(&hio_tmr);
  VERB3("hio_datset_close rc:%d", hrc);
  if (HIO_SUCCESS != hrc) {
    VERB0("hio_datset_close failed rc:%d", hrc);
    hio_err_print_all(context, stderr, "hio_dataset_close error: ");
  }
  U64 rw_count_sum[2];
  MPI_CK(MPI_Reduce(rw_count, rw_count_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD));
  if (myrank == 0)
    VERB1("hdo-hdc R/W GB: %f %f  time: %f S  R/W speed: %f %f GB/S",
          (double)rw_count_sum[0]/1E9, (double)rw_count_sum[1]/1E9, time, 
           rw_count_sum[0] / time / 1E9, rw_count_sum[1] / time / 1E9 );  
}

ACTION_RUN(hf_run) {
  hio_return_t hrc;
  hrc = hio_fini(&context);
  VERB3("hio_fini rc:%d", hrc);
  if (hrc != HIO_SUCCESS) {
    VERB0("hio_fini failed, rc:%d", hrc);
    hio_err_print_all(context, stderr, "hio_fini error: ");
  }
}

ACTION_RUN(hxrc_run) {
  hio_exp = V0.i;
  VERB0("%s; HIO expected rc now %d(%s)", A.desc, V0.i, V0.s);
}

#endif

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

//----------------------------------------------------------------------------
// Enum conversion tables
//----------------------------------------------------------------------------
#ifdef HIO
ENUM_START(etab_hflg)
ENUM_NAMP(HIO_FLAG_, RDONLY)
ENUM_NAMP(HIO_FLAG_, WRONLY)
ENUM_NAMP(HIO_FLAG_, CREAT)
ENUM_NAMP(HIO_FLAG_, TRUNC)
ENUM_NAMP(HIO_FLAG_, APPEND)
ENUM_END(etab_hflg, 1, ",")

ENUM_START(etab_hdsm)  // hio dataset mode
ENUM_NAMP(HIO_SET_ELEMENT_, UNIQUE)
ENUM_NAMP(HIO_SET_ELEMENT_, SHARED)
ENUM_END(etab_hdsm, 0, NULL)

ENUM_START(etab_onff)  // On, Off + case variants
ENUM_NAME("OFF", 0)
ENUM_NAME("ON",  1)
ENUM_NAME("off", 0)
ENUM_NAME("on",  1)
ENUM_NAME("Off", 0)
ENUM_NAME("On",  1)
ENUM_END(etab_onff, 0, NULL)
#endif

//----------------------------------------------------------------------------
// Argument string parsing table
//----------------------------------------------------------------------------
struct parse {
  char * cmd;
  enum ptype param[MAX_PARAM];
  action_check * checker;
  action_run * runner;
} parse[] = {
// Command  V0    V1    V2    V3    V4    Check          Run
  {"v",    {UINT, NONE, NONE, NONE, NONE}, verbose_check, verbose_run },
  {"d",    {UINT, NONE, NONE, NONE, NONE}, debug_check,   debug_run   },
  {"qof",  {UINT, NONE, NONE, NONE, NONE}, NULL,          qof_run     },
  {"im",   {STR,  NONE, NONE, NONE, NONE}, imbed_check,   NULL        },
  {"lc",   {UINT, NONE, NONE, NONE, NONE}, loop_check,    lc_run      },
  {"lt",   {DOUB, NONE, NONE, NONE, NONE}, loop_check,    lt_run      },
  #ifdef MPI
  {"ls",   {DOUB, NONE, NONE, NONE, NONE}, loop_check,    ls_run      },
  #endif
  {"le",   {NONE, NONE, NONE, NONE, NONE}, loop_check,    le_run      },
  {"o",    {UINT, NONE, NONE, NONE, NONE}, NULL,          stdout_run  },
  {"e",    {UINT, NONE, NONE, NONE, NONE}, NULL,          stderr_run  },
  {"s",    {DOUB, NONE, NONE, NONE, NONE}, sleep_check,   sleep_run   },
  {"va",   {UINT, NONE, NONE, NONE, NONE}, va_check,      va_run      },
  {"vt",   {PINT, NONE, NONE, NONE, NONE}, vt_check,      vt_run      },
  {"vf",   {NONE, NONE, NONE, NONE, NONE}, vf_check,      vf_run      },
  #ifdef MPI
  {"mi",   {NONE, NONE, NONE, NONE, NONE}, NULL,          mi_run      },
  {"msr",  {PINT, PINT, NONE, NONE, NONE}, NULL,          msr_run     },
  {"mb",   {NONE, NONE, NONE, NONE, NONE}, NULL,          mb_run      },
  {"mf",   {NONE, NONE, NONE, NONE, NONE}, NULL,          mf_run      },
  #endif
  {"fi",   {UINT, PINT, NONE, NONE, NONE}, fi_check,      fi_run      },
  {"fr",   {PINT, PINT, NONE, NONE, NONE}, fr_check,      fr_run      },
  {"ff",   {NONE, NONE, NONE, NONE, NONE}, ff_check,      fr_run      },
  {"hx",   {UINT, UINT, UINT, UINT, UINT}, hx_check,      hx_run      },
  #ifdef DLFCN
  {"dlo",  {STR,  NONE, NONE, NONE, NONE}, dlo_check,     dlo_run     },
  {"dls",  {STR,  NONE, NONE, NONE, NONE}, dls_check,     dls_run     },
  {"dlc",  {NONE, NONE, NONE, NONE, NONE}, dlc_check,     dlc_run     },
  #endif
  #ifdef HIO
  {"hi",   {STR,  STR,  NONE, NONE, NONE}, NULL,          hi_run      },
  {"hdo",  {STR,  UINT, HFLG, HDSM, NONE}, NULL,          hdo_run     },
  {"hck",  {ONFF, NONE, NONE, NONE, NONE}, NULL,          hck_run     },
  {"heo",  {STR,  HFLG, UINT, NONE, NONE}, heo_check,     heo_run     },
  {"hew",  {SINT, UINT, NONE, NONE, NONE}, hew_check,     hew_run     },
  {"her",  {SINT, UINT, NONE, NONE, NONE}, her_check,     her_run     },
  {"hec",  {NONE, NONE, NONE, NONE, NONE}, NULL,          hec_run     },
  {"hdc",  {NONE, NONE, NONE, NONE, NONE}, NULL,          hdc_run     },
  {"hf",   {NONE, NONE, NONE, NONE, NONE}, NULL,          hf_run      },
  {"hxrc", {HERR, NONE, NONE, NONE, NONE}, NULL,          hxrc_run    },
  #endif
  {"k",    {UINT, NONE, NONE, NONE, NONE}, NULL,          raise_run   },
  {"x",    {UINT, NONE, NONE, NONE, NONE}, NULL,          exit_run    },
};

//----------------------------------------------------------------------------
// Argument string parser - call check routines, build action vector
//----------------------------------------------------------------------------
void parse_action() {
  int t = -1, i, j;
  ACTION nact;

  msg_context_set_verbose(MY_MSG_CTX, 0); 
  msg_context_set_debug(MY_MSG_CTX, 0); 

  #ifdef DLFCN
    dl_num = -1;
  #endif

  while ( ++t < tokc ) {
    for (i = 0; i < DIM1(parse); ++i) { // parse table loop
      if (0 == strcmp(tokv[t], parse[i].cmd)) {
        DBG3("match: tokv[%d]: %s parse[%d].cmd: %s", t, tokv[t], i, parse[i].cmd);
        nact.tokn = t;
        nact.actn = actc;
        nact.action = tokv[t];
        nact.desc = ALLOC_PRINTF("action %d: %s", actc+1, tokv[t]);
      
        for (j = 0; j < MAX_PARAM; ++j) {
          if (parse[i].param[j] == NONE) break;
          if (tokc - t <= 1) ERRX("action %d \"%s\" missing param %d", nact.tokn, nact.action, j+1);
          nact.desc = STRCATRX(STRCATRX(nact.desc, " "), tokv[t+1]);
          DBG5("%s ...; parse[%d].param[%d]: %d", nact.desc, i, j, parse[i].param[j]);
          switch (parse[i].param[j]) {
            case SINT:
            case UINT:
            case PINT:
              t++;
              nact.v[j].u = getI64(tokv[t], parse[i].param[j], &nact);
              break;
            case DOUB:
              t++;
              nact.v[j].d = getDoub(tokv[t], parse[i].param[j], &nact);
              break;
            case STR:
              nact.v[j].s = tokv[++t];
              break;
            #ifdef HIO
            case HFLG:
              t++; 
              int rc = str2enum(MY_MSG_CTX, &etab_hflg, tokv[t], &nact.v[j].i); 
              if (rc) ERRX("%s ...; invalid hio flag \"%s\"", nact.desc, tokv[t]);
              rc = enum2str(MY_MSG_CTX, &etab_hflg, nact.v[j].i, &nact.v[j].s); 
              if (rc) ERRX("%s ...; invalid hio flag \"%s\"", nact.desc, tokv[t]);
              break;
            case HDSM:
              t++; 
              rc = str2enum(MY_MSG_CTX, &etab_hdsm, tokv[t], &nact.v[j].i); 
              if (rc) ERRX("%s ...; invalid hio mode \"%s\"", nact.desc, tokv[t]);
              rc = enum2str(MY_MSG_CTX, &etab_hdsm, nact.v[j].i, &nact.v[j].s); 
              if (rc) ERRX("%s ...; invalid hio mode \"%s\"", nact.desc, tokv[t]);
              break;
            case HERR:
              t++; 
              rc = str2enum(MY_MSG_CTX, &etab_herr, tokv[t], &nact.v[j].i); 
              if (rc) ERRX("%s ...; invalid hio return \"%s\"", nact.desc, tokv[t]);
              rc = enum2str(MY_MSG_CTX, &etab_herr, nact.v[j].i, &nact.v[j].s); 
              if (rc) ERRX("%s ...; invalid hio return \"%s\"", nact.desc, tokv[t]);
              break;
            #endif
            case ONFF:
              t++;
              rc = str2enum(MY_MSG_CTX, &etab_onff, tokv[t], &nact.v[j].i); 
              if (rc) ERRX("%s ...; invalid ON / OFF token \"%s\"", nact.desc, tokv[t]);
              nact.v[j].s = nact.v[j].i?"ON":"OFF";
              break;
            case NONE:
              break;
            default:
              ERRX("%s ...; internal parse error parse[%d].param[%d]: %d", nact.desc, i, j, parse[i].param[j]);
          }
        }
        nact.runner = parse[i].runner; 
        add2actv(&nact);
        DBG2("Checking %s", nact.desc); 
        if (parse[i].checker) parse[i].checker(&actv[actc-1], t);
        break; // break parse table loop
      }
    }
    if (i >= DIM1(parse)) ERRX("action %d: \"%s\" not recognized.", t, tokv[t]);
  }
  if (lcur-lctl > 0) ERRX("Unterminated loop - more loop starts than loop ends");
  IFDBG4( for (int a=0; a<actc; a++) DBG0("actv[%d].desc: %s", a, actv[a].desc) );
  DBG1("Parse complete actc: %d", actc);
}

//----------------------------------------------------------------------------
// Action runner - call run routines for action vector entries
//----------------------------------------------------------------------------
void run_action() {
  int a = -1;

  msg_context_set_verbose(MY_MSG_CTX, 0); 
  msg_context_set_debug(MY_MSG_CTX, 0); 

  #ifdef DLFCN
    dl_num = -1;
  #endif

  while ( ++a < actc ) {
    DBG2("Running %s", actv[a].desc); 
    // Runner routine may modify variable a for looping
    if (actv[a].runner) actv[a].runner(&actv[a], &a);
    DBG3("Done %s; fails: %d qof: %d", actv[a].desc, fail_count, quit_on_fail);
    if (quit_on_fail != 0 && fail_count >= quit_on_fail) {
      VERB0("Quiting due to fails: %d >= qof: %d", fail_count, quit_on_fail); 
      break;
    }
  }
  VERB0("Action execution ended, fails: %d", fail_count);

}


//----------------------------------------------------------------------------
// Main - write help, call parser / dispatcher
//----------------------------------------------------------------------------
int main(int argc, char * * argv) {

  if (argc <= 1 || 0 == strncmp("-h", argv[1], 2)) {
    fputs(help, stdout);
    return 1;
  }

  msg_context_init(MY_MSG_CTX, 0, 0);
  get_id();  

  add2tokv(argc-1, argv+1); // Make initial copy of argv so im works

  // Make two passes through args, first to check, second to run.
  parse_action();
  run_action();

  return 0;
}
// --- end of xexec.c ---
