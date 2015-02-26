#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
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
  "  hxc <0|1>     Enable read data checking\n"
  "  hew <offset> <size> Element write - if offset negative, auto increment\n"
  "  her <offset> <size> Element read - if offset negative, auto increment\n"
  "  hec <name>    Element close\n"
  "  hdc           Dataset close\n"
  "  hf            Fini\n"
  #endif
  "  k <signal>    raise <signal> (number)\n"
  "  x <status>    exit with <status>\n"
  "\n"
  "  Numbers can be specified with suffixes k, K, ki, M, Mi, G, Gi, etc.\n"
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
#ifdef MPI
int myrank, mpi_size = 0;
#endif
int actc = 0;
char * * actv = NULL;
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

// Simple non-threadsafe timer start/stop routines
#ifdef CLOCK_REALTIME
  static struct timespec timer_start_time, timer_end_time;
  void timer_start(void) {
    if (clock_gettime(CLOCK_REALTIME, &timer_start_time)) ERRX("clock_gettime() failed: %s", strerror(errno));
  }
  double timer_end(void) {
    if (clock_gettime(CLOCK_REALTIME, &timer_end_time)) ERRX("clock_gettime() failed: %s", strerror(errno));
    return (double)(timer_end_time.tv_sec - timer_start_time.tv_sec) +
             (1E-9 * (double) (timer_end_time.tv_nsec - timer_start_time.tv_nsec));
  }
#else
  // Silly Mac OS doesn't support clock_gettime :-(
  static struct timeval timer_start_time, timer_end_time;
  void timer_start(void) {
    if (gettimeofday(&timer_start_time, NULL)) ERRX("gettimeofday() failed: %s", strerror(errno));
  }
  double timer_end(void) {
    if (gettimeofday(&timer_end_time, NULL)) ERRX("gettimeofday() failed: %s", strerror(errno));
    return (double)(timer_end_time.tv_sec - timer_start_time.tv_sec) +
             (1E-6 * (double) (timer_end_time.tv_usec - timer_start_time.tv_usec));
  }
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

enum ptype { SINT, UINT, PINT, STR, HFLG, HDSM, NONE };

U64 getI64(char * num, enum ptype type, int actn) {
  long long int n;
  char * endptr;
  DBG3("getI64 num: %s", num);
  errno = 0;
  n = strtoll(num, &endptr, 0);
  if (errno != 0) ERRX("action %d, invalid integer \"%s\"", actn, num);
  if (type == UINT && n < 0) ERRX("action %d, negative integer \"%s\"", actn, num);
  if (type == PINT && n <= 0) ERRX("action %d, non-positive integer \"%s\"", actn, num);
  if (*endptr == '\0');
  else if (!strcmp("k",  endptr)) n *= 1000;
  else if (!strcmp("K",  endptr)) n *= 1024;
  else if (!strcmp("ki", endptr)) n *= 1024;
  else if (!strcmp("M",  endptr)) n *= (1000 * 1000);
  else if (!strcmp("Mi", endptr)) n *= (1024 * 1024);
  else if (!strcmp("G",  endptr)) n *= (1000 * 1000 * 1000);
  else if (!strcmp("Gi", endptr)) n *= (1024 * 1024 * 1024);
  else if (!strcmp("T",  endptr)) n *= (1ll * 1000 * 1000 * 1000 * 1000);
  else if (!strcmp("Ti", endptr)) n *= (1ll * 1024 * 1024 * 1024 * 1024);
  else if (!strcmp("P",  endptr)) n *= (1ll * 1000 * 1000 * 1000 * 1000 * 1000);
  else if (!strcmp("Pi", endptr)) n *= (1ll * 1024 * 1024 * 1024 * 1024 * 1024);
  else ERRX("action %d, invalid integer \"%s\"", actn, num);
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
  unsigned char * buf = malloc(size);

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
// Action handler definitions
//----------------------------------------------------------------------------
typedef struct pval {
  U64 u;
  char * s;
  int i;  // Use for enums 
} pval;

// Many action handlers, use macro to keep function defs in sync
#define MAX_PARAM 5
#define ACTION_HAND(name) void (name)(int run, char * action, int * pactn, pval v0, pval v1, pval v2, pval v3, pval v4)
typedef ACTION_HAND(action_hand);

//----------------------------------------------------------------------------
// v, d (verbose, debug) action handlers
//----------------------------------------------------------------------------
ACTION_HAND(verbose_hand) {
  int verbose = v0.u;
  if (verbose > MAX_VERBOSE) ERRX("Verbosity level %d > maximum %d", verbose, MAX_VERBOSE);
  msg_context_set_verbose(MY_MSG_CTX, verbose);
  if (run) VERB0("Verbosity level is now %d", verbose);
}

ACTION_HAND(debug_hand) {
  int debug = v0.u;

  if (debug == 5) {
    lfsr_test(); 
  }

  if (debug > DBGMAXLEV) ERRX("requested debug level %d > maximum %d."
                            " Rebuild with -DDBGMAXLEV=<n> to increase"
                            " (see comments in source.)", debug, DBGMAXLEV);
  msg_context_set_debug(MY_MSG_CTX, debug);
  VERB0("Debug message level is now %d; run: %d", debug, run);
}

//----------------------------------------------------------------------------
// im (imbed) action handler
//----------------------------------------------------------------------------
void add2actv(int n, char * * newact) {
  if (n == 0) return;
  actv = realloc(actv, (actc + n) * sizeof(char *));
  if (!actv) ERRX("actv realloc error: %s", strerror(errno));
  memcpy(actv + actc, newact, n * sizeof(char *));
  actc += n;
}

ACTION_HAND(imbed_hand) {
  DBG1("Starting %s fn: \"%s\"", action, v0.s);
  if (!run) {
    // Open file and read into buffer
    FILE * file;
    if (!strcmp(v0.s, "-")) file = stdin;
    else file = fopen(v0.s, "r");
    if (!file) ERRX("unable to open file %s: %s", v0.s, strerror(errno));
    #define BUFSZ 1024*1024
    void * p = malloc(BUFSZ);
    if (!p) ERRX("malloc error: %s", strerror(errno));
    size_t size;
    size = fread(p, 1, BUFSZ, file);
    DBG4("fread %s returns %d", v0.s, size);
    if (ferror(file)) ERRX("error reading file %s %d %s", v0.s, ferror(file), strerror(ferror(file)));
    if (!feof(file)) ERRX("imbed file %s larger than buffer (%d bytes)", v0.s, BUFSZ);
    fclose(file);
    p = realloc(p, size);
    if (!p) ERRX("realloc error: %s", strerror(errno));

    // Save old actc / actv, copy up through current action into new actc / actv
    int old_actc = actc;
    char * * old_actv = actv;
    actc = 0;
    actv = NULL;
    add2actv(*pactn+1, old_actv);

    // tokenize buffer, append to actc / actv
    char * sep = " \t\n\f\r";
    char * a = strtok(p, sep);
    while (a) {
      DBG4("imbed_hand add tok: \"%s\" actc: %d", a, actc);
      add2actv(1, &a);
      a = strtok(NULL, sep);
    }

    // append remainder of old actc / actv to new
    add2actv(old_actc - *pactn - 1, &old_actv[*pactn + 1]);
    free(old_actv);
  } else {
    VERB1("Imbed %s", v0.s);
  }
}


//----------------------------------------------------------------------------
// lc, lt, ls, le (looping) action handlers
//----------------------------------------------------------------------------
#define MAX_LOOP 16
enum looptype {COUNT, TIME, SYNC};
struct loop_ctl {
  enum looptype type;
  int count;
  int top;
  struct timeval start;
  struct timeval end;
} lctl[MAX_LOOP+1];
struct loop_ctl * lcur = &lctl[0];

ACTION_HAND(loop_hand) {
  if (!strcmp(action, "lc")) {
    DBG1("Starting lc count: %lld", v0.u);
    if (++lcur - lctl >= MAX_LOOP) ERRX("Maximum nested loop depth of %d exceeded", MAX_LOOP);
    if (run) {
      DBG4("loop count start; depth: %d top actn: %d count: %d", lcur-lctl, *pactn, v0.u);
      lcur->type = COUNT;
      lcur->count = v0.u;
      lcur->top = *pactn;
    }
  } else if (!strcmp(action, "lt")) {
    DBG1("Starting lt seconds: %lld", v0.u);
    if (++lcur - lctl >= MAX_LOOP) ERRX("Maximum nested loop depth of %d exceeded", MAX_LOOP);
    if (run) {
      DBG4("loop time start; depth: %d top actn: %d time: %d", lcur - lctl, *pactn, v0.u);
      lcur->type = TIME;
      lcur->top = *pactn;
      if (gettimeofday(&lcur->end, NULL)) ERRX("loop time start: gettimeofday() failed: %s", strerror(errno));
      lcur->end.tv_sec += v0.u;  // Save future time of loop end
    }
  #ifdef MPI
  } else if (!strcmp(action, "ls")) {
    DBG1("Starting ls seconds: %lld", v0.u);
    if (++lcur - lctl >= MAX_LOOP) ERRX("Maximum nested loop depth of %d exceeded", MAX_LOOP);
    if (run) {

      DBG4("loop sync start; depth: %d top actn: %d time: %d", lcur - lctl, *pactn, v0.u);
      lcur->type = SYNC;
      lcur->top = *pactn;
      if (myrank == 0) {
        if (gettimeofday(&lcur->end, NULL)) ERRX("loop time start: gettimeofday() failed: %s", strerror(errno));
        lcur->end.tv_sec += v0.u;  // Save future time of loop end
      }
    }
  #endif
  } else if (!strcmp(action, "le")) {
  DBG1("Starting le");
    if (lcur <= lctl) ERRX("loop end when no loop active - more loop ends than loop starts");
    if (!run) {
      lcur--;
    } else {
      if (lcur->type == COUNT) { // Count loop
        if (--lcur->count > 0) {
          *pactn = lcur->top;
          DBG4("loop count end, not done; depth: %d top actn: %d count: %d", lcur-lctl, lcur->top, lcur->count);
        } else {
          DBG4("loop count end, done; depth: %d top actn: %d count: %d", lcur-lctl, lcur->top, lcur->count);
          lcur--;
        }
      } else if (lcur->type == TIME) { // timed loop
        struct timeval now;
        if (gettimeofday(&now, NULL)) ERRX("loop end: gettimeofday() failed: %s", strerror(errno));
        if (now.tv_sec > lcur->end.tv_sec ||
             (now.tv_sec == lcur->end.tv_sec && now.tv_usec >= lcur->end.tv_usec) ) {
          DBG4("loop time end, done; depth: %d top actn: %d", lcur-lctl, lcur->top);
          lcur--;
        } else {
          *pactn = lcur->top;
          DBG4("loop time end, not done; depth: %d top actn: %d", lcur-lctl, lcur->top);
        }
      #ifdef MPI
      } else { // Sync loop
        int time2stop = 0;
        struct timeval now;
        if (myrank == 0) {
          if (gettimeofday(&now, NULL)) ERRX("loop end: gettimeofday() failed: %s", strerror(errno));
          if (now.tv_sec > lcur->end.tv_sec ||
               (now.tv_sec == lcur->end.tv_sec && now.tv_usec >= lcur->end.tv_usec) ) {
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
      #endif
      }
    }

  } else ERRX("internal error loop_hand invalid action: %s", action);
}

//----------------------------------------------------------------------------
// o, e (stdout, stderr) action handlers
//----------------------------------------------------------------------------
ACTION_HAND(stdout_hand) {
  if (run) {
    U64 line;
    for (line = 1; line <= v0.u; line++) {
      // Message padded to exactly 100 bytes long.
      MSG("action %-4u stdout line %-8lu of %-8lu %*s", *pactn - 1, line, v0.u, 34 - id_string_len, "");
    }
  }
}

ACTION_HAND(stderr_hand) {
  if (run) {
    U64 line;
    for (line = 1; line <= v0.u; line++) {
      // Message padded to exactly 100 bytes long.
      MSGE("action %-4u stderr line %-8lu of %-8lu %*s", *pactn - 1, line, v0.u, 34 - id_string_len, "");
    }
  }
}

//----------------------------------------------------------------------------
// s (sleep) action handler
//----------------------------------------------------------------------------
ACTION_HAND(sleep_hand) {
  if (run) {
    DBG1("Sleeping for %llu seconds", v0.u);
    sleep(v0.u);
  }
}

//----------------------------------------------------------------------------
// va, vt, vf (memory allocate, touch, free) action handlers
//----------------------------------------------------------------------------
ACTION_HAND(mem_hand) {
  struct memblk {
    size_t size;
    struct memblk * prev;
  };
  static struct memblk * memptr;
  static int memcount;

  DBG1("mem_hand start; action: %s memcount: %d", action, memcount);
  if (!strcmp(action, "va")) { // va - allocate memory
    memcount++;
    if (run) {
      size_t len = v0.u;
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
  } else if (!strcmp(action, "vt")) { // vt - touch memory
    U64 stride = v0.u;
    if (!run) {
      if (memcount <= 0) ERRX("Touch without cooresponding allocate");
    } else {
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
  } else if (!strcmp(action, "vf")) { // vf - free memory
    if (!run) {
      if (memcount-- <= 0) ERRX("Free without cooresponding allocate");
    } else {
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
  } else ERRX("internal error mem_hand invalid action: %s", action);
}


//----------------------------------------------------------------------------
// mi, mb, mf (MPI init, barrier, finalize) action handlers
//----------------------------------------------------------------------------
#ifdef MPI
ACTION_HAND(mpi_hand) {
static void *mpi_sbuf, *mpi_rbuf;
static size_t mpi_buf_len;

  if (run) {
    DBG1("Starting %s", action);
    if (!strcmp(action, "mi")) {
      MPI_CK(MPI_Init(NULL, NULL));
      get_id();
    } else if (!strcmp(action, "msr")) {
      int len = v0.u;
      int stride = v1.u;
      MPI_Status status;
      if (mpi_buf_len != len) {
        mpi_sbuf = realloc(mpi_sbuf, len);
        if (!mpi_sbuf) ERRX("msr realloc %d error: %s", len, strerror(errno));
        mpi_rbuf = realloc(mpi_rbuf, len);
        if (!mpi_rbuf) ERRX("msr realloc %d error: %s", len, strerror(errno));
        mpi_buf_len = len;
      }
      int dest = (myrank + stride) % mpi_size;
      int source = (myrank - stride + mpi_size) % mpi_size;
      DBG2("msr len: %d dest: %d source: %d", len, dest, source);
      MPI_CK(MPI_Sendrecv(mpi_sbuf, len, MPI_BYTE, dest, 0,
                          mpi_rbuf, len, MPI_BYTE, source, 0,
                          MPI_COMM_WORLD, &status));
    } else if (!strcmp(action, "mb")) {
      MPI_CK(MPI_Barrier(MPI_COMM_WORLD));
    } else if (!strcmp(action, "mf")) {
      MPI_CK(MPI_Finalize());
      get_id();
    } else ERRX("internal error mpi_hand invalid action: %s", action);
  }
}
#endif


//----------------------------------------------------------------------------
// fi, fr, ff (floating point addition init, run, free) action handlers
//----------------------------------------------------------------------------
ACTION_HAND(flap_hand) {
  static double * nums;
  static U64 size, count;
  U64 i, iv;

  DBG1("Starting %s", action);
  if (!strcmp(action, "fi")) {
    size = v0.u;
    count = v1.u;
    DBG1("flapper init starting; size: %llu count: %llu", size, count);
    if (size<2) ERRX("flapper: size must be at least 2");

    if (run) {
      U64 N = size * count;
      int rc;

      rc = posix_memalign((void * *)&nums, 4096, N * sizeof(double));
      if (rc) ERRX("flapper: posix_memalign %d doubles failed: %s", N, strerror(rc));

      iv = 0;
      for (i=0; i<N; ++i) {
        if (i%size != 0) {
          nums[i] = (double) ++iv;
          DBG4("nums[%d] = %d", i, iv);
        }
      }

    }
  } else if (!strcmp(action, "fr")) {
    U64 rep = v0.u;
    U64 stride = v1.u;

    if (!size) ERRX("fr without prior fi");
    if ((count-1)%stride != 0) ERRX("flapper: count-1 must equal a multiple of stride");
    if (rep<1) ERRX("flapper: rep must be at least 1");

    if (run) {
      double sum, delta_t, predicted;
      U64 b, ba, r, d, fp_add_ct, max_val;
      U64 N = size * count;
      DBG1("flapper run starting; rep: %llu stride: %llu", rep, stride);

      max_val = (size-1) * count;
      predicted = (pow((double) max_val, 2.0) + (double) max_val ) / 2 * (double)rep;
      DBG1("v: %d predicted: %f", max_val, predicted);
      fp_add_ct = (max_val * rep) + count;

      for (i=0; i<N; i+=size) {
        nums[i] = 0.0;
          DBG3("nums[%d] = %d", i, 0);
      }

      DBG1("flapper starting; size: %llu count: %llu rep: %llu stride: %llu", size, count, rep, stride);
      timer_start();

      for (b=0; b<count; ++b) {
        ba = b * stride % count;
        U64 d_sum = ba*size;
        U64 d_first = d_sum + 1;
        U64 d_lastp1 = (ba+1)*size;
        DBG3("b: %llu ba:%llu", b, ba);
        for (r=0; r<rep; ++r) {
          sum = nums[d_sum];
          for (d=d_first; d<d_lastp1; ++d) {
            sum += nums[d];
            DBG3("val: %f sum: %f", nums[d], sum)
          }
          nums[d_sum] = sum;
        }
      }

      sum = 0.0;
      for (d=0; d<count*size; d+=size) {
        sum += nums[d];
      }

      delta_t = timer_end();

      VERB2("flapper done; predicted: %e sum: %e delta: %e", predicted, sum, sum - predicted);
      VERB2("FP Adds: %llu, time: %f Seconds, MFLAPS: %e", fp_add_ct, delta_t, (double)fp_add_ct / delta_t / 1000000.0);
    }
  } else if (!strcmp(action, "ff")) {
    if (!size) ERRX("ff without prior fi");
    size = 0;
    if (run) free(nums);
  } else ERRX("internal error flap_hand invalid action: %s", action);

}


//----------------------------------------------------------------------------
// hx (heap exercisor) action handler
//----------------------------------------------------------------------------
ACTION_HAND(heap_hand) {
  U64 min = v0.u;
  U64 max = v1.u;
  U64 blocks = v2.u;
  U64 limit = v3.u;
  U64 count = v4.u;

  DBG1("Starting %s", action);
  if (!run) {
    if (min < 1) ERRX("heapx: min < 1");
    if (min > max) ERRX("heapx: min > max");
    if (max > limit) ERRX("heapx: max > limit");
  } else {
    double min_l2 = log2(min), max_l2 = log2(max);
    double range_l2 = max_l2 - min_l2;
    U64 i, n, k, total = 0;
    int b;

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
        timer_start();
        free(blk[n].ptr);
        stat[b].ftime += timer_end();
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
          timer_start();
          free(blk[k].ptr);
          stat[b].ftime += timer_end();
          total -= blk[k].size;
          blk[k].size = 0;
          blk[k].ptr = 0;
        }
      }

      VERB2("heapx: total: %llu; malloc and touch %td bytes", total, blk[n].size);
      b = (int) log2(blk[n].size);
      timer_start();
      blk[n].ptr = malloc(blk[n].size);
      stat[b].atime += timer_end();
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
        timer_start();
        free(blk[n].ptr);
        stat[b].ftime += timer_end();
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
}


//----------------------------------------------------------------------------
// dlo, dls, dlc (dl open, sym, close) action handlers
//----------------------------------------------------------------------------
#ifdef DLFCN
static int dl_num = -1;
ACTION_HAND(dl_hand) {
  static void * handle[100];

  DBG1("Starting %s", action);
  if (!strcmp(action, "dlo")) {
    char * name = v0.s;
    if (++dl_num >= DIM1(handle)) ERRX("Too many dlo commands, limit is %d", DIM1(handle));
    if (run) {
      DBG2("dlo name: %s dl_num: %d", name, dl_num);
      handle[dl_num] = dlopen(name, RTLD_NOW);
      VERB3("dlopen(%s) returns %p", name, handle[dl_num]);
      if (!handle[dl_num]) {
        VERB0("dlopen failed: %s", dlerror());
        dl_num--;
      }
    }
  } else if (!strcmp(action, "dls")) {
    char * symbol = v0.s;
    if (dl_num < 0) ERRX("No currently open dynamic library");
    if (run) {
      char * error = dlerror();
      DBG1("dls symbol: %s dl_num: %d handle[dl_num]: %p", symbol, dl_num, handle[dl_num]);
      void * sym = dlsym(handle[dl_num], symbol);
      VERB3("dlsym(%s) returns %p", symbol, sym);
      error = dlerror();
      if (error) VERB0("dlsym error: %s", error);
    }
  } else if (!strcmp(action, "dlc")) {
    if (dl_num < 0) ERRX("No currently open dynamic library");
    if (run) {
      DBG1("dlc dl_num: %d handle[dl_num]: %p", dl_num, handle[dl_num]);
      int rc = dlclose(handle[dl_num]);
      VERB3("dlclose() returns %d", rc);
      if (rc) VERB0("dlclose error: %s", dlerror());
    }
    dl_num--;
  } else ERRX("internal error dl_hand invalid action: %s", action);
}
#endif

//----------------------------------------------------------------------------
// hi, hdo, heo, hew, her, hec, hdc, hf (HIO) action handlers
//----------------------------------------------------------------------------
#ifdef HIO
static hio_context_t context = NULL;
static hio_dataset_t dataset = NULL;
static hio_element_t element = NULL;
static void * wbuf = NULL, *rbuf = NULL;
static U64 bufsz = 0;
static int hio_check = 0;
static U64 hew_ofs, her_ofs;
static U64 rw_count[2];
ACTION_HAND(hio_hand) {
  int rc;
  if (!strcmp(action, "hi")) {
    char * context_name = v0.s;
    char * data_root = v1.s;
    char * tmp_str;
    char * root_var = "data_roots";
    if (run) {
      DBG4("HIO_SET_ELEMENT_UNIQUE: %lld", HIO_SET_ELEMENT_UNIQUE);
      DBG4("HIO_SET_ELEMENT_SHARED: %lld", HIO_SET_ELEMENT_SHARED);
      MPI_Comm myworld = MPI_COMM_WORLD;
      // Workaround for read only context_data_root and no default context
      char ename[255];
      snprintf(ename, sizeof(ename), "HIO_context_%s_%s", context_name, root_var); 
      DBG4("setenv %s=%s", ename, data_root);
      rc = setenv(ename, data_root, 1);
      DBG4("setenv %s=%s rc:%d", ename, data_root, rc);
      DBG4("getenv(%s) returns \"%s\"", ename, getenv(ename));
      // end workaround
      DBG1("Invoking hio_init_mpi context:%s root:%s", context_name, data_root);
      rc = hio_init_mpi(&context, &myworld, NULL, NULL, context_name);
      VERB3("hio_init_mpi rc:%d context_name:%s", rc, context_name);
      if (HIO_SUCCESS != rc) {
        VERB0("hio_init_mpi failed rc:%d", rc);
        hio_err_print_all(context, stderr, "hio_init_mpi error: ");
      } else {
        //DBG1("Invoking hio_config_set_value var:%s val:%s", root_var, data_root);
        //rc = hio_config_set_value((hio_object_t)context, root_var, data_root);
        //VERB2("hio_config_set_value rc:%d var:%s val:%s", rc, root_var, data_root);
        DBG1("Invoking hio_config_get_value var:%s", root_var);
        rc = hio_config_get_value((hio_object_t)context, root_var, &tmp_str);
        VERB3("hio_config_get_value rc:%d, var:%s value=\"%s\"", rc, root_var, tmp_str);
        if (HIO_SUCCESS == rc) {
          free(tmp_str);
        } else {
          VERB0("hio_config_get_value failed, rc:%d var:%s", rc, root_var);
          hio_err_print_all(context, stderr, "hio_config_get_value error: ");
        }
      }
    }
  } else if (!strcmp(action, "hdo")) {
    char * ds_name = v0.s;
    U64 ds_id = v1.u;
    int flag_i = v2.i;
    char * flag_s = v2.s;
    int mode_i = v3.i;
    char * mode_s = v3.s;
    if (run) {
      rw_count[0] = rw_count[1] = 0;
      DBG1("Invoking hio_dataset_open name:%s id:%lld flags:%s mode:%s", ds_name, ds_id, flag_s, mode_s);
      MPI_CK(MPI_Barrier(MPI_COMM_WORLD));
      timer_start();
      rc = hio_dataset_open (context, &dataset, ds_name, ds_id, flag_i, mode_i);
      VERB3("hio_dataset_open rc:%d name:%s id:%lld flags:%s mode:%s", rc, ds_name, ds_id, flag_s, mode_s);
      if (HIO_SUCCESS != rc) {
        VERB0("hio_dataset_open failed rc:%d name:%s id:%lld flag_s:%s mode:%s", rc, ds_name, ds_id, flag_s, mode_s);
        hio_err_print_all(context, stderr, "hio_dataset_open error: ");
      }
    }
  } else if (!strcmp(action, "heo")) {
    char * el_name = v0.s;
    int flag_i = v1.i;
    char * flag_s = v1.s;
    bufsz = v2.u;
    if (run) {
      DBG1("Invoking hio_element_open name:%s flags:%s", el_name, flag_s);
      rc = hio_element_open (dataset, &element, el_name, flag_i);
      VERB3("hio_element_open rc:%d name:%s flags:%s", rc, el_name, flag_s);
      if (HIO_SUCCESS != rc) {
        VERB0("hio_elememt_open failed rc:%d name:%s flags:%s", rc, el_name, flag_s);
        hio_err_print_all(context, stderr, "hio_element_open error: ");
      }

      DBG1("Invoking malloc(%d)", bufsz);
      wbuf = malloc(bufsz+LFSR_22_CYCLE);
      VERB3("malloc(%d) returns %p", bufsz+LFSR_22_CYCLE, wbuf);
      if (!wbuf) ERRX("malloc(%d) failed", bufsz+LFSR_22_CYCLE);
      lfsr_22_byte_init();
      lfsr_22_byte(wbuf, bufsz+LFSR_22_CYCLE); 

      DBG1("Invoking malloc(%d)", bufsz);
      rbuf = malloc(bufsz);
      VERB3("malloc(%d) returns %p", bufsz, rbuf);
      if (!wbuf) ERRX("malloc(%d) failed", bufsz);
      hew_ofs = her_ofs = 0;
    }
  } else if (!strcmp(action, "hxc")) {
    I64 flag = v0.u;
    if (run) {
      if (flag) {
        hio_check = 1;
      } else {
        hio_check = 0;
      }
      VERB0("HIO read data checking is now %s", hio_check?"on": "off"); 
    }
  } else if (!strcmp(action, "hew")) {
    I64 p_ofs = v0.u;
    U64 size = v1.u;
    U64 a_ofs;
    if (!run) {
      if (size > bufsz) ERRX("hew: size > bufsz");
    } else {
      if (p_ofs < 0) { 
        a_ofs = hew_ofs;
        hew_ofs += -p_ofs;
      } else {
        a_ofs = p_ofs;
      }
      DBG1("Invoking hio_element_write ofs:%lld size:%lld", a_ofs, size);
      rc = hio_element_write (element, a_ofs, 0, wbuf + (a_ofs%LFSR_22_CYCLE), 1, size);
      VERB3("hio_element_write rc:%d ofs:%lld size:%lld", rc, a_ofs, size);
      if (size != rc) {
        VERB0("hio_element_write failed rc:%d ofs:%lld size:%lld", rc, a_ofs, size);
        hio_err_print_all(context, stderr, "hio_element_write error: ");
      } else {
        rw_count[1] += size;
      }
    }
  } else if (!strcmp(action, "her")) {
    I64 p_ofs = v0.u;
    U64 size = v1.u;
    U64 a_ofs;
    if (!run) {
      if (size > bufsz) ERRX("her: size > bufsz");
    } else {
      if (p_ofs < 0) { 
        a_ofs = her_ofs;
        her_ofs += -p_ofs;
      } else {
        a_ofs = p_ofs;
      }
      DBG1("Invoking hio_element_read ofs:%lld size:%lld", a_ofs, size);
      rc = hio_element_read (element, a_ofs, 0, rbuf, 1, size);
      VERB3("hio_element_read rc:%d ofs:%lld size:%lld", rc, a_ofs, size);
      if (size != rc) {
        VERB0("hio_element_read failed rc:%d ofs:%lld size:%lld", rc, a_ofs, size);
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
  } else if (!strcmp(action, "hec")) {
    if (run) {
      DBG1("Invoking hio_element_close");
      rc = hio_element_close(&element);
      VERB3("hio_element_close rc:%d", rc);
      if (HIO_SUCCESS != rc) {
        VERB0("hio_close element_failed rc:%d", rc);
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
  } else if (!strcmp(action, "hdc")) {
    if (run) {
      DBG1("Invoking hio_dataset_close");
      rc = hio_dataset_close(&dataset);
      MPI_CK(MPI_Barrier(MPI_COMM_WORLD));
      double time = timer_end();
      VERB3("hio_datset_close rc:%d", rc);
      if (HIO_SUCCESS != rc) {
        VERB0("hio_datset_close failed rc:%d", rc);
        hio_err_print_all(context, stderr, "hio_dataset_close error: ");
      }
      U64 rw_count_sum[2];
      MPI_CK(MPI_Reduce(rw_count, rw_count_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD));
      if (myrank == 0)
        VERB1("R/W bytes: %lld %lld time:%f R/W speed: %f %f GB/S", rw_count_sum[0], rw_count_sum[1], time, 
               rw_count_sum[0] / time / 1E9, rw_count_sum[1] / time / 1E9 );  
    }
  } else if (!strcmp(action, "hf")) {
    if (run) {
      DBG1("Invoking hio_fini");
      rc = hio_fini(&context);
      VERB3("hio_fini rc:%d", rc);
      if (rc != HIO_SUCCESS) {
        VERB0("hio_fini failed, rc:%d", rc);
        hio_err_print_all(context, stderr, "hio_fini error: ");
      }
    }
  } else ERRX("internal error hio_hand invalid action: %s", action);
}
#endif

//----------------------------------------------------------------------------
// k, x (signal, exit) action handlers
//----------------------------------------------------------------------------
ACTION_HAND(raise_hand) {
  if (run) {
    VERB0("Raising signal %d", v0.u);
    raise(v0.u);
  }
}

ACTION_HAND(exit_hand) {
  if (run) {
    VERB0("Exiting with status %d", v0.u);
    exit(v0.u);
  }
}

//----------------------------------------------------------------------------
// Enum conversion tables
//----------------------------------------------------------------------------
ENUM_START(etab_hflg)
ENUM_NAME("RDONLY", HIO_FLAG_RDONLY)
ENUM_NAME("WRONLY", HIO_FLAG_WRONLY)
ENUM_NAME("CREAT",  HIO_FLAG_CREAT)
ENUM_NAME("TRUNC",  HIO_FLAG_TRUNC)
ENUM_NAME("APPEND", HIO_FLAG_APPEND)
ENUM_END(etab_hflg, 1, ",")

ENUM_START(etab_hdsm)  // hio dataset mode
ENUM_NAME("UNIQUE",  HIO_SET_ELEMENT_UNIQUE)
ENUM_NAME("SHARED",  HIO_SET_ELEMENT_SHARED)
ENUM_END(etab_hdsm, 0, NULL)

//----------------------------------------------------------------------------
// Argument string parsing table
//----------------------------------------------------------------------------
struct parse {
  char * action;
  enum ptype param[MAX_PARAM];
  action_hand * handler;
} parse[] = {
  {"v",   {UINT, NONE, NONE, NONE, NONE}, verbose_hand},
  {"d",   {UINT, NONE, NONE, NONE, NONE}, debug_hand},
  {"im",  {STR,  NONE, NONE, NONE, NONE}, imbed_hand},
  {"lc",  {UINT, NONE, NONE, NONE, NONE}, loop_hand},
  {"lt",  {UINT, NONE, NONE, NONE, NONE}, loop_hand},
  #ifdef MPI
  {"ls",  {UINT, NONE, NONE, NONE, NONE}, loop_hand},
  #endif
  {"le",  {NONE, NONE, NONE, NONE, NONE}, loop_hand},
  {"o",   {UINT, NONE, NONE, NONE, NONE}, stdout_hand},
  {"e",   {UINT, NONE, NONE, NONE, NONE}, stderr_hand},
  {"s",   {UINT, NONE, NONE, NONE, NONE}, sleep_hand},
  {"va",  {UINT, NONE, NONE, NONE, NONE}, mem_hand},
  {"vt",  {PINT, NONE, NONE, NONE, NONE}, mem_hand},
  {"vf",  {NONE, NONE, NONE, NONE, NONE}, mem_hand},
  #ifdef MPI
  {"mi",  {NONE, NONE, NONE, NONE, NONE}, mpi_hand},
  {"msr", {PINT, PINT, NONE, NONE, NONE}, mpi_hand},
  {"mb",  {NONE, NONE, NONE, NONE, NONE}, mpi_hand},
  {"mb",  {NONE, NONE, NONE, NONE, NONE}, mpi_hand},
  {"mf",  {NONE, NONE, NONE, NONE, NONE}, mpi_hand},
  #endif
  {"fi",  {UINT, PINT, NONE, NONE, NONE}, flap_hand},
  {"fr",  {PINT, PINT, NONE, NONE, NONE}, flap_hand},
  {"ff",  {NONE, NONE, NONE, NONE, NONE}, flap_hand},
  {"hx",  {UINT, UINT, UINT, UINT, UINT}, heap_hand},
  #ifdef DLFCN
  {"dlo", {STR,  NONE, NONE, NONE, NONE}, dl_hand},
  {"dls", {STR,  NONE, NONE, NONE, NONE}, dl_hand},
  {"dlc", {NONE, NONE, NONE, NONE, NONE}, dl_hand},
  #endif
  #ifdef HIO
  {"hi",  {STR,  STR,  NONE, NONE, NONE}, hio_hand},
  {"hdo", {STR,  UINT, HFLG, HDSM, NONE}, hio_hand},
  {"hxc", {UINT, NONE, NONE, NONE, NONE}, hio_hand},
  {"heo", {STR,  HFLG, UINT, NONE, NONE}, hio_hand},
  {"hew", {SINT, UINT, NONE, NONE, NONE}, hio_hand},
  {"her", {SINT, UINT, NONE, NONE, NONE}, hio_hand},
  {"hec", {NONE, NONE, NONE, NONE, NONE}, hio_hand},
  {"hdc", {NONE, NONE, NONE, NONE, NONE}, hio_hand},
  {"hf",  {NONE, NONE, NONE, NONE, NONE}, hio_hand},
  #endif
  {"k",   {UINT, NONE, NONE, NONE, NONE}, raise_hand},
  {"x",   {UINT, NONE, NONE, NONE, NONE}, exit_hand}
};

//----------------------------------------------------------------------------
// Argument string parser - calls action handlers
//----------------------------------------------------------------------------
void parse_and_dispatch(int run) {
  int a = 0, aa, i, j, rc;
  pval vals[MAX_PARAM];

  msg_context_set_verbose(MY_MSG_CTX, 0); 
  msg_context_set_debug(MY_MSG_CTX, 0); 

  #ifdef DLFCN
    dl_num = -1;
  #endif

  while ( ++a < actc ) {
    for (i = 0; i < DIM1(parse); ++i) {
      if (0 == strcmp(actv[a], parse[i].action)) {
        DBG3("match: actv[%d]: %s parse[%d].action: %s run: %d", a, actv[a], i, parse[i].action, run);
        aa = a;
        for (j = 0; j < MAX_PARAM; ++j) {
          if (parse[i].param[j] == NONE) break;
          if (actc - a <= 1) ERRX("action %d \"%s\" missing param %d", aa, actv[aa], j+1);
          switch (parse[i].param[j]) {
            case SINT:
            case UINT:
            case PINT:
              a++;
              vals[j].u = getI64(actv[a], parse[i].param[j], a);
              break;
            case STR:
              vals[j].s = actv[++a];
              break;
            case HFLG:
              a++; 
              rc = str2enum(MY_MSG_CTX, &etab_hflg, actv[a], &vals[j].i); 
              if (rc) ERRX("action %d \"%s\" invalid hio flag \"%s\"", aa, actv[aa], actv[a]);
              rc = enum2str(MY_MSG_CTX, &etab_hflg, vals[j].i, &vals[j].s); 
              if (rc) ERRX("action %d \"%s\" invalid hio flag \"%s\"", aa, actv[aa], actv[a]);
              break;
            case HDSM:
              a++; 
              rc = str2enum(MY_MSG_CTX, &etab_hdsm, actv[a], &vals[j].i); 
              if (rc) ERRX("action %d \"%s\" invalid hio mode \"%s\"", aa, actv[aa], actv[a]);
              rc = enum2str(MY_MSG_CTX, &etab_hdsm, vals[j].i, &vals[j].s); 
              if (rc) ERRX("action %d \"%s\" invalid hio mode \"%s\"", aa, actv[aa], actv[a]);
              break;
            case NONE:
              break;
            default:
              ERRX("internal parse error parse[%d].param[%d]: %d", i, j, parse[i].param[j]);
          }
        }
        parse[i].handler(run, actv[aa], &a, vals[0], vals[1], vals[2], vals[3], vals[4]);
        break;
      }
    }
    if (i >= DIM1(parse)) ERRX("action %d: \"%s\" not recognized.", a, actv[a]);
  }
  if (lcur-lctl > 0) ERRX("Unterminated loop - more loop starts than loop ends");
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

  add2actv(argc, argv); // Make initial copy of argv so im works

  // Make two passes through args, first to check, second to run.
  parse_and_dispatch(0);
  parse_and_dispatch(1);

  VERB0("Returning from main rc: 0");
  return 0;
}
// --- end of xexec.c ---
