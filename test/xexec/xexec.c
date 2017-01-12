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
#include "xexec.h"
#define _GNU_SOURCE 1
#include <errno.h>
#include <signal.h>
#include <execinfo.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#ifdef MPI
#include <mpi.h>
#endif  // MPI

#define VERB_LEV_MULTI 2

char * help_pre =
  "xexec - multi-pupose HPC exercise and testing tool.  Processes command\n"
  "        line arguments and file input in sequence to control actions.\n"
  "        Version " XEXEC_VERSION " " __DATE__ " " __TIME__ "\n"
  "\n"
  "  Syntax:  xexec -h | [ action [param ...] ] ...\n"
  "\n"
  "  Where valid actions and their parameters are:"
  "\n"
;  

char * help_post =
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
  " MPI actions can be enabled by building with -DMPI.\n"
  "\n"
  #endif // MPI
  #ifndef DLFCN
  " dlopen and related actions can be enabled by building with -DDLFCN.\n"
  "\n"
  #endif // DLFCN
;

//----------------------------------------------------------------------------
// Global variables
//----------------------------------------------------------------------------
static GLOBAL * xexec_global_ptr;
#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)

ENUM_START(etab_opt)
ENUM_NAMP(OPT_, ROF)
ENUM_NAMP(OPT_, RCHK)
ENUM_NAMP(OPT_, XPERF)
ENUM_NAMP(OPT_, PERFXCHK)
ENUM_NAMP(OPT_, SMSGV1)
ENUM_NAMP(OPT_, PAVM)
ENUM_NAMP(OPT_, SIGHAND)
ENUM_END(etab_opt, 1, "+")

static const char * options_init = "-ROF+RCHK+XPERF-PERFXCHK-SMSGV1-PAVM+SIGHAND"; 

#define MAX_LOOP 16
enum looptype {COUNT, TIME, SYNC};

struct mod_table {
  module_init * init;
  module_help * help;
  void * state;
};

// State local to this module, used for parsing and execution control
typedef struct ctrl_state {
  size_t modsN;            // size of modules table
  struct mod_table * mods; // xexec modules table
  int parseN;          // Parse table count 
  struct xexec_act_parse * parse; // Parse table
  int tokc;            // size of tokv array
  char * * tokv;       // Token array 
  int tokn;            // In parse, next token after current action, used by im
  int actN;            // Total number of actions in array
  ACTION * actv;       // Action array
  int act_cur;         // Currently active action 
  struct loop_ctl {    
    enum looptype type;
    int count;
    double ltime;
    int top;
    ETIMER tmr;
  } lctl[MAX_LOOP+1];
  struct loop_ctl * lcur;
} CTRL;

static CTRL ctrl_state;

//----------------------------------------------------------------------------
// Common subroutines and macros
//----------------------------------------------------------------------------
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


void set_msg_id(GLOBAL * gptr) {
  char * p;
  char tmp_id[sizeof(G.id_string)];
  int rc;

  rc = gethostname(tmp_id, sizeof(tmp_id));
  if (rc != 0) ERRX("gethostname rc: %d %s", rc, strerror(errno));
  p = strchr(tmp_id, '.');
  if (p) *p = '\0';

  IF_MPI(
    MPI_CK(MPI_Comm_rank(G.mpi_comm, &G.myrank));
    G.my_prior_rank = G.myrank;
    sprintf(tmp_id+strlen(tmp_id), ".%d", G.myrank);
    MPI_CK(MPI_Comm_size(G.mpi_comm, &G.mpi_size));
    sprintf(tmp_id+strlen(tmp_id), "/%d", G.mpi_size);
  ) ELSE_MPI(
    G.myrank = 0;
    G.mpi_size = 0;
  )

  strcat(tmp_id, " ");
  strcpy(G.id_string, tmp_id);
  G.id_string_len = strlen(G.id_string);
  MY_MSG_CTX->id_string=G.id_string;
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
  for (size_t i = 0; i<sizeof(lfsr_state); ++i) {
    lfsr_state[i] = random() % 256;
  }
}

void lfsr_22_byte_init_p(void) {
  // Use a very simple PRNG to initialize lfsr_state
  int prime = 15485863; // The 1 millionth prime
  lfsr_state[0] = 0xA5;
  for (size_t i = 1; i<sizeof(lfsr_state); ++i) {
    lfsr_state[i] = (lfsr_state[i-1] * prime) % 256;
  }
  // Cycle a few times to mix things up
  unsigned char t[1000];
  lfsr_22_byte(t, sizeof(t));
}

# if 1
void lfsr_test(GLOBAL * gptr) {
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
struct mm_val_s {
  double val;
  char id[24];
};

struct min_max_s {
  struct mm_val_s min;
  struct mm_val_s max;
};

#ifdef MPI
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

void prt_mmmst(GLOBAL * gptr, double val, char * desc, char * unit) {
  double sum = 0, mean, diff, sumdiff = 0, sd;
  char b1[32], b2[32], b3[32], b4[32], b5[32];
  if (mpi_active()) {
    struct min_max_s mm;
    struct min_max_s mmr;
    #ifdef MPI
      MPI_Datatype mmtype;
      MPI_Op mmw_op;
    #endif // MPI

    // Set up MPI data type and op
    MPI_CK( MPI_Type_contiguous(sizeof(struct min_max_s), MPI_BYTE, &mmtype) );
    MPI_CK( MPI_Type_commit(&mmtype) );
    MPI_CK( MPI_Op_create(min_max_who, true, & mmw_op) );

    // Use two pass method to calculate variance
    MPI_CK( MPI_Allreduce(&val, &sum, 1, MPI_DOUBLE, MPI_SUM, G.mpi_comm) );
    mean = sum/(double)G.mpi_size;
    diff = (val - mean) * (val - mean);
    MPI_CK( MPI_Reduce(&diff, &sumdiff, 1, MPI_DOUBLE, MPI_SUM, 0, G.mpi_comm) );
    sd = sqrt(sumdiff / G.mpi_size);

    // Get min, max
    mm.min.val = mm.max.val = val;
    strncpy(mm.min.id, G.id_string, sizeof(mm.min.id)); 
    mm.min.id[sizeof(mm.min.id)-1] = '\0';
    char * p = strchr(mm.min.id, ' ');
    if (p) *p = '\0';
    strncpy(mm.max.id, mm.min.id, sizeof(mm.max.id)); 
    MPI_CK( MPI_Reduce(&mm, &mmr, 1, mmtype, mmw_op, 0, G.mpi_comm) );
    MPI_CK( MPI_Type_free(&mmtype) );
    MPI_CK( MPI_Op_free(&mmw_op) );

    if (0 == G.myrank) VERB1("%s mean/min/max/s/tot: %s %s %s %s %s %s   min/max: %s %s", desc,
                              eng_not(b1, sizeof(b1), mean, "D6.4", ""),
                              eng_not(b2, sizeof(b2), mmr.min.val,  "D6.4", ""), 
                              eng_not(b3, sizeof(b3), mmr.max.val,  "D6.4", ""), 
                              eng_not(b4, sizeof(b4), sd,   "D6.4", ""),
                              eng_not(b5, sizeof(b5), sum,  "D6.4", ""), unit,
                              mmr.min.id, mmr.max.id);
  } else { 
    VERB1("%s: %s", desc, eng_not(b1, sizeof(b1), val, "D6.4", unit));
  } 
}

//----------------------------------------------------------------------------
// data buffer setup and data check routines
//----------------------------------------------------------------------------
ENUM_START(etab_dbuf)  // Write buffer type
ENUM_NAME("RAND22P", RAND22P)
ENUM_NAME("RAND22", RAND22)
ENUM_NAME("OFS20",  OFS20)
ENUM_END(etab_dbuf, 0, NULL)

#define PRIME_LT_64KI    65521 // A prime a little less than 2**16
#define PRIME_75PCT_2MI 786431 // A prime about 75% of 2**20
#define PRIME_LT_2MI   1048573 // A prime a little less that 2**20

void dbuf_init(GLOBAL * gptr, enum dbuf_type type, U64 size) {
  G.rwbuf_len = size;
  int rc;

  FREEX(G.wbuf_ptr);
  FREEX(G.rbuf_ptr);

  switch (type) {
    case RAND22P:
      G.wbuf_bdy = 1;
      G.wbuf_repeat_len = LFSR_22_CYCLE;
      G.wbuf_data_object_hash_mod = PRIME_LT_64KI;
      rc = posix_memalign((void * *)&G.wbuf_ptr, 4096, G.rwbuf_len + G.wbuf_repeat_len);
      if (rc) ERRX("G.wbuf posix_memalign %d bytes failed: %s", G.rwbuf_len + G.wbuf_repeat_len, strerror(rc));
      lfsr_22_byte_init_p();
      lfsr_22_byte(G.wbuf_ptr, G.rwbuf_len + G.wbuf_repeat_len);
      break;
    case RAND22:
      G.wbuf_bdy = 1;
      G.wbuf_repeat_len = LFSR_22_CYCLE;
      G.wbuf_data_object_hash_mod = PRIME_LT_2MI;
      rc = posix_memalign((void * *)&G.wbuf_ptr, 4096, G.rwbuf_len + G.wbuf_repeat_len);
      if (rc) ERRX("G.wbuf posix_memalign %d bytes failed: %s", G.rwbuf_len + G.wbuf_repeat_len, strerror(rc));
      lfsr_22_byte_init();
      lfsr_22_byte(G.wbuf_ptr, G.rwbuf_len + G.wbuf_repeat_len);
      break;
    case OFS20:
      G.wbuf_bdy = sizeof(uint32_t); 
      G.wbuf_repeat_len = 1024 * 1024;
      G.wbuf_data_object_hash_mod = 1024 * 1024;
      rc = posix_memalign((void * *)&G.wbuf_ptr, 4096, G.rwbuf_len + G.wbuf_repeat_len);
      if (rc) ERRX("G.wbuf posix_memalign %d bytes failed: %s", G.rwbuf_len + G.wbuf_repeat_len, strerror(rc));
      for (U64 i = 0; i<(G.rwbuf_len+G.wbuf_repeat_len)/sizeof(uint32_t); ++i) {
        ((uint32_t *)G.wbuf_ptr)[i] = i * sizeof(uint32_t);
      }
      break;
  }

  G.rbuf_ptr = MALLOCX(G.rwbuf_len);

  DBG3("dbuf_init type: %s size: %lld G.wbuf_ptr: 0x%lX",              \
        enum_name(MY_MSG_CTX, &etab_dbuf, type), size, G.wbuf_ptr);  
  IFDBG3( hex_dump(G.wbuf_ptr, 32) );                                             

}

// Returns a pointer into wbuf based on offset and data object hash
void * get_wbuf_ptr(GLOBAL * gptr, char * ctx, U64 offset, U64 data_object_hash) {
  void * expected = (char *)G.wbuf_ptr + ( (offset + data_object_hash) % G.wbuf_repeat_len);
  DBG2("%s: object_hash: 0x%lX  G.wbuf_ptr: 0x%lX expected-G.wbuf_ptr: 0x%lX", ctx, data_object_hash, G.wbuf_ptr, (char *)expected - (char *)G.wbuf_ptr);  
  return expected;
}

// Convert string that uniquely identifies a data object into a hash
U64 get_data_object_hash(GLOBAL * gptr, char * obj_desc_string) {
  U64 obj_desc_hash = BDYDN(crc32(0, obj_desc_string, strlen(obj_desc_string)) % G.wbuf_data_object_hash_mod, G.wbuf_bdy);
  DBG4("data_object_desc_hash: \"%s\" 0x%lX", obj_desc_string, obj_desc_hash);
  return obj_desc_hash;
}

int check_read_data(GLOBAL * gptr, char * ctx, void * buf, size_t len, U64 offset, U64 data_object_hash) {
  int rc;
  void * expected = get_wbuf_ptr(&G, "hew", offset, data_object_hash);
  void * mis_comp;
  if ( ! (mis_comp = memdiff(buf, expected, len)) ) {
    rc = 0;
    VERB3("hio_element_read data check successful");
  } else {
    // Read data miscompare - dump lots of data about it
    rc = 1;
    I64 misc_ofs = (char*)mis_comp - (char*)buf;
    I64 dump_start = MAX(0, misc_ofs - 16) & (~15);
    VERB0("Error: %s data check miscompare", ctx);
    VERB0("       Data offset: 0x%llX  %lld", offset, offset); 
    VERB0("       Data length: 0x%llX  %lld", len, len); 
    VERB0("      Data address: 0x%llX", buf); 
    VERB0("Miscompare address: 0x%llX", mis_comp); 
    VERB0(" Miscompare offset: 0x%llX  %lld", (char*)mis_comp - (char*)buf, (char*)mis_comp - (char*)buf); 

    VERB0("Debug: G.wbuf_ptr addr: 0x%lX:", G.wbuf_ptr); hex_dump(G.wbuf_ptr, 32);

    VERB0("Miscompare expected data at offset 0x%llX %lld follows:", dump_start, dump_start);
    hex_dump( (char*)expected + dump_start, 96);

    VERB0("Miscompare actual data at offset 0x%llX %lld follows:", dump_start, dump_start);
    hex_dump( (char*)buf + dump_start, 96);

    VERB0("XOR of Expected addr: 0x%lX  Miscomp addr: 0x%lX", expected, mis_comp);
    char * xorbuf_ptr = MALLOCX(len);
    for (size_t i=0; i<len; i++) {
      ((char *)xorbuf_ptr)[i] = ((char *)buf)[i] ^ ((char *)expected)[i];
    }
    hex_dump( xorbuf_ptr, len);
    FREEX(xorbuf_ptr);

  }
  return rc;
}

//----------------------------------------------------------------------------
// Compare regex, handle errors. Returns 0 on match or REG_NOMATCH
//----------------------------------------------------------------------------
int rx_run(GLOBAL * gptr, int n, struct action * actionp, char * line) {
  int rc = regexec(A.v[n].rxp, line, 0, NULL, 0);
  if (rc != 0 && rc != REG_NOMATCH) {
    char buf[512];
    regerror(rc, A.v[n].rxp, buf, sizeof(buf));
    ERRX("%s; regex: %s", A.desc, buf);
  }
  return rc;
}

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

//----------------------------------------------------------------------------
// Local module for actions related action sequence and other controls
// State data for this module controls action sequence and is shared
// with action_runner 
//----------------------------------------------------------------------------
static char * help = 
  "  opt +-<option> ...  Set (+) or unset (-) options. Valid options are:\n"
  "                %s\n"
  "                Initial value is: %s plus env XEXEC_OPT\n"
  "                ? displays current set options\n"
  "  name <test name> Set test name for final success / fail message\n"
  "  im <file>     imbed a file of actions at this point, - means stdin\n"
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
  "                <rank> = -1 tests for last rank, -2, -3, etc. similarly\n"
  "  eif           terminates ifr conditional, may not be nested\n"
;


MODULE_INIT(xexec_ctrl_init) {
  CTRL * s = state;
  s->act_cur = -1;  
  s->lcur = &(s->lctl[0]);  
  return 0;
}

MODULE_HELP(xexec_ctrl_help) {
  fprintf(file, help, enum_list(MY_MSG_CTX, &etab_opt), options_init);
  return 0;
}

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

  //prt_mmmst((double) G.myrank, "rank", "");
 
}

//----------------------------------------------------------------------------
// opt (set option flag) action handler
//----------------------------------------------------------------------------

void parse_opt(GLOBAL * gptr, char *ctx, const char *flags, int *set, int *clear) {
  int rc = flag2enum(MY_MSG_CTX, &etab_opt, flags, set, clear);
  if (rc) ERRX("%s; invalid option. Valid values are +/- %s", ctx, enum_list(MY_MSG_CTX, &etab_opt));
  DBG4("%s: flags: %s set: 0x%X  clear: 0x%X", ctx, flags, *set, *clear);
}

void show_opt(GLOBAL * gptr, char *ctx, int val) {
  char * opt_desc;
  enum2str(MY_MSG_CTX, &etab_opt, val, &opt_desc);
  VERB1("%s options: %s", ctx, opt_desc);
  FREEX(opt_desc);
}

ACTION_CHECK(opt_check) {
  char * flags = V0.s;
  if (0 == strcmp("?", flags)) V0.c = ~(V0.i = 0);
  else parse_opt(&G, A.desc, flags, &V0.i, &V0.c);
  DBG4("opt: flags: %s set: 0x%X  clear: 0x%X \n", flags, V0.i, V0.c);
}

ACTION_RUN(opt_run) {
  if (0 == V0.i && ~0 == V0.c) show_opt(&G, A.desc, G.options);
  else G.options = (enum options) ( V0.c & (V0.i | G.options) ); 
}

//----------------------------------------------------------------------------
// name action handler
//----------------------------------------------------------------------------
ACTION_RUN(name_run) {
  G.test_name = V0.s;
}

//----------------------------------------------------------------------------
// im (imbed) action handler
//----------------------------------------------------------------------------
#define S (*state)
void add2actv(GLOBAL * gptr, CTRL * state, ACTION * newact) {
  S.actv = REALLOCX(S.actv, (S.actN + 1) * sizeof(ACTION));
  memcpy(S.actv+S.actN, newact, sizeof(ACTION));
  S.actN++;
}

void add2tokv(GLOBAL * gptr, CTRL * state, int n, char * * newtok) {
  if (n == 0) return;
  S.tokv = REALLOCX(S.tokv, (S.tokc + n) * sizeof(char *));
  memcpy(S.tokv+S.tokc, newtok, n * sizeof(char *));
  S.tokc += n;
}
#undef S

#define S (* ((CTRL *)(actionp->state)) )
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

    // Save old S.tokc / S.tokv, copy up through current action into new S.tokc / S.tokv
    int old_tokc = S.tokc;
    char * * old_tokv = S.tokv;
    S.tokc = 0;
    S.tokv = NULL;
    add2tokv(&G, &S, S.tokn+1, old_tokv);

    // tokenize buffer, append to S.tokc / S.tokv
    char * sep = " \t\n\f\r";
    char * a = strtok(p, sep);
    while (a) {
      DBG4("imbed_hand add tok: \"%s\" S.tokc: %d", a, S.tokc);
      add2tokv(&G, &S, 1, &a);
      a = strtok(NULL, sep);
    }

    // append remainder of old S.tokc / S.tokv to new
    add2tokv(&G, &S, old_tokc - S.tokn - 1, &old_tokv[S.tokn + 1]);
    FREEX(old_tokv);
}

//----------------------------------------------------------------------------
// lc, lt, ls, le (looping) action handlers
//----------------------------------------------------------------------------

ACTION_CHECK(loop_check) {
  if ( !strcmp(A.action, "lc")  ||
       !strcmp(A.action, "lcr") ||
    #ifdef MPI
       !strcmp(A.action, "ls")  ||
    #endif
       !strcmp(A.action, "lt") ) {
    if (++S.lcur - S.lctl >= MAX_LOOP) ERRX("%s: Maximum nested loop depth of %d exceeded", A.desc, MAX_LOOP);
  } else if (!strcmp(A.action, "le")) {
    if (S.lcur <= S.lctl) ERRX("%s: loop end when no loop active - more loop ends than loop starts", A.desc);
    S.lcur--;
  } else ERRX("%s: internal error loop_hand invalid action: %s", A.desc, A.action);
}

ACTION_RUN(lc_run) {
  S.lcur++;
  DBG4("loop count start; depth: %d top act_cur: %d count: %d", S.lcur-S.lctl, S.act_cur, V0.u);
  S.lcur->type = COUNT;
  S.lcur->count = V0.u;
  S.lcur->top = S.act_cur;
}

ACTION_RUN(lcr_run) {
  int count = rand_range(V0.u, V1.u, 1);
  S.lcur++;
  DBG4("loop count rand start; depth: %d top act_cur: %d count: %d", S.lcur-S.lctl, S.act_cur, count);
  S.lcur->type = COUNT;
  S.lcur->count = count;
  S.lcur->top = S.act_cur;
}

ACTION_RUN(lt_run) {
  S.lcur++;
  DBG4("loop time start; depth: %d top act_cur: %d time: %d", S.lcur - S.lctl, S.act_cur, V0.u);
  S.lcur->type = TIME;
  S.lcur->top = S.act_cur;
  S.lcur->ltime = V0.d;
  ETIMER_START(&S.lcur->tmr);
}

#ifdef MPI
ACTION_RUN(ls_run) {
  S.lcur++;
  DBG4("loop sync start; depth: %d top act_cur: %d time: %d", S.lcur - S.lctl, S.act_cur, V0.u);
  S.lcur->type = SYNC;
  S.lcur->top = S.act_cur;
  S.lcur->ltime = V0.d;
  if (G.myrank == 0) {
    ETIMER_START(&S.lcur->tmr);
  }
}
#endif // MPI

ACTION_RUN(le_run) {
  #ifdef MPI
  int time2stop = 0;
  #endif
  switch (S.lcur->type) {
    case COUNT:;
      if (--S.lcur->count > 0) {
        S.act_cur = S.lcur->top;
        DBG4("loop count end, not done; depth: %d top actn: %d count: %d", S.lcur-S.lctl, S.lcur->top, S.lcur->count);
      } else {
        DBG4("loop count end, done; depth: %d top actn: %d count: %d", S.lcur-S.lctl, S.lcur->top, S.lcur->count);
        S.lcur--;
      }
      break;
    case TIME:;
      if (S.lcur->ltime <= ETIMER_ELAPSED(&S.lcur->tmr)) {
        DBG4("loop time end, done; depth: %d top actn: %d", S.lcur-S.lctl, S.lcur->top);
        S.lcur--;
      } else {
        S.act_cur = S.lcur->top;
        DBG4("loop time end, not done; depth: %d top actn: %d", S.lcur-S.lctl, S.lcur->top);
      }
      break;
    #ifdef MPI
    case SYNC:;
      if (G.myrank == 0) {
        if (S.lcur->ltime <= ETIMER_ELAPSED(&S.lcur->tmr)) {
          DBG4("loop sync rank 0 end, done; depth: %d top actn: %d", S.lcur-S.lctl, S.lcur->top);
          time2stop = 1;
        } else {
          DBG4("loop sync rank 0 end, not done; depth: %d top actn: %d", S.lcur-S.lctl, S.lcur->top);
        }
      }
      MPI_CK(MPI_Bcast(&time2stop, 1, MPI_INT, 0, G.mpi_comm));
      if (time2stop) {
        VERB1("loop sync end, done; depth: %d top actn: %d", S.lcur-S.lctl, S.lcur->top);
        S.lcur--;
      } else {
        S.act_cur = S.lcur->top;
        DBG4("loop sync end, not done; depth: %d top actn: %d", S.lcur-S.lctl, S.lcur->top);
      }
      break;
    #endif // MPI
    default:
      ERRX("%s: internal error le_run invalid looptype %d", A.desc, S.lcur->type);
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
      if (G.myrank == req_rank) cond = true;
    } else {
      if ( G.myrank == G.mpi_size + req_rank) cond = true;
    } 
  } else {
    cond = true;
  }
  
  if (!cond) S.act_cur = ife_num;

  DBG4("ifr done req_rank: %d cond: %d act_cur: %d", req_rank, cond, S.act_cur);
}

ACTION_RUN(eif_run) {
  DBG4("ife done act_cur: %d", S.act_cur);
}

// Special action runner for printing out /@@ comments
ACTION_RUN(cmsg_run) {
  R0_OR_VERB_START
    VERB1("%s", V0.s);
  R0_OR_VERB_END
}
#undef S

MODULE_INSTALL(xexec_ctrl_install) {
  struct xexec_act_parse parse[] = {
  // Command   V0    V1    V2    V3    V4     Check          Run
    {"ztest", {SINT, DOUB, STR,  STR,  NONE}, NULL,          ztest_run   },   // For testing code fragments  
    {"opt",   {STR,  NONE, NONE, NONE, NONE}, opt_check,     opt_run     },
    {"name",  {STR,  NONE, NONE, NONE, NONE}, NULL,          name_run    },
    {"im",    {STR,  NONE, NONE, NONE, NONE}, imbed_check,   NULL        },
    {"lc",    {UINT, NONE, NONE, NONE, NONE}, loop_check,    lc_run      },
    {"lcr",   {UINT, UINT, NONE, NONE, NONE}, loop_check,    lcr_run     },
    {"lt",    {DOUB, NONE, NONE, NONE, NONE}, loop_check,    lt_run      },
    {"ifr",   {SINT, NONE, NONE, NONE, NONE}, ifr_check,     ifr_run     },
    {"eif",   {NONE, NONE, NONE, NONE, NONE}, eif_check,     eif_run     },
    #ifdef MPI
    {"ls",    {DOUB, NONE, NONE, NONE, NONE}, loop_check,    ls_run      },
    #endif
    {"le",    {NONE, NONE, NONE, NONE, NONE}, loop_check,    le_run      },
  };

  xexec_act_add(&G, parse, DIM1(parse), xexec_ctrl_init, &ctrl_state, xexec_ctrl_help);

  return 0;
}

//----------------------------------------------------------------------------
// Argument string parser - call check routines, build action vector
//----------------------------------------------------------------------------
void decode(GLOBAL * gptr, ENUM_TABLE * etptr, char * tok, char * name, char * desc, PVAL * val) {
  int rc = str2enum(MY_MSG_CTX, etptr, tok, &val->i);
  if (rc) ERRX("%s ...; invalid %s \"%s\". Valid values are %s",
               desc, name, tok, enum_list(MY_MSG_CTX, etptr));
  rc = enum2str(MY_MSG_CTX, etptr, val->i, &val->s);
  if (rc) ERRX("%s ...; invalid %s \"%s\"", desc, name, tok);
}

void decode_int(GLOBAL * gptr, ENUM_TABLE * etptr, char * tok, char * name, char * desc, PVAL * val) {
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

// Compare parse table cmd strings for qsort and bsearch
int parse_comp(const void * p1, const void * p2) {
  return strcmp( ((struct xexec_act_parse *)p1)->cmd,
                 ((struct xexec_act_parse *)p2)->cmd );
}

// hio related enums defined in xexec_hio.c
extern ENUM_TABLE etab_hflg, etab_hdsm, etab_herr, etab_hulm, etab_hdsi, etab_hcpr, etab_hvat, etab_hvao;

// Convert one parameter token to its value based on type
void cvt_param(GLOBAL * gptr, char * token, enum ptype type, PVAL * val, char * desc) {
  char buf[256];
  int rc;
  switch (type) {
    case SINT:
    case UINT:
    case PINT:
      rc = cvt_num((enum cvt_num_type)type, token, &val->u, buf, sizeof(buf));
      if (rc) ERRX("%s ...; %s", desc, buf);
       break;
    case DOUB:
      rc = cvt_num((enum cvt_num_type)type, token, &val->d, buf, sizeof(buf));
      if (rc) ERRX("%s ...; %s", desc, buf);
      break;
    case STR:
      val->s = token;
      break;
    case REGX:
      val->rxp = MALLOCX(sizeof(regex_t));
      rc = regcomp(val->rxp, token, REG_EXTENDED | REG_NOSUB);
      if (rc) {
        regerror(rc, NULL, buf, sizeof(buf));
        ERRX("%s; regex: %s", desc, buf);
      }
      break;
    case DBUF:
      decode(&G, &etab_dbuf, token, "Data buffer pattern type", desc, val);
      break;
    #ifdef HIO
    case HFLG:
      decode(&G, &etab_hflg, token, "hio flag", desc, val);
      break;
    case HDSM:
      decode(&G, &etab_hdsm, token, "hio mode", desc, val);
      break;
    case HERR:
      decode(&G, &etab_herr, token, "hio return", desc, val);
      break;
    case HULM:
      decode(&G, &etab_hulm, token, "hio unlink mode", desc, val);
      break;
    case HDSI:
      decode_int(&G, &etab_hdsi, token, "hio dataset ID", desc, val);
      break;
    case HCPR:
      decode(&G, &etab_hcpr, token, "hio recommendation", desc, val);
      break;
    case HVAT:
      decode(&G, &etab_hvat, token, "value assert type", desc, val);
      break;
    case HVAO:
      decode(&G, &etab_hvao, token, "value assert operator", desc, val);
      break;
    #if HIO_USE_DATAWARP
    case DWST:
      decode(&G, &etab_dwst, token, "DataWarp stage type", desc, val);
      break;
    #endif
    #endif
    case ONFF:
      decode(&G, &etab_onff, token, "ON / OFF value", desc, val);
      break;
    default:
      ERRX("internal error: param type invalid; token: \"%s\" desc: \"%s\" type: %d", token, desc, type);
  } // end switch
}

// Parse every action and its params from tokens into action array
#define S (*state)
void parse_action(GLOBAL * gptr, CTRL * state) {
  int t = -1, j;
  ACTION nact;

  msg_context_set_verbose(MY_MSG_CTX, 0);
  msg_context_set_debug(MY_MSG_CTX, 0);

  int comment_depth=0;
  char * comment_msg = NULL;

  for (size_t i=0; i<S.modsN; ++i) if (S.mods[i].init) S.mods[i].init(&G, S.mods[i].state);

  while ( ++t < S.tokc ) {
    if (0 == strcmp(S.tokv[t], "/@")) {
      comment_depth++;
      DBG3("comment start: S.tokv[%d]: %s depth: %d", t, S.tokv[t], comment_depth);
    } else if (0 == strcmp(S.tokv[t], "/@@")) {
      comment_depth++;
      comment_msg = STRDUPX("***");
      DBG3("comment start: S.tokv[%d]: %s depth: %d", t, S.tokv[t], comment_depth);
    } else if (0 == strcmp(S.tokv[t], "@/")) {
      comment_depth = MAX(0, comment_depth - 1);
      DBG3("comment end: S.tokv[%d]: %s depth: %d", t, S.tokv[t], comment_depth);
      if (comment_msg) {
        nact.tokn = t;
        nact.actn = S.actN;
        nact.action = S.tokv[t];
        nact.desc = ALLOC_PRINTF("action %d: /@@ %s", S.actN+1, comment_msg);
        nact.runner = cmsg_run;
        nact.v[0].s = comment_msg;
        add2actv(&G, &S, &nact);
        comment_msg = NULL;
      }
    } else if (comment_depth > 0) {
      if (comment_msg) comment_msg = STRCATRX(STRCATRX(comment_msg, " "), S.tokv[t]);
      DBG3("Token in comment skipped: S.tokv[%d]: %s depth: %d", t, S.tokv[t], comment_depth);
    } else {
      struct xexec_act_parse xap, *act_desc;
      xap.cmd = S.tokv[t];
      act_desc = bsearch(&xap, S.parse, S.parseN, sizeof(xap), parse_comp);
      if (act_desc) {
        DBG3("match: S.tokv[%d]: %s act_desc->cmd: %s", t, S.tokv[t], act_desc->cmd);
        nact.tokn = t;
        nact.actn = S.actN;
        nact.action = S.tokv[t];
        nact.desc = ALLOC_PRINTF("action %d: %s", S.actN, S.tokv[t]);

        for (j = 0; j < MAX_PARAM; ++j) { // loop over params
          if (act_desc->param[j] == NONE) break; // for j loop over params
          t++;
          if (t >= S.tokc) ERRX("action %d \"%s\" missing param %d", nact.tokn, nact.action, j+1);
          nact.desc = STRCATRX(STRCATRX(nact.desc, " "), S.tokv[t]);
          DBG5("%s ...; act_desc->param[%d]: %d", nact.desc, j, act_desc->param[j]);

          cvt_param(&G, S.tokv[t], act_desc->param[j], &nact.v[j], nact.desc);
        } // end for j loop
        nact.runner = act_desc->runner;
        nact.state = act_desc->state;
        add2actv(&G, &S, &nact);
        DBG2("Checking %s action.actn: %d", nact.desc, nact.actn);
        S.tokn = t;  // next token after current action
        if (act_desc->checker) act_desc->checker(&G, &S.actv[S.actN-1]);
      } else {
        ERRX("action %d: \"%s\" not recognized.", t, S.tokv[t]);
      }
    }
  }
  if (S.lcur-S.lctl > 0) ERRX("Unterminated loop - more loop starts than loop ends");
  if (ifr_depth != 0) ERRX("Unterminated ifr - ifr without eif");
  if (comment_depth > 0) ERRX("Unterminated comment - more comment starts than comment ends");
  IFDBG4( for (int a=0; a<S.actN; a++) DBG0("S.actv[%d].desc: %s", a, S.actv[a].desc) );
  DBG1("Parse complete S.actN: %d", S.actN);
}

//----------------------------------------------------------------------------
// Action runner - call run routines for action vector entries
//----------------------------------------------------------------------------
void run_action(GLOBAL * gptr, CTRL * state) {
  S.act_cur = -1;

  msg_context_set_verbose(MY_MSG_CTX, 1);
  msg_context_set_debug(MY_MSG_CTX, 0);

  for (size_t i=0; i<S.modsN; ++i) if (S.mods[i].init) S.mods[i].init(&G, S.mods[i].state);
  while ( ++(S.act_cur) < S.actN ) {
    VERB2("--- Running %s", S.actv[S.act_cur].desc);
    // Runner routine may modify variable a for looping or conditional
    int old_a = S.act_cur;
    errno = 0;
    if (S.actv[S.act_cur].runner) S.actv[S.act_cur].runner(&G, &S.actv[S.act_cur]);
    DBG3("Done %s; fails: %d ROF: %d act_cur: %d", S.actv[old_a].desc, G.local_fails, (G.options & OPT_ROF) ? 1: 0, S.act_cur);
    if (G.local_fails > 0 && !(G.options & OPT_ROF)) {
      VERB0("Quiting due to fails: %d and ROF not set", G.local_fails);
      break;
    }
  }
  VERB2("Action execution ended, Fails: %d", G.local_fails);

}
#undef S

//----------------------------------------------------------------------------
// Signal handler - init and run
//----------------------------------------------------------------------------
#undef G
#define G (*xexec_global_ptr)
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
#undef G

//----------------------------------------------------------------------------
// Main - write help, call parser / dispatcher
//----------------------------------------------------------------------------
int main(int argc, char * * argv) {
  // Establish global data - must be first since everything uses it
  xexec_global_ptr = calloc(1, sizeof(GLOBAL));
  if (!xexec_global_ptr) {
    fprintf(stderr, "calloc %lu bytes failed, exitting\n", sizeof(GLOBAL));
    return EXIT_FAILURE;
  }
  GLOBAL * gptr = xexec_global_ptr;
  #define G (*gptr)
  G.test_name = "<unnamed>";

  msg_context_init(MY_MSG_CTX, 0, 0);
  set_msg_id(&G);

  #define S ctrl_state

  add2tokv(&G, &S, argc-1, argv+1); // Make initial copy of argv so im works

  // Parse init and env option values
  int set, clear, rc;
  enum options newopt;
  char * xexec_opt = getenv("XEXEC_OPT");

  parse_opt(&G, "internal options_init", options_init, &set, &clear);
  newopt = (enum options) (clear & set);

  if (xexec_opt) { 
    parse_opt(&G, "XEXEC_OPT env var", xexec_opt, &set, &clear);
    newopt = (enum options) (clear & (set | newopt));
  }   
  G.options = newopt;

  if (G.options & OPT_SIGHAND) {
    sig_init();
  }

  // Add action routines to parse table
  rc = xexec_ctrl_install(&G); if (rc) ERRX("xexec_ctrl_init rc: %d", rc);
  rc = xexec_base_install(&G); if (rc) ERRX("xexec_base_init rc: %d", rc);
  #ifdef MPI
  rc = xexec_mpi_install(&G);  if (rc) ERRX("xexec_mpi_init rc: %d", rc);
  #endif // MPI 
  rc = xexec_fio_install(&G);  if (rc) ERRX("xexec_fio_init rc: %d", rc);
  rc = xexec_hio_install(&G);  if (rc) ERRX("xexec_hio_init rc: %d", rc);

  if (argc <= 1 || 0 == strncmp("-h", argv[1], 2)) {
    fprintf(stdout, "%s", help_pre);
    for (size_t i=0; i<S.modsN; ++i) if (S.mods[i].help) S.mods[i].help(&G, S.mods[i].state, stdout);
    fprintf(stdout, help_post, cvt_num_suffix());
    return 0;
  }

  // Make two passes through args, first to check, second to run.
  G.options = newopt;
  parse_action(&G, &S);

  G.options = newopt;
  run_action(&G, &S);

  // Suppress SUCCESS result message from all but rank 0
  // Suppress  if G.gather_fails, not rank 0 and local fails = 0
  if (G.gather_fails && G.my_prior_rank != 0 && G.local_fails == 0) {
    // do nothing
  } else {
    if (G.local_fails + G.global_fails == 0) { 

      if (G.options & OPT_SMSGV1) { // SMSGV1 option allows really quiet output on V0
        VERB1("xexec " XEXEC_VERSION " done.  Result: %s  Fails: %d  Test name: %s",
              "SUCCESS", G.local_fails + G.global_fails, G.test_name);
      } else {
        VERB0("xexec " XEXEC_VERSION " done.  Result: %s  Fails: %d  Test name: %s",
              "SUCCESS", G.local_fails + G.global_fails, G.test_name);
      }

    } else { // But failure always prints message
      VERB0("xexec done.  Result: %s  Fails: %d  Test name: %s",
            "FAILURE", G.local_fails + G.global_fails, G.test_name);
    }

    // Pavilion message
    if (G.options & OPT_PAVM) {
      printf("<result> %s <<< xexec done.  Test name: %s  Fails: %d >>>\n",
             (G.local_fails + G.global_fails) ? "fail" : "pass",
             G.test_name, G.local_fails + G.global_fails);
    }
  }

  rc = (G.local_fails + G.global_fails) ? EXIT_FAILURE : EXIT_SUCCESS;

  #ifdef MPI
  if (rc != EXIT_SUCCESS && mpi_active()) {
    VERB0("MPI_abort due to error exit from main with MPI active rc: %d", rc);
    MPI_CK(MPI_Abort(G.mpi_comm, rc));
  }
  #endif // MPI

  return rc;
}

int xexec_act_add(GLOBAL *gptr, struct xexec_act_parse * parse_add, int n_add, 
                  module_init * init, void * state, module_help * help) {
  S.parse = REALLOCX(S.parse, sizeof(struct xexec_act_parse) * (S.parseN + n_add));

  for (int i=0; i<n_add; ++i) {
    S.parse[i+S.parseN] = parse_add[i];
    S.parse[i+S.parseN].state = state;
  }
  S.parseN += n_add;
  qsort(S.parse, S.parseN, sizeof(struct xexec_act_parse), parse_comp);
 
  for (int i=0; i<(S.parseN-1); ++i) {
    if (!strcmp(S.parse[i].cmd, S.parse[i+1].cmd)) ERRX("Duplicate action name: %s", S.parse[i].cmd);      
  } 
 
  S.mods = REALLOCX(S.mods, sizeof(struct mod_table) * ++S.modsN);
  S.mods[S.modsN-1].init = init;
  S.mods[S.modsN-1].help = help;
  S.mods[S.modsN-1].state = state;
  
  return 0;
}

// --- end of xexec.c ---
