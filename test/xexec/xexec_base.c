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
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <math.h>
#ifdef DLFCN
  #include <dlfcn.h>
#endif  // DLFCN

//----------------------------------------------------------------------------
// xexec base module - contains basic functions
//----------------------------------------------------------------------------
#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)

static char * help =
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
  "  o <count>     write <count> lines to stdout\n"
  "  e <count>     write <count> lines to stderr\n"
  "  s <seconds>   sleep for <seconds>\n"
  "  srr <seed>    seed random rank - seed RNG with <seed> mixed with rank (if MPI)\n"
  "  va <bytes>    allocate <bytes> of memory with malloc\n"
  "  vt <stride>   touch latest allocation every <stride> bytes\n"
  "  vw            write data pattern to latest allocation\n" 
  "  vr            read (and check) data pattern from latest allocation\n" 
  "  vf            free latest allocation\n"
  "  hx <min> <max> <blocks> <limit> <count>\n"
  "                Perform <count> malloc/touch/free cycles on memory blocks ranging\n"
  "                in size from <min> to <max>.  Allocate no more than <limit> bytes\n"
  "                in <blocks> separate allocations.  Sequence and sizes of\n"
  "                allocations are randomized.\n"
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
  #ifdef DLFCN
  "  dlo <name>    Issue dlopen for specified file name\n"
  "  dls <symbol>  Issue dlsym for specified symbol in most recently opened library\n"
  "  dlc           Issue dlclose for most recently opened library\n"
  #endif // DLFCN
  "  grep <regex>  <file>  Search <file> and print (verbose 1) matching lines [1]\n"
  "                <file> = \"@ENV\" searches environment\n"
  #ifdef __linux__
  "  dca           Display CPU Affinity\n"
  #endif
  "  k <signal>    raise <signal> (number)\n"
  "  x <status>    exit with <status>\n"
  "  segv          force SIGSEGV\n" 
;

struct memblk {            // memblk queue element
  size_t size;
  struct memblk * prev;
};

static struct base_state {
  int memcount;            // Number of entries in memblk queue
  struct memblk * memptr;  // memblk queue
  double * fp_nums;
  U64 flap_size;
  U64 fp_count;

  #ifdef DLFCN
    int dl_num;
    void * dl_handle[20];
  #endif
} base_state;

MODULE_INIT(xexec_base_init) {
  struct base_state * s = state;
  #ifdef DLFCN
    s->dl_num = -1;
  #endif
  return 0;
}

MODULE_HELP(xexec_base_help) {
  fprintf(file, "%s", help);
  return 0;
}

//----------------------------------------------------------------------------
// v, d (verbose, debug) action handlers
//----------------------------------------------------------------------------
#define S (* ((struct base_state *)(actionp->state)) )

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
// o, e (stdout, stderr) action handlers
//----------------------------------------------------------------------------
ACTION_RUN(stdout_run) {
  U64 line;
  for (line = 1; line <= V0.u; line++) {
    // Message padded to exactly 100 bytes long.
    MSG("action %-4u stdout line %-8lu of %-8lu %*s", A.actn + 1, line, V0.u, 34 - G.id_string_len, "");
  }
}

ACTION_RUN(stderr_run) {
  U64 line;
  for (line = 1; line <= V0.u; line++) {
    // Message padded to exactly 100 bytes long.
    MSGE("action %-4u stderr line %-8lu of %-8lu %*s", A.actn + 1, line, V0.u, 34 - G.id_string_len, "");
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
    seed += (G.myrank * PR231_200);
  #endif
  seed16v[0] = seed & 0xFFFF;
  seed16v[1] = seed >> 16;
  seed16v[2] = 0;
  DBG3("srr seed: %lld; calling seed48(0x%04hX 0x%04hX 0x%04hX)", seed, seed16v[2], seed16v[1], seed16v[0]);
  seed48(seed16v);
}

//----------------------------------------------------------------------------
// va, vt, vf (memory allocate, touch, free) action handlers
//----------------------------------------------------------------------------

ACTION_CHECK(va_check) {
  S.memcount++;
}

ACTION_CHECK(vtwr_check) {
  if (S.memcount <= 0) ERRX("%s: %s without corresponding va allocation", A.desc, A.action);
}

ACTION_CHECK(vf_check) {
  if (S.memcount-- <= 0) ERRX("%s: free without corresponding allocate", A.desc);
}


ACTION_RUN(va_run) {
  S.memcount++;
  size_t len = V0.u;
  struct memblk * p;
  DBG2("Calling malloc(%lld)", len);
  p = (struct memblk *)malloc(len);
  VERB3("malloc returns %p", p);
  if (p) {
    p->size = len;
    p->prev = S.memptr;
    S.memptr = p;
  } else {
    VERB0("mem_hand - Warning: malloc returned NULL");
    S.memcount--;
  }
}

ACTION_RUN(vt_run) {
  U64 stride = V0.u;
  char *p, *end_p1;
  ETIMER tmr;
  if (S.memcount > 0) {
    p = (char*)S.memptr + sizeof(struct memblk);
    end_p1 = p + S.memptr->size;
    DBG4("Touching memory at %p, length 0x%llx, stride: %lld", p, S.memptr->size, stride);
    ETIMER_START(&tmr);
    while (p < end_p1) {
      DBG5("touch S.memptr: %p memlen: 0x%llx: end_p1: %p p: %p", S.memptr, S.memptr->size, end_p1, p);
      *p = 'x';
      p += stride;
    }
    double delta_t = ETIMER_ELAPSED(&tmr);
    U64 count = S.memptr->size / stride;
    prt_mmmst(&G, (double)count/delta_t, "vt rate", "TpS");

  } else {
    VERB0("mem_hand - Warning: no memory allocation to touch");
  }
}

ACTION_RUN(vw_run) {
  if (S.memcount > 0) {
    ETIMER tmr;
    char * p = (char*)S.memptr + sizeof(struct memblk);
    size_t len = S.memptr->size - sizeof(struct memblk);
    if (! G.wbuf_ptr ) dbuf_init(&G,RAND22, 20 * 1024 * 1024); 
    DBG4("Writing memory at %p, length 0x%llx", p, len);
    void * expected = get_wbuf_ptr(&G, "vw", 0, (U64)p);
    ETIMER_START(&tmr);
    memcpy(p, expected, len);
    double delta_t = ETIMER_ELAPSED(&tmr);
    prt_mmmst(&G, (double)len/delta_t, "vw rate", "B/S");
  } else {
    VERB0("mem_hand - Warning: no memory allocation to write");
  }
}

ACTION_RUN(vr_run) {
  if (S.memcount > 0) {
    ETIMER tmr;
    char * p = (char*)S.memptr + sizeof(struct memblk);
    size_t len = S.memptr->size - sizeof(struct memblk);
    if (! G.wbuf_ptr ) dbuf_init(&G,RAND22, 20 * 1024 * 1024); 
    DBG4("Reading memory at %p, length 0x%llx", p, len);
    ETIMER_START(&tmr);
    if (G.options & OPT_RCHK) {
      int rc = check_read_data(&G, "vr", p, len, 0, (U64)p);
      if (rc) { 
        G.local_fails++;
      }
    } else {
      memcpy(G.rbuf_ptr, p, len);
    }
    double delta_t = ETIMER_ELAPSED(&tmr);
    prt_mmmst(&G, (double)len/delta_t, "vr rate", "B/S");
  } else {
    VERB0("mem_hand - Warning: no memory allocation to read");
  }
}

ACTION_RUN(vf_run) {
  if (S.memcount > 0) {
    struct memblk * p;
    p = S.memptr->prev;
    DBG2("Calling free(%p)", S.memptr);
    free(S.memptr);
    S.memptr = p;
    S.memcount--;
  } else {
    VERB0("mem_hand - Warning: no memory allocation to free");
  }
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
  size_t b;
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

  #ifdef __clang_analyzer__
    memset(stat, 0, sizeof(stat));
  #endif
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
// ni, nr, nf (floating point addition init, run, free) action handlers
//----------------------------------------------------------------------------
ACTION_CHECK(ni_check) {
  S.flap_size = V0.u;
  S.fp_count = V1.u;
  if (S.flap_size < 2) ERRX("%s; size must be at least 2", A.desc);
}

ACTION_CHECK(nr_check) {
  U64 rep = V0.u;
  U64 stride = V1.u;

  if (!S.flap_size) ERRX("%s; nr without prior ni", A.desc);
  if ((S.fp_count-1)%stride != 0) ERRX("%s; S.fp_count-1 must equal a multiple of stride", A.desc);
  if (rep<1) ERRX("%s; rep must be at least 1", A.desc);
}

ACTION_CHECK(nf_check) {
  if (!S.flap_size) ERRX("%s; nf without prior ni", A.desc);
  S.flap_size = 0;
}

ACTION_RUN(ni_run) {
  S.flap_size = V0.u;
  S.fp_count = V1.u;
  U64 N = S.flap_size * S.fp_count;

  int rc = posix_memalign((void * *)&S.fp_nums, 4096, N * sizeof(double));
  if (rc) ERRX("%s; posix_memalign %d doubles failed: %s", A.desc, N, strerror(rc));

  U64 iv = 0;
  for (U64 i=0; i<N; ++i) {
    if (i%S.flap_size != 0) {
      S.fp_nums[i] = (double) ++iv;
      DBG4("%s; S.fp_nums[%d] = %d", A.desc, i, iv);
    }
  }
}

ACTION_RUN(nr_run) {
  double sum, delta_t, predicted;
  U64 b, ba, r, d, fp_add_ct, max_val;
  U64 N = S.flap_size * S.fp_count;
  U64 rep = V0.u;
  U64 stride = V1.u;
  ETIMER tmr;

  max_val = (S.flap_size-1) * S.fp_count;
  predicted = (pow((double) max_val, 2.0) + (double) max_val ) / 2 * (double)rep;
  DBG1("%s; v: %d predicted: %f", A.desc, max_val, predicted);
  fp_add_ct = (max_val * rep) + S.fp_count;

  for (U64 i=0; i<N; i+=S.flap_size) {
    S.fp_nums[i] = 0.0;
      DBG3("%s; S.fp_nums[%d] = %d", A.desc, i, 0);
  }

  DBG1("flapper starting; size: %llu S.fp_count: %llu rep: %llu stride: %llu", S.flap_size, S.fp_count, rep, stride);
  ETIMER_START(&tmr);

  for (b=0; b<S.fp_count; ++b) {
    ba = b * stride % S.fp_count;
    U64 d_sum = ba*S.flap_size;
    U64 d_first = d_sum + 1;
    U64 d_lastp1 = (ba+1)*S.flap_size;
    DBG3("b: %llu ba:%llu", b, ba);
    for (r=0; r<rep; ++r) {
      sum = S.fp_nums[d_sum];
      for (d=d_first; d<d_lastp1; ++d) {
        sum += S.fp_nums[d];
        DBG3("%s; val: %f sum: %f", A.desc, S.fp_nums[d], sum)
      }
      S.fp_nums[d_sum] = sum;
    }
  }

  sum = 0.0;
  for (d=0; d<S.fp_count*S.flap_size; d+=S.flap_size) {
    sum += S.fp_nums[d];
  }

  delta_t = ETIMER_ELAPSED(&tmr);

  VERB2("flapper done; FP Adds %llu, predicted: %e sum: %e delta: %e", fp_add_ct, predicted, sum, sum - predicted);
  VERB2("FP Adds: %llu, time: %f Seconds, MFLAPS: %e", fp_add_ct, delta_t, (double)fp_add_ct / delta_t / 1000000.0);
  prt_mmmst(&G, delta_t, " Add time", "S");
  prt_mmmst(&G, (double)fp_add_ct / delta_t, "FLAP rate", "F/S" );
}

ACTION_RUN(nf_run) {
  S.flap_size = 0;
  FREEX(S.fp_nums);
}

//----------------------------------------------------------------------------
// dlo, dls, dlc (dl open, sym, close) action handlers
//----------------------------------------------------------------------------
#ifdef DLFCN
ACTION_CHECK(dlo_check) {
  if (++(S.dl_num) >= (int)DIM1(S.dl_handle)) ERRX("%s; too many dlo commands, limit is %d", A.desc, DIM1(S.dl_handle));
}

ACTION_CHECK(dls_check) {
  if (S.dl_num < 0) ERRX("%s; o currently open dynamic library", A.desc);
}

ACTION_CHECK(dlc_check) {
  if (S.dl_num-- < 0) ERRX("%s; no currently open dynamic library", A.desc);
}

ACTION_RUN(dlo_run) {
  char * name = V0.s;
  S.dl_handle[++S.dl_num] = dlopen(name, RTLD_NOW);
  VERB3("%s; dlopen(%s) returns %p", A.desc, name, S.dl_handle[S.dl_num]);
  if (!S.dl_handle[S.dl_num]) {
    VERB0("%s; dlopen failed: %s", A.desc, dlerror());
    S.dl_num--;
  }
}

ACTION_RUN(dls_run) {
  char * symbol = V0.s;
  char * error;
  void * sym = dlsym(S.dl_handle[S.dl_num], symbol);
  VERB3("%s; dlsym(%s) returns %p", A.desc, symbol, sym);
  error = dlerror();
  if (error) VERB0("%s; dlsym error: %s", A.desc, error);
}

ACTION_RUN(dlc_run) {
  int rc = dlclose(S.dl_handle[S.dl_num--]);
  VERB3("%s; dlclose returns %d", A.desc, rc);
  if (rc) VERB0("%s; dlclose error: %s", A.desc, dlerror());
}
#endif // DLFC

//----------------------------------------------------------------------------
// grep action handler
//----------------------------------------------------------------------------
extern char * * environ;

ACTION_RUN(grep_run) {
  char * fname = V1.s;

  if (0 == strcmp(fname, "@ENV")) {
    for (char ** eptr = environ; *eptr; eptr++) {
      if (!rx_run(&G, 0, actionp, *eptr)) {
        VERB1("grep: %s", *eptr);
      }
    }
  } else {
    char line[512];
    FILE * f = fopen(fname, "r");
    if (!f) ERRX("%s: error opening \"%s\" %s", A.desc, fname, strerror(errno));
    while (fgets(line, sizeof(line), f)) {
      if (!rx_run(&G, 0, actionp, line)) {
        char * last = line + strlen(line) - 1;
        if ('\n' == *last) *last = '\0';
        VERB1("grep: %s", line);
      }
    }
    fclose(f);
  }
}

//----------------------------------------------------------------------------
// dca action handlers
//----------------------------------------------------------------------------
#ifdef __linux__
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
  #ifndef __clang_analyzer__
    * (volatile char *) 0 = '\0';
  #endif
}

//----------------------------------------------------------------------------
// xexec_base - init foundational commands
//----------------------------------------------------------------------------
MODULE_INSTALL(xexec_base_install) {
  struct xexec_act_parse parse[] = {
  // Command   V0    V1    V2    V3    V4     Check          Run
    {"v",     {UINT, NONE, NONE, NONE, NONE}, verbose_check, verbose_run },
    {"d",     {UINT, NONE, NONE, NONE, NONE}, debug_check,   debug_run   },
    {"o",     {UINT, NONE, NONE, NONE, NONE}, NULL,          stdout_run  },
    {"e",     {UINT, NONE, NONE, NONE, NONE}, NULL,          stderr_run  },
    {"s",     {DOUB, NONE, NONE, NONE, NONE}, sleep_check,   sleep_run   },
    {"srr",   {SINT, NONE, NONE, NONE, NONE}, NULL,          srr_run     },
    {"va",    {UINT, NONE, NONE, NONE, NONE}, va_check,      va_run      },
    {"vt",    {PINT, NONE, NONE, NONE, NONE}, vtwr_check,    vt_run      },
    {"vw",    {NONE, NONE, NONE, NONE, NONE}, vtwr_check,    vw_run      },
    {"vr",    {NONE, NONE, NONE, NONE, NONE}, vtwr_check,    vr_run      },
    {"vf",    {NONE, NONE, NONE, NONE, NONE}, vf_check,      vf_run      },
    {"hx",    {UINT, UINT, UINT, UINT, UINT}, hx_check,      hx_run      },
    {"ni",    {UINT, PINT, NONE, NONE, NONE}, ni_check,      ni_run      },
    {"nr",    {PINT, PINT, NONE, NONE, NONE}, nr_check,      nr_run      },
    {"nf",    {NONE, NONE, NONE, NONE, NONE}, nf_check,      nf_run      },
    #ifdef DLFCN
    {"dlo",   {STR,  NONE, NONE, NONE, NONE}, dlo_check,     dlo_run     },
    {"dls",   {STR,  NONE, NONE, NONE, NONE}, dls_check,     dls_run     },
    {"dlc",   {NONE, NONE, NONE, NONE, NONE}, dlc_check,     dlc_run     },
    #endif // DLFCN
    {"grep",  {REGX, STR,  NONE, NONE, NONE}, NULL,          grep_run    },
    #ifdef __linux__
    {"dca",   {NONE, NONE, NONE, NONE, NONE}, NULL,          dca_run     },
    #endif // __linux__
    {"k",     {UINT, NONE, NONE, NONE, NONE}, NULL,          raise_run   },
    {"x",     {UINT, NONE, NONE, NONE, NONE}, NULL,          exit_run    },
    {"segv",  {NONE, NONE, NONE, NONE, NONE}, NULL,          segv_run    },
  };

  xexec_act_add(&G, parse, DIM1(parse), xexec_base_init, &base_state, xexec_base_help);

  return 0;
}

