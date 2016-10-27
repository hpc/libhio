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
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

//----------------------------------------------------------------------------
// xexec fio module - contains various posix file I/O drivers and tests
//----------------------------------------------------------------------------
#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)

static char * help =
  "  dbuf RAND22 | RAND22P | OFS20  <size>  Allocates and initializes data buffer\n"
  "                with pattern used for read data check.  Write and read patterns\n"
  "                must match.\n"
  "  imd <path> <mode> Issue mkdir(path, mode)\n"
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
;

static struct fio_state {
  int fio_o_count;
  char * fio_path;
  FILE * fio_file;
  U64 fio_ofs;
  U64 fio_id_hash;
  U64 fio_rw_count[2]; // read, write byte counts
  ETIMER fio_foc_tmr;
  double fio_o_time, fio_w_time, fio_r_time, fio_exc_time, fio_c_time;
} fio_state;

MODULE_INIT(xexec_fio_init) {
  //struct fio_state * s = state;
  return 0;
}

MODULE_HELP(xexec_fio_help) {
  fprintf(file, "%s", help);
  return 0;
}

#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)
#define S (* ((struct fio_state *)(actionp->state)) )


//----------------------------------------------------------------------------
// dbuf action handler
//----------------------------------------------------------------------------
ACTION_CHECK(dbuf_check) {
  G.rwbuf_len = V1.u;
}

ACTION_RUN(dbuf_run) {
  dbuf_init(&G, (enum dbuf_type)V0.i, V1.u);
}

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
// fo, fw, fr, gc file access action handler
//----------------------------------------------------------------------------

// Substitute rank & PID for %r and %p in string.  Must be free'd by caller
char * str_sub(GLOBAL *gptr, char * string) {
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
          d += sprintf(d, "%.8d", (*s == 'p') ? getpid(): (mpi_active() ? G.myrank: 0) );
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

#define IF_ERRNO(COND, FMT, ...)                                                 \
  if ( (COND) ) {                                                                \
    VERB0(FMT " failed.  errno: %d (%s)", __VA_ARGS__, errno, strerror(errno));  \
    G.local_fails++;                                                               \
  }

ACTION_CHECK(fo_check) {
  if (S.fio_o_count++ != 0) ERRX("nested fo");
  if (G.rwbuf_len == 0) G.rwbuf_len = 20 * 1024 * 1024;
} 

ACTION_RUN(fo_run) {
  ETIMER local_tmr;
  S.fio_path = str_sub(&G, V0.s);
  char * mode = V1.s;

  S.fio_o_time = S.fio_w_time = S.fio_r_time = S.fio_exc_time = S.fio_c_time = 0;
  S.fio_ofs = 0;

  if (! G.wbuf_ptr ) dbuf_init(&G,RAND22, 20 * 1024 * 1024); 
  S.fio_id_hash = get_data_object_hash(&G, S.fio_path);
  IF_MPI( MPI_CK(MPI_Barrier(G.mpi_comm)) );

  ETIMER_START(&S.fio_foc_tmr);
  ETIMER_START(&local_tmr);
  S.fio_file = fopen(S.fio_path, mode);
  IF_ERRNO( !S.fio_file, "fo: fopen(%s, %s)", S.fio_path, mode );
  S.fio_rw_count[0] = S.fio_rw_count[1] = 0;
} 

ACTION_CHECK(fw_check) {
  U64 size = V1.u;
  if (S.fio_o_count != 1) ERRX("fw without preceeding fo");
  if (size > G.rwbuf_len) ERRX("%s; size > G.rwbuf_len", A.desc);
} 
  

ACTION_RUN(fw_run) {
  ssize_t len_act;
  I64 ofs_param = V0.u;
  ssize_t len_req = V1.u;
  U64 ofs_abs;
  ETIMER local_tmr;

  ofs_abs = S.fio_ofs + ofs_param;
  DBG2("fw ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", S.fio_ofs, ofs_param, ofs_abs, len_req);
  void * expected = get_wbuf_ptr(&G, "fw", ofs_abs, S.fio_id_hash);
  ETIMER_START(&local_tmr);

  if (S.fio_ofs != ofs_abs) {
    int rc = fseeko(S.fio_file, ofs_abs, SEEK_SET); 
    IF_ERRNO( rc, "fw: fseek(%s, %lld, SEEK_SET)", S.fio_path, ofs_abs);
    S.fio_ofs = ofs_abs;
  }

  len_act = fwrite_rd(S.fio_file, expected, len_req);
  S.fio_w_time += ETIMER_ELAPSED(&local_tmr);
  S.fio_ofs += len_act;
  IF_ERRNO( len_act != len_req, "fw: fwrite(%s, , %lld) len_act: %lld", S.fio_path, len_req, len_act);
  S.fio_rw_count[1] += len_act;
}

ACTION_CHECK(fr_check) {
  U64 size = V1.u;
  if (S.fio_o_count != 1) ERRX("fr without preceeding fo");
  if (size > G.rwbuf_len) ERRX("%s; size > G.rwbuf_len", A.desc);
}
 
ACTION_RUN(fr_run) {
  ssize_t len_act;
  I64 ofs_param = V0.u;
  ssize_t len_req = V1.u;
  U64 ofs_abs;
  ETIMER local_tmr;

  ofs_abs = S.fio_ofs + ofs_param;
  DBG2("fr ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", S.fio_ofs, ofs_param, ofs_abs, len_req);

  ETIMER_START(&local_tmr);

  if (S.fio_ofs != ofs_abs) {
    int rc = fseeko(S.fio_file, ofs_abs, SEEK_SET); 
    IF_ERRNO( rc, "fr: fseek(%s, %lld, SEEK_SET)", S.fio_path, ofs_abs);
    S.fio_ofs = ofs_abs;
  }

  len_act = fread_rd(S.fio_file, G.rbuf_ptr, len_req);
  S.fio_r_time += ETIMER_ELAPSED(&local_tmr);
  S.fio_ofs += len_act;
  IF_ERRNO( len_act != len_req, "fr: fread(%s, , %lld) len_act: %lld", S.fio_path, len_req, len_act);
  
  if (G.options & OPT_RCHK) {
    ETIMER_START(&local_tmr);
    // Force error for unit test
    // *(char *)(G.rbuf_ptr+16) = '\0';
    int rc = check_read_data(&G, "fread", G.rbuf_ptr, len_act, ofs_abs, S.fio_id_hash);
    if (rc) { 
      G.local_fails++;
    }
    S.fio_exc_time += ETIMER_ELAPSED(&local_tmr);
  }
  S.fio_rw_count[0] += len_act;
}

ACTION_CHECK(fc_check) {
  if (S.fio_o_count != 1) ERRX("fc without preceeding fo");
  S.fio_o_count--;
}

ACTION_RUN(fc_run) {
  int rc;
  ETIMER local_tmr;

  ETIMER_START(&local_tmr);
  rc = fclose(S.fio_file);
  S.fio_c_time += ETIMER_ELAPSED(&local_tmr);

  ETIMER_START(&local_tmr);
  IF_MPI( MPI_CK(MPI_Barrier(G.mpi_comm)) );
  double bar_time = ETIMER_ELAPSED(&local_tmr);
  double foc_time = ETIMER_ELAPSED(&S.fio_foc_tmr);

  IF_ERRNO( rc, "fc: fclose(%s)", S.fio_path);

  double una_time = foc_time - (S.fio_o_time + S.fio_w_time + S.fio_r_time +
                                S.fio_c_time + bar_time + S.fio_exc_time);

  char * desc = "         data check time"; 
  if (G.options & OPT_PERFXCHK) desc = "excluded data check time"; 

  if (G.options & OPT_XPERF) {  
    prt_mmmst(&G, S.fio_o_time,   "              fopen time", "S");
    prt_mmmst(&G, S.fio_w_time,   "             fwrite time", "S");
    prt_mmmst(&G, S.fio_r_time,   "              fread time", "S");
    prt_mmmst(&G, S.fio_c_time,   "             fclose time", "S");
    prt_mmmst(&G, bar_time,     "post fclose barrier time", "S");
    prt_mmmst(&G, S.fio_exc_time, desc,                       "S");
    prt_mmmst(&G, foc_time,     " fopen-fclose total time", "S");
    prt_mmmst(&G, una_time,     "    unaccounted for time", "S");
  }

  U64 rw_count_sum[2];
  IF_MPI(
    MPI_CK(MPI_Reduce(S.fio_rw_count, rw_count_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, G.mpi_comm))
  ) ELSE_MPI ( 
    rw_count_sum[0] = S.fio_rw_count[0];
    rw_count_sum[1] = S.fio_rw_count[1];
  )
  
  if (0 == G.myrank) {
    if (G.options & OPT_PERFXCHK) foc_time -= S.fio_exc_time;
    char b1[32], b2[32], b3[32], b4[32], b5[32];
    VERB1("fo-fc R/W Size: %s %s  Time: %s  R/W Speed: %s %s",
          eng_not(b1, sizeof(b1), (double)rw_count_sum[0],    "B6.4", "B"),
          eng_not(b2, sizeof(b2), (double)rw_count_sum[1],    "B6.4", "B"),
          eng_not(b3, sizeof(b3), (double)foc_time,           "D6.4", "S"),
          eng_not(b4, sizeof(b4), rw_count_sum[0] / foc_time, "B6.4", "B/S"),
          eng_not(b5, sizeof(b5), rw_count_sum[1] / foc_time, "B6.4", "B/S"));
  }        
  S.fio_path = FREEX(S.fio_path);
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

  if (0 == G.myrank) {
    for (U64 i=0; i<file_num; ++i) {
      char * fname = ALLOC_PRINTF("%s/test_%d", dir, i);
      FILE *f = fopen(fname, "w");
      if (!f) ERRX("%s: error opening \"%s\" %s", A.desc, fname, strerror(errno));
      U64 remain = file_sz;
      while (remain > 0) {
        U64 req_len = MIN(blk_sz, remain);
        DBG4("fctw: write %lld bytes to %s", req_len, fname);
        U64 act_len = fwrite(buf, 1, req_len, f);
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

  if (0 != G.myrank) {
    for (int i=file_num-1; i>=0; --i) {
      char * fname = ALLOC_PRINTF("%s/test_%d", dir, i);
      FILE *f = fopen(fname, "r");
      if (!f) ERRX("%s: error opening \"%s\" %s", A.desc, fname, strerror(errno));
      U64 remain = file_sz;
      while (remain > 0) {
        U64 req_len = MIN(blk_sz, remain);
        U64 act_len = fread(buf, 1, req_len, f);
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
// xexec_fio - init file I/O commands
//----------------------------------------------------------------------------
MODULE_INSTALL(xexec_fio_install) {
  struct xexec_act_parse parse[] = {
  // Command   V0    V1    V2    V3    V4     Check          Run
    {"dbuf",  {DBUF, UINT, NONE, NONE, NONE}, dbuf_check,    dbuf_run    },
    {"imd",   {STR,  SINT, NONE, NONE, NONE}, NULL,          imd_run     },
    {"fo",    {STR,  STR,  NONE, NONE, NONE}, fo_check,      fo_run      },
    {"fw",    {SINT, UINT, NONE, NONE, NONE}, fw_check,      fw_run      },
    {"fr",    {SINT, UINT, NONE, NONE, NONE}, fr_check,      fr_run      },
    {"fc",    {NONE, NONE, NONE, NONE, NONE}, fc_check,      fc_run      },
    {"fctw",  {STR,  UINT, UINT, UINT, NONE}, fct_check,     fctw_run    },
    {"fctr",  {STR,  UINT, UINT, UINT, NONE}, fct_check,     fctr_run    },
    {"fget",  {STR,  NONE, NONE, NONE, NONE}, NULL,          fget_run    },
  };

  xexec_act_add(&G, parse, DIM1(parse), xexec_fio_init, &fio_state, xexec_fio_help);

  return 0;
}

