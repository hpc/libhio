/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "xexec.h"
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#ifdef HIO // #endif at bottom of this file

//----------------------------------------------------------------------------
// xexec hio module - drive libHIO functions and DataWarp functions
//----------------------------------------------------------------------------
#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)

static char * help =
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
  "  heful         EXPERIMENTAL Local unlink of most recent element file\n"
  "  hefum <th_ct> EXPERIMENTAL Multi-threaded unlink of most recent element files from rank 0\n"
  "  hf            Fini\n"
  "  hxrc <rc_name|ANY> Expect non-SUCCESS rc on next HIO action\n"
  "  hxct <count>  Expect count != request on next R/W.  -999 = any count\n"
  "  hxdi <id> Expect dataset ID on next hdo\n"
  "  hvp <type regex> <name regex> Prints config and performance variables that match\n"
  "                the type and name regex's [1][2].  Types are two letter codes {c|p} {c|d|e}\n"
  "                where c|p is config or perf and c|d|e is context or dataset or element\n"
  "                \"hvp . .\" will print everything\n"
  "                \"hvp p total\" will print all perf vars containing \"total\" in the name\n"
  "  hvp2 <type regex> <name regex> Issues hio_print_vars with the specified\n"
  "                type and name regex's [1][2].  Types are two letter codes {c|d|e} {f|p}\n"
  "                where c|d|e is context or dataset or element and f|p is config or perf\n"
  "                \"hvp . .\" will print everything\n"
  "                \"hvp p total\" will print all perf vars containing \"total\" in the name\n"
  "  hvsc <name> <value>  Set hio context variable\n"
  "  hvsd <name> <value>  Set hio dataset variable\n"
  "  hvse <name> <value>  Set hio element variable\n"
  "  hvai <type> <name> <op> <integer value>  Assert hio integer variable\n"
  "  hvaf <type> <name> <op> <float value>    Assert hio float variable\n"
  "  hvai <type> <name> <op> <string value>   Assert hio integer variable\n"
  "                type is {c|p} {c|d|e} like hvp. op is {LT|LE|EQ|NE|GE|GT}\n"
  "                false compare is an error. Config variables are all strings\n"   
  "  hdsc <name> <expected> Issue hio_dataset_should_checkpont. Valid <expected> are:\n"
  "                  %s\n"
  #if HIO_USE_DATAWARP
  "  dwds <directory> Issue dw_wait_directory_stage\n"
  "  dsdo <dw_dir> <pfs_dir> <type> Issue dw_stage_directory_out\n"
  "  dwss <path> <size> <width> Issue dw_set_stripe_configuration; if file exists, must be\n"
  "                             empty, if not exist, file created, if directory exists, striping set\n"
  "  dwgs <path>   Issue dw_get_stripe_configuration, display results\n"
  "  dwmp <dw_root> <ppn> Issue dw_get_mds_path wuth <dw_root> int(rank/<ppn>).  Set env var\n"
  "                HIO_datawarp_root from result.\n"   
  #if DW_PH_2
  "  dwws <file>   Issue dw_wait_sync_complete\n"
  #endif // DW_PH_2
  #endif // HIO_USE_DATAWARP
;

static struct hio_state {
  hio_context_t context;
  hio_dataset_t dataset;
  hio_element_t element;
  char * hio_context_name;
  char * hio_dataset_name;
  I64 hio_ds_id_req;
  I64 hio_ds_id_act;
  I64 hio_ds_id_latest; // Most recent Dataset ID alloc'd
  hio_flags_t hio_dataset_flags;
  hio_dataset_mode_t hio_dataset_mode;
  char * hio_element_name;
  U64 hio_element_hash;
  hio_return_t hio_rc_exp;
  I64 hio_cnt_exp;
  I64 hio_dsid_exp;
  int hio_dsid_exp_set;
  int hio_fail;
  U64 hio_e_ofs;
  I64 hseg_start;
  U64 hio_rw_count[2];
  ETIMER hio_hdaf_tmr;
  double hio_hda_time, hio_hdo_time, hio_heo_time, hio_hew_time, hio_her_time,
       hio_hec_time, hio_hdc_time, hio_hdf_time, hio_exc_time;
} hio_state;

#define EL_HASH_MODULUS 65521     // A prime a little less than 2**16
#define HIO_CNT_REQ -998
#define HIO_CNT_ANY -999


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

#define HIO_ANY 999    // "special" hio rc value, means any rc OK
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


#define HIO_DATASET_ID_PREVIOUS -0x10000098  // "special" value, most recently used ID
#define HIO_DATASET_ID_NEXT -0x10000099      // "special" means 1 greater than previous ID

ENUM_START(etab_hdsi) // hio_dataset_id
ENUM_NAMP(HIO_DATASET_, ID_NEWEST)
ENUM_NAMP(HIO_DATASET_, ID_HIGHEST)
ENUM_NAMP(HIO_DATASET_, ID_PREVIOUS)
ENUM_NAMP(HIO_DATASET_, ID_NEXT)
ENUM_END(etab_hdsi, 0, NULL)

#define HIO_SCP_ANY -999  // "special" recommendation value, means any value OK
ENUM_START(etab_hcpr)    // hio_recommendation_t
ENUM_NAMP(HIO_SCP_, NOT_NOW)
ENUM_NAMP(HIO_SCP_, MUST_CHECKPOINT)
ENUM_NAMP(HIO_SCP_, ANY)
ENUM_END(etab_hcpr, 0, NULL)


#if HIO_USE_DATAWARP
ENUM_START(etab_dwst)  // DataWarp stage type
ENUM_NAME("IMMEDIATE", DW_STAGE_IMMEDIATE)
ENUM_NAME("JOB_END",   DW_STAGE_AT_JOB_END)
ENUM_NAME("REVOKE",    DW_REVOKE_STAGE_AT_JOB_END)
ENUM_NAME("ACTIVATE",  DW_ACTIVATE_DEFERRED_STAGE) 
ENUM_END(etab_dwst, 0, NULL)
#endif // HIO_USE_DATAWARP


MODULE_INIT(xexec_hio_init) {
  struct hio_state * s = state;
  s->hio_ds_id_latest = 0;
  s->hio_rc_exp = HIO_SUCCESS;
  s->hio_cnt_exp = HIO_CNT_REQ;
  s->hio_dsid_exp = -999;
  return 0;
}

MODULE_HELP(xexec_hio_help) {
  fprintf(file, help, enum_list(MY_MSG_CTX, &etab_hcpr));
  return 0;
}

#define G (*gptr)
#define MY_MSG_CTX (&G.xexec_msg_context)
#define S (* ((struct hio_state *)(actionp->state)) )

//----------------------------------------------------------------------------
// hi, hda, hdo, heo, hew, her, hec, hdc, hdf, hf (HIO) action handlers
//----------------------------------------------------------------------------

#define HRC_TEST(API_NAME)  {                                                                \
  G.local_fails += (S.hio_fail = (hrc != S.hio_rc_exp && S.hio_rc_exp != HIO_ANY) ? 1: 0);   \
  if (S.hio_fail || MY_MSG_CTX->verbose_level >= 3) {                                        \
    MSG("%s: " #API_NAME " %s; rc: %s exp: %s errno: %d(%s)", A.desc,                        \
         S.hio_fail ? "FAIL": "OK", enum_name(MY_MSG_CTX, &etab_herr, hrc),                  \
         enum_name(MY_MSG_CTX, &etab_herr, S.hio_rc_exp), errno, strerror(errno));           \
  }                                                                                          \
  if (hrc != HIO_SUCCESS) hio_err_print_all(S.context, stderr, "[" #API_NAME " error]");     \
  S.hio_rc_exp = HIO_SUCCESS;                                                                \
}

#define HCNT_TEST(API_NAME)  {                                                                       \
  if (HIO_CNT_REQ == S.hio_cnt_exp) S.hio_cnt_exp = hreq;                                            \
  G.local_fails += (S.hio_fail = ( hcnt != S.hio_cnt_exp && S.hio_cnt_exp != HIO_CNT_ANY ) ? 1: 0);  \
  if (S.hio_fail || MY_MSG_CTX->verbose_level >= 3) {                                                \
    MSG("%s: " #API_NAME " %s; cnt: %d exp: %d errno: %d(%s)", A.desc,                               \
         S.hio_fail ? "FAIL": "OK", hcnt, S.hio_cnt_exp, errno, strerror(errno));                    \
  }                                                                                                  \
  hio_err_print_all(S.context, stderr, "[" #API_NAME " error]");                                     \
  S.hio_cnt_exp = HIO_CNT_REQ;                                                                       \
}

ACTION_RUN(hi_run) {
  hio_return_t hrc;
  S.hio_context_name = V0.s;
  char * data_root = V1.s;
  char * root_var_name = "data_roots";

  IF_MPI(
    DBG2("Calling hio_init_mpi(&S.context, &G.mpi_comm, NULL, NULL, \"%s\")", S.hio_context_name);
    hrc = hio_init_mpi(&S.context, &G.mpi_comm, NULL, NULL, S.hio_context_name);
    HRC_TEST(hio_init_mpi) 
  ) ELSE_MPI(
    DBG2("Calling hio_init_single(&S.context, NULL, NULL, \"%s\")", S.hio_context_name);
    hrc = hio_init_single(&S.context, NULL, NULL, S.hio_context_name); 
    HRC_TEST(hio_init_single)
  ) 

  if (HIO_SUCCESS == hrc) {
    DBG2("Calling hio_config_set(S.context, \"%s\", \"%s\")", root_var_name, data_root);
    hrc = hio_config_set_value((hio_object_t)S.context, root_var_name, data_root);
    HRC_TEST(hio_config_set_value)
  }

  if (HIO_SUCCESS == hrc) {
    char * tmp_str = NULL;
    hrc = hio_config_get_value((hio_object_t)S.context, root_var_name, &tmp_str);
    HRC_TEST(hio_config_get_value)
    if (HIO_SUCCESS == hrc) {
      VERB3("hio_config_get_value var:%s value=\"%s\"", root_var_name, tmp_str);
    }
  }
}

ACTION_RUN(hda_run) {
  hio_return_t hrc = HIO_SUCCESS;
  ETIMER local_tmr;
  S.hio_dataset_name = V0.s;
  S.hio_dataset_flags = (enum hio_flags_t) V2.i;
  S.hio_dataset_mode = (enum hio_dataset_mode_t) V3.i;

  switch (V1.u) {
    case HIO_DATASET_ID_PREVIOUS:
      S.hio_ds_id_req = S.hio_ds_id_latest;
      break;
    case HIO_DATASET_ID_NEXT:
      S.hio_ds_id_req = S.hio_ds_id_latest + 1;
      break;
    default:
      S.hio_ds_id_req = V1.u;
      break;
  } 
  S.hio_ds_id_latest = S.hio_ds_id_req; 

  S.hio_rw_count[0] = S.hio_rw_count[1] = 0;
  IF_MPI(MPI_CK(MPI_Barrier( G.mpi_comm)));
  S.hio_hda_time = S.hio_hdo_time = S.hio_heo_time = S.hio_hew_time = S.hio_her_time =
                 S.hio_hec_time = S.hio_hdc_time = S.hio_hdf_time = S.hio_exc_time = 0.0;
  DBG2("hda_run: dataset: %p", S.dataset);
  DBG2("Calling hio_datset_alloc(S.context, &S.dataset, %s, %lld, %d(%s), %d(%s))", S.hio_dataset_name, S.hio_ds_id_req,
        S.hio_dataset_flags, V2.s, S.hio_dataset_mode, V3.s);
  ETIMER_START(&S.hio_hdaf_tmr);
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_alloc (S.context, &S.dataset, S.hio_dataset_name, S.hio_ds_id_req, S.hio_dataset_flags, S.hio_dataset_mode);
  S.hio_hda_time += ETIMER_ELAPSED(&local_tmr);
  DBG2("hda_run: dataset: %p", S.dataset);
  HRC_TEST(hio_dataset_alloc);
}

#include <inttypes.h>

ACTION_RUN(hdo_run) {
  hio_return_t hrc;
  ETIMER local_tmr;
  DBG2("calling hio_dataset_open(%p)", S.dataset);
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_open (S.dataset);
  S.hio_hdo_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_dataset_open);
  if (HIO_SUCCESS == hrc) {
    hrc = hio_dataset_get_id(S.dataset, &S.hio_ds_id_act);
    HRC_TEST(hio_dataset_get_id);
    G.local_fails += S.hio_fail = (S.hio_dsid_exp_set && S.hio_dsid_exp != S.hio_ds_id_act);
    if (S.hio_fail || MY_MSG_CTX->verbose_level >= 3) {
      if (S.hio_dsid_exp_set) {
        MSG("%s: hio_dataset_get_id %s actual: %" PRIi64 " exp: %" PRIi64, A.desc, S.hio_fail ? "FAIL": "OK",
            S.hio_ds_id_act, S.hio_dsid_exp);
      } else {
        MSG("%s: hio_dataset_get_id actual %"PRIi64, A.desc, S.hio_ds_id_act);
      }
    }
  }
  S.hio_dsid_exp = -999;
  S.hio_dsid_exp_set = 0;
}

ACTION_CHECK(heo_check) {
  if (G.rwbuf_len == 0) G.rwbuf_len = 20 * 1024 * 1024;
}

ACTION_RUN(heo_run) {
  hio_return_t hrc;
  ETIMER local_tmr;
  S.hio_element_name = V0.s;
  int flag_i = V1.i;
  ETIMER_START(&local_tmr);
  hrc = hio_element_open (S.dataset, &S.element, S.hio_element_name, flag_i);
  S.hio_heo_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_element_open)

  ETIMER_START(&local_tmr);
  if (! G.wbuf_ptr ) dbuf_init(&G, RAND22P, 20 * 1024 * 1024); 

  char * element_id = ALLOC_PRINTF("%s %s %d %s %d", S.hio_context_name, S.hio_dataset_name,
                                   S.hio_ds_id_act, S.hio_element_name,
                                   (HIO_SET_ELEMENT_UNIQUE == S.hio_dataset_mode) ? G.myrank: 0);
  S.hio_element_hash = get_data_object_hash(&G, element_id);
  //S.hio_element_hash = BDYDN(crc32(0, element_id, strlen(element_id)) % G.wbuf_data_object_hash_mod, G.wbuf_bdy);
  FREEX(element_id);

  S.hio_e_ofs = 0;
  S.hio_exc_time += ETIMER_ELAPSED(&local_tmr);
}

ACTION_RUN(hso_run) {
  S.hio_e_ofs = V0.u;
}

ACTION_CHECK(hew_check) {
  U64 size = V1.u;
  if (size > G.rwbuf_len) ERRX("%s; size > G.rwbuf_len", A.desc);
}

ACTION_RUN(hsega_run) {
  U64 start = V0.u;
  U64 size_per_rank = V1.u;
  U64 rank_shift = V2.u;
  S.hseg_start = start;
  S.hio_e_ofs = S.hseg_start + size_per_rank * ((G.myrank + rank_shift) % G.mpi_size);
  S.hseg_start += size_per_rank * G.mpi_size;
}

ACTION_RUN(hsegr_run) {
  U64 start = V0.u;
  U64 size_per_rank = V1.u;
  U64 rank_shift = V2.u;
  S.hseg_start += start;
  S.hio_e_ofs = S.hseg_start + size_per_rank * ((G.myrank + rank_shift) % G.mpi_size);
  S.hseg_start += size_per_rank * G.mpi_size;
}

ACTION_RUN(hew_run) {
  ssize_t hcnt;
  I64 ofs_param = V0.u;
  U64 hreq = V1.u;
  U64 ofs_abs;
  ETIMER local_tmr;

  ofs_abs = S.hio_e_ofs + ofs_param;
  DBG2("hew el_ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", S.hio_e_ofs, ofs_param, ofs_abs, hreq);
  S.hio_e_ofs = ofs_abs + hreq;
  void * expected = get_wbuf_ptr(&G, "hew", ofs_abs, S.hio_element_hash);
  ETIMER_START(&local_tmr);
  hcnt = hio_element_write (S.element, ofs_abs, 0, expected, 1, hreq);
  S.hio_hew_time += ETIMER_ELAPSED(&local_tmr);
  HCNT_TEST(hio_element_write)
  S.hio_rw_count[1] += hcnt;
}

// Randomize length, then call hew action handler
ACTION_RUN(hewr_run) {
  struct action new = *actionp;
  new.v[1].u = rand_range(V1.u, V2.u, V3.u);
  hew_run(&G, &new);
}

ACTION_CHECK(her_check) {
  U64 size = V1.u;
  if (size > G.rwbuf_len) ERRX("%s; size > G.rwbuf_len", A.desc);
}

char * get_hio_dw_path(GLOBAL * gptr, struct action * actionp, int rank) {
  // Return the HIO DataWarp physical file name; caller must free
  // This works for the current version of HIO in basic mode, no guarantees
   // going forward.
  char * retval = NULL;
  char * dw_path = getenv("DW_JOB_STRIPED");
  if (dw_path) {
    char path[512];
    //                                      root
    //                                         ctx    dsn
    //                                                   id                     el_name
    int len = snprintf(path, sizeof(path), "%s/%s.hio/%s/%"PRIi64"/element_data.%s",
                       dw_path, S.hio_context_name, S.hio_dataset_name,
                       S.hio_ds_id_act, S.hio_element_name);
    if (HIO_SET_ELEMENT_UNIQUE == S.hio_dataset_mode) {
      snprintf(path+len, sizeof(path)-len, ".%08d", rank);
    }
    retval = STRDUPX(path);
  } 
  return retval;
}

ACTION_RUN(her_run) {
  ssize_t hcnt;
  I64 ofs_param = V0.u;
  U64 hreq = V1.u;
  U64 ofs_abs;
  ETIMER local_tmr;

  ofs_abs = S.hio_e_ofs + ofs_param;
  DBG2("her el_ofs: %lld ofs_param: %lld ofs_abs: %lld len: %lld", S.hio_e_ofs, ofs_param, ofs_abs, hreq);
  S.hio_e_ofs = ofs_abs + hreq;
  ETIMER_START(&local_tmr);
  hcnt = hio_element_read (S.element, ofs_abs, 0, G.rbuf_ptr, 1, hreq);
  S.hio_her_time += ETIMER_ELAPSED(&local_tmr);
  HCNT_TEST(hio_element_read)
  S.hio_rw_count[0] += hcnt;
  
  if ((G.options & OPT_RCHK) && (hcnt >= 0)) {
    ETIMER_START(&local_tmr);
    // Force error for unit test
    //*(((char *)G.rbuf_ptr)+16) = '\0';
    int rc = check_read_data(&G, "hio_element_read", G.rbuf_ptr, hcnt, ofs_abs, S.hio_element_hash);
    if (rc) { 
      G.local_fails++;
      char * path = get_hio_dw_path(&G, actionp, G.myrank);
      if (path) {
        FILE * elf = fopen(path, "r");
        if (!elf) {
          VERB0("fopen(\"%s\", \"r\") failed, errno: %d", path, errno);
        } else {
          int rc = fseeko(elf, ofs_abs, SEEK_SET);
          if (rc != 0 ) {
            VERB0("fseek( , %llu, SEEK_SET) failed, errno: %d", ofs_abs, errno);
          } else {
            char * fbuf = MALLOCX(hreq);
            U64 count = fread_rd(elf, fbuf, hreq);
            if (count != hreq) VERB0("Warning: fread_rd count: %ld, len_req: %ld", count, hreq);
            VERB0("File %s at offset 0x%lX:", path, ofs_abs);
            hex_dump(fbuf, hreq);  
            FREEX(fbuf);
            rc = fclose(elf); 
            if (rc != 0) VERB0("fclose failed rc: %d errno: %d", rc, errno);
          }
        } 
      }
      FREEX(path);
    }
    S.hio_exc_time += ETIMER_ELAPSED(&local_tmr);
  }

}

// Randomize length, then call her action handler
ACTION_RUN(herr_run) {
  struct action new = *actionp;
  new.v[1].u = rand_range(V1.u, V2.u, V3.u);
  her_run(&G, &new);
}

ACTION_RUN(hec_run) {
  hio_return_t hrc;
  ETIMER local_tmr;
  ETIMER_START(&local_tmr);
  hrc = hio_element_close(&S.element);
  S.hio_hec_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_element_close)
}

#define GIGBIN (1024.0 * 1024.0 * 1024.0)

ACTION_RUN(hdc_run) {
  hio_return_t hrc;
  ETIMER local_tmr;
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_close(S.dataset);
  S.hio_hdc_time += ETIMER_ELAPSED(&local_tmr);
  HRC_TEST(hio_dataset_close)
}

ACTION_RUN(hdf_run) {
  hio_return_t hrc;
  ETIMER local_tmr;
  DBG3("Calling hio_dataset_free(%p); dataset: %p", &S.dataset, S.dataset);
  ETIMER_START(&local_tmr);
  hrc = hio_dataset_free(&S.dataset);
  S.hio_hdf_time += ETIMER_ELAPSED(&local_tmr);
  ETIMER_START(&local_tmr);
  IF_MPI(MPI_CK(MPI_Barrier(G.mpi_comm)));
  double bar_time = ETIMER_ELAPSED(&local_tmr);
  double hdaf_time = ETIMER_ELAPSED(&S.hio_hdaf_tmr);
  HRC_TEST(hio_dataset_close)
  U64 hio_rw_count_sum[2];
  IF_MPI( 
    MPI_CK(MPI_Reduce(S.hio_rw_count, hio_rw_count_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, G.mpi_comm));
  ) ELSE_MPI(
    hio_rw_count_sum[0] = S.hio_rw_count[0];
    hio_rw_count_sum[1] = S.hio_rw_count[1];
  )
  DBG3("After hio_dataset_free(); S.dataset: %p", S.dataset);
  IFDBG3( hex_dump(&S.dataset, sizeof(S.dataset)) );
  double una_time = hdaf_time - (S.hio_hda_time + S.hio_hdo_time + S.hio_heo_time + S.hio_hew_time + S.hio_her_time
                                 + S.hio_hec_time + S.hio_hdc_time + S.hio_hdf_time + bar_time + S.hio_exc_time);
 
  char * desc = "           data check time"; 
  if (G.options & OPT_PERFXCHK) desc = "  excluded data check time";
 
  if (G.options & OPT_XPERF) {  
    prt_mmmst(&G, S.hio_hda_time, " hio_dataset_allocate time", "S");
    prt_mmmst(&G, S.hio_hdo_time, "     hio_dataset_open time", "S");
    prt_mmmst(&G, S.hio_heo_time, "     hio_element_open time", "S");
    prt_mmmst(&G, S.hio_hew_time, "    hio_element_write time", "S");
    prt_mmmst(&G, S.hio_her_time, "     hio_element_read time", "S");
    prt_mmmst(&G, S.hio_hec_time, "    hio_element_close time", "S");
    prt_mmmst(&G, S.hio_hdc_time, "    hio_dataset_close time", "S");
    prt_mmmst(&G, S.hio_hdf_time, "     hio_dataset_free time", "S");
    prt_mmmst(&G, bar_time,     "    post free barrier time", "S");
    prt_mmmst(&G, S.hio_exc_time, desc ,                        "S");
    prt_mmmst(&G, hdaf_time,    "        hda-hdf total time", "S");
    prt_mmmst(&G, una_time,     "      unaccounted for time", "S");
  }

  if (G.myrank == 0) {
    if (G.options & OPT_PERFXCHK) hdaf_time -= S.hio_exc_time;
    char b1[32], b2[32], b3[32], b4[32], b5[32];
    VERB1("hda-hdf R/W Size: %s %s  Time: %s  R/W Speed: %s %s",
          eng_not(b1, sizeof(b1), (double)hio_rw_count_sum[0],     "B6.4", "B"),
          eng_not(b2, sizeof(b2), (double)hio_rw_count_sum[1],     "B6.4", "B"),
          eng_not(b3, sizeof(b3), (double)hdaf_time,           "D6.4", "S"),
          eng_not(b4, sizeof(b4), hio_rw_count_sum[0] / hdaf_time, "B6.4", "B/S"),
          eng_not(b5, sizeof(b5), hio_rw_count_sum[1] / hdaf_time, "B6.4", "B/S"));
    
    if (G.options & OPT_PAVM) {
      printf("<td> Read_speed %f GiB/S\n", hio_rw_count_sum[0] / hdaf_time / GIGBIN );
      printf("<td> Write_speed %f GiB/S\n", hio_rw_count_sum[1] / hdaf_time / GIGBIN );
    }
  }
}

ACTION_RUN(hdu_run) {
  hio_return_t hrc;
  char * name = V0.s;
  U64 id;

  switch (V1.u) {
    case HIO_DATASET_ID_PREVIOUS:
      id = S.hio_ds_id_latest;
      break;
    case HIO_DATASET_ID_NEXT:
      id = S.hio_ds_id_latest + 1;
      break;
    default:
      id = V1.u;
      break;
  } 
  S.hio_ds_id_latest = id; 

  hio_unlink_mode_t ulm = (enum hio_unlink_mode_t) V2.i;
  hrc = hio_dataset_unlink(S.context, name, id, ulm); 
  HRC_TEST(hio_dataset_unlink)
}

ACTION_RUN(heful_run) {
  char * path = get_hio_dw_path(&G, actionp, G.myrank);
  if (path) {
    ETIMER local_tmr;
    ETIMER_START(&local_tmr);
    int rc = unlink(path);
    double unl_time = ETIMER_ELAPSED(&local_tmr);
    if (rc) {
      G.local_fails++;
      VERB0("unlink(%s) rc: %d, errno: %d(%s)", path, rc, errno, strerror(errno));
    }
    prt_mmmst(&G, unl_time,"heful local unlink time", "S");
    FREEX(path);
  }
}

struct thparm {
  pthread_t thread;
  GLOBAL * gptr;
  struct action * actionp;
  int rank_first;
  int rank_stride;
};

void * unlink_file(void * vtp) {
  struct thparm * tp = (struct thparm *) vtp;
  int retval = 0;
  for (int rank = tp->rank_first; rank < tp->gptr->mpi_size; rank += tp->rank_stride) {
    char * path = get_hio_dw_path(tp->gptr, tp->actionp, rank);
    if (path) {
      int rc = unlink(path);
      if (rc) {
        printf("unlink(%s) rc: %d errno: %d(%s)\n", (char *) path, rc, errno, strerror(errno));
        retval = errno;
      } 
      free(path);
    }
  }
  return (void *)(I64)retval;
}

ACTION_RUN(hefum_run) {
  if (G.myrank == 0) {
    int tnum = V0.u;
    ETIMER local_tmr;
    ETIMER_START(&local_tmr);
    struct thparm tp[tnum];
    for (int i=0; i<tnum; ++i) {
      tp[i].gptr = &G;
      tp[i].actionp = actionp;
      tp[i].rank_first = i;
      tp[i].rank_stride = tnum;
      int rc = pthread_create(&tp[i].thread, NULL, unlink_file, &tp[i]);
      if (rc) ERRX("ptread_create rc: %d(%s)", rc, strerror(rc));
    }
    for (int i=0; i<tnum; ++i) {
      void * trc;
      int rc = pthread_join(tp[i].thread, &trc);
      if (rc || trc) ERRX("pthread_join rc: %d trc: %p", rc, trc);
    } 
    double unl_time = ETIMER_ELAPSED(&local_tmr);
    VERB1("hefum threads: %d unlink time: %f S", tnum, unl_time);
  } 
}

ACTION_RUN(hf_run) {
  hio_return_t hrc;
  hrc = hio_fini(&S.context);
  HRC_TEST(hio_fini);
}

ACTION_RUN(hxrc_run) {
  S.hio_rc_exp = (enum hio_return_t) V0.i;
  VERB3("%s; HIO expected rc now %s(%d)", A.desc, V0.s, V0.i);
}

ACTION_CHECK(hxct_check) {
  I64 count = V0.u;
  if (count < 0 && count != HIO_CNT_ANY && count != HIO_CNT_REQ)
    ERRX("%s; count negative and not %d (ANY) or %d (REQ)", A.desc, HIO_CNT_ANY, HIO_CNT_REQ);
}

ACTION_RUN(hxct_run) {
  S.hio_cnt_exp = V0.u;
  VERB3("%s; HIO expected count now %lld", A.desc, V0.u);
}

ACTION_RUN(hxdi_run) {
  S.hio_dsid_exp = V0.u;
  S.hio_dsid_exp_set = 1;
  VERB3("%s; HIO expected dataset id now %lld", A.desc, S.hio_dsid_exp);
}

//----------------------------------------------------------------------------
// hvp action handlers
//----------------------------------------------------------------------------
void pr_cfg(GLOBAL * gptr, hio_object_t object, char * obj_name, struct action * actionp) {
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
      if (!rx_run(&G, 1, actionp, name)) {
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

union config_type {                // Union member names match hio_config_type_t names
  bool BOOL;
  char STRING[512];
  I32 INT32;
  U32 UINT32;
  I64 INT64;
  U64 UINT64;
  float FLOAT;
  double DOUBLE;
};

void pr_perf(GLOBAL * gptr, hio_object_t object, char * obj_name, struct action * actionp) {
  hio_return_t hrc;
  int count;
  hrc = hio_perf_get_count((hio_object_t) object, &count);
  HRC_TEST("hio_perf_get_count");
  DBG3("hio_perf_get_count %s returns count: %d", obj_name, count);
  for (int i = 0; i< count; i++) {
    char * name;
    hio_config_type_t type;
    union config_type value;

    hrc = hio_perf_get_info((hio_object_t) object, i, &name, &type);
    HRC_TEST("hio_perf_get_info");
    if (HIO_SUCCESS == hrc) {
      if (!rx_run(&G, 1, actionp, name)) {
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

ACTION_RUN(hvp_run) {
  R0_OR_VERB_START
    if (!rx_run(&G, 0, actionp, "cc") && S.context)  pr_cfg(gptr, (hio_object_t)S.context, "Context", actionp);
    if (!rx_run(&G, 0, actionp, "cd") && S.dataset)  pr_cfg(gptr, (hio_object_t)S.dataset, "Dataset", actionp);
    if (!rx_run(&G, 0, actionp, "ce") && S.element)  pr_cfg(gptr, (hio_object_t)S.element, "Element", actionp);
    if (!rx_run(&G, 0, actionp, "pc") && S.context) pr_perf(gptr, (hio_object_t)S.context, "Context", actionp);
    if (!rx_run(&G, 0, actionp, "pd") && S.dataset) pr_perf(gptr, (hio_object_t)S.dataset, "Dataset", actionp);
    if (!rx_run(&G, 0, actionp, "pe") && S.element) pr_perf(gptr, (hio_object_t)S.element, "Element", actionp);
  R0_OR_VERB_END
}

ACTION_RUN(hvp2_run) {
  hio_return_t hrc;
  char time_str[64];
  time_t now;
  char tmp_id[128];
  hio_object_t obj;

  now = time(0);
  strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", localtime(&now));

  strncpy(tmp_id, G.id_string, sizeof(tmp_id) - 1);
  tmp_id[sizeof(tmp_id)-1] = '\0';
  tmp_id[strlen(tmp_id)-1] = '\0';

       if (S.element) obj = (hio_object_t) S.element;
  else if (S.dataset) obj = (hio_object_t) S.dataset;
  else if (S.context) obj = (hio_object_t) S.context;
  else ERRX("%s; hvp2 - HIO not ininitialized", A.desc);

  hrc = hio_print_vars(obj, V0.s, V1.s, stdout, "%s %s", time_str, tmp_id);
  HRC_TEST("hio_print_vars");
  //hio_err_print_all(S.context, stdout, "hio_print_vars[hpa]");
}

ACTION_RUN(hvsc_run) {
  hio_return_t hrc;
  if (!S.context) ERRX("%s: hio context not established", A.desc);
  hrc = hio_config_set_value((hio_object_t) S.context, V0.s, V1.s);
  HRC_TEST("hio_config_set_value");
}

ACTION_RUN(hvsd_run) {
  hio_return_t hrc;
  if (!S.dataset) ERRX("%s: hio dataset not open", A.desc);
  hrc = hio_config_set_value((hio_object_t) S.dataset, V0.s, V1.s);
  HRC_TEST("hio_config_set_value");
}

ACTION_RUN(hvse_run) {
  hio_return_t hrc;
  if (!S.element) ERRX("%s: hio element not open", A.desc);
  hrc = hio_config_set_value((hio_object_t) S.element, V0.s, V1.s);
  HRC_TEST("hio_config_set_value");
}

//----------------------------------------------------------------------------
// hva{i|f|s} HIO value assert routines
// Note: due to HIO currently only utilizing integer data types for perf vars,
//       other data have not been tested.
//----------------------------------------------------------------------------
enum hva_type { // HIO variable assert type
  HVAT_cc,      // Config - context
  HVAT_cd,      // Config - dataset
  HVAT_ce,      // Config - element
  HVAT_pc,      // Perf - context
  HVAT_pd,      // Perf - dataset
  HVAT_pe       // Perf - element
};

ENUM_START(etab_hvat) // hva_type
ENUM_NAMP(HVAT_, cc)
ENUM_NAMP(HVAT_, cd)
ENUM_NAMP(HVAT_, ce)
ENUM_NAMP(HVAT_, pc)
ENUM_NAMP(HVAT_, pd)
ENUM_NAMP(HVAT_, pe)
ENUM_END(etab_hvat, 0, NULL)

enum hva_op {  // HIO variable assert operator 
  HVAO_LT,     // Less than
  HVAO_LE,     // Less than or equal
  HVAO_EQ,     // Equal
  HVAO_GE,     // Greater than or equal
  HVAO_GT,     // Greater than
  HVAO_NE      // Not equal
};

ENUM_START(etab_hvao) // hva_op
ENUM_NAMP(HVAO_, LT)
ENUM_NAMP(HVAO_, LE)
ENUM_NAMP(HVAO_, EQ)
ENUM_NAMP(HVAO_, GE)
ENUM_NAMP(HVAO_, GT)
ENUM_NAMP(HVAO_, NE)
ENUM_END(etab_hvao, 0, NULL)

hio_return_t get_perf_type(GLOBAL * gptr, hio_object_t obj, char * name, hio_config_type_t *type, struct action * actionp) {
  hio_return_t hrc;
  int count, i;
  char * vname;
  hio_config_type_t vtype;

  hrc = hio_perf_get_count(obj, &count); 
  DBG4("hio_perf_get_count count:%d rc:%d", count, hrc); 
  HRC_TEST("hio_perf_get_count");
  if (HIO_SUCCESS == hrc) {
  for (i=0; i<count; ++i) {
      hrc = hio_perf_get_info(obj, i, &vname, &vtype);
      DBG4("hio_perf_get_info i: %d name: %s type: %s rc: %d", i, vname, enum_name(MY_MSG_CTX, &etab_hcfg, vtype), hrc); 
      HRC_TEST("hio_perf_get_info");
      if (HIO_SUCCESS == hrc && !strcmp(vname, name)) {
        *type = vtype;
        return hrc;
      }
    }
  }
  return HIO_ERR_NOT_FOUND;
}    
    
ACTION_CHECK(hvaif_check) {
  enum hva_type type = (enum hva_type)V0.i;
  if ( HVAT_cc == type || HVAT_cd == type || HVAT_ce == type ) {
    ERRX("%s: Configuration variable is not comparable with integer or float", A.desc);
  }
}  


ACTION_RUN(hvai_run) {
  enum hva_type type = (enum hva_type)V0.i;
  char * name = V1.s;
  enum hva_op op = (enum hva_op)V2.i;
  I64 val = V3.u;
  hio_return_t hrc = HIO_SUCCESS;
  hio_object_t obj = NULL;
  hio_config_type_t vtype;
  union config_type pval;
  I64 vi64 = 0;

  DBG4("hvai: type: %s name: %s op: %s val: %lld", enum_name(MY_MSG_CTX, &etab_hvat, type),
        name, enum_name(MY_MSG_CTX, &etab_hvao, op), val);
 
       if (HVAT_pc == type) obj = (hio_object_t) S.context;
  else if (HVAT_pd == type) obj = (hio_object_t) S.dataset;
  else if (HVAT_pe == type) obj = (hio_object_t) S.element;
  else ERRX("%s: internal error: invalid type %d", A.desc, type);

  if (!obj) ERRX("%s: object not valid (type: %s)", A.desc, enum_name(MY_MSG_CTX, &etab_hvat, type));
 
  hrc = get_perf_type(gptr, obj, name, &vtype, actionp);
  HRC_TEST("get_perf_type");
  if (HIO_SUCCESS == hrc) {
    hrc = hio_perf_get_value(obj, name, &pval, sizeof(pval));
    HRC_TEST("hio_get_perf_value");
    if (HIO_SUCCESS == hrc) {
      switch(vtype) {
        case HIO_CONFIG_TYPE_BOOL:
          vi64 = pval.BOOL ? 1 : 0;
          break;
        case HIO_CONFIG_TYPE_INT32:
          vi64 = (I64) pval.INT32; 
          break;
        case HIO_CONFIG_TYPE_UINT32:
          vi64 = (I64) pval.INT32; 
          break;
        case HIO_CONFIG_TYPE_INT64:
          vi64 = (I64) pval.INT32; 
          break;
        case HIO_CONFIG_TYPE_UINT64:
          vi64 = (I64) pval.UINT64; 
          break;
        case HIO_CONFIG_TYPE_STRING:
        case HIO_CONFIG_TYPE_FLOAT:
        case HIO_CONFIG_TYPE_DOUBLE:
          MSG("%s: Perf variable %s is %s, not comparable with integer",
              A.desc, name, enum_name(MY_MSG_CTX, &etab_hcfg, vtype)); 
          hrc = HIO_ERR_BAD_PARAM;
          G.local_fails += S.hio_fail = 1;
          break;
      }
    }
    if (HIO_SUCCESS == hrc) {
      switch(op) {
        case HVAO_LT: if ( !(vi64 <  val) ) S.hio_fail = 1; break;
        case HVAO_LE: if ( !(vi64 <= val) ) S.hio_fail = 1; break;
        case HVAO_EQ: if ( !(vi64 == val) ) S.hio_fail = 1; break;
        case HVAO_GE: if ( !(vi64 >= val) ) S.hio_fail = 1; break;
        case HVAO_GT: if ( !(vi64 >  val) ) S.hio_fail = 1; break;
        case HVAO_NE: if ( !(vi64 != val) ) S.hio_fail = 1; break;
      }
      G.local_fails += S.hio_fail;
      if (S.hio_fail || MY_MSG_CTX->verbose_level >= 2) {
        MSG("%s: Assert %s.  Actual value: %lld", A.desc, S.hio_fail ? "failed" : "succeeded", vi64);
      }
    }
  }
}

ACTION_RUN(hvaf_run) {
  enum hva_type type = (enum hva_type)V0.i;
  char * name = V1.s;
  enum hva_op op = (enum hva_op)V2.i;
  double val = V3.d;
  hio_return_t hrc = HIO_SUCCESS;
  hio_object_t obj = NULL;
  hio_config_type_t vtype;
  union config_type pval;
  double vdoub = 0.0;

  DBG4("hvaf: type: %s name: %s op: %s val: %f", enum_name(MY_MSG_CTX, &etab_hvat, type),
        name, enum_name(MY_MSG_CTX, &etab_hvao, op), val);
 
       if (HVAT_pc == type) obj = (hio_object_t) S.context;
  else if (HVAT_pd == type) obj = (hio_object_t) S.dataset;
  else if (HVAT_pe == type) obj = (hio_object_t) S.element;
  else ERRX("%s: internal error: invalid type %d", A.desc, type); 

  if (!obj) ERRX("%s: object not valid (type: %s)", A.desc, enum_name(MY_MSG_CTX, &etab_hvat, type));
 
  hrc = get_perf_type(gptr, obj, name, &vtype, actionp);
  HRC_TEST("get_perf_type");
  if (HIO_SUCCESS == hrc) {
    hrc = hio_perf_get_value(obj, name, &pval, sizeof(pval));
    HRC_TEST("hio_get_perf_value");
    if (HIO_SUCCESS == hrc) {
      switch(vtype) {
        case HIO_CONFIG_TYPE_FLOAT:
          vdoub = pval.FLOAT;
          break;
        case HIO_CONFIG_TYPE_DOUBLE:
          vdoub = pval.DOUBLE;
          break;
        case HIO_CONFIG_TYPE_BOOL:
        case HIO_CONFIG_TYPE_INT32:
        case HIO_CONFIG_TYPE_UINT32:
        case HIO_CONFIG_TYPE_INT64:
        case HIO_CONFIG_TYPE_UINT64:
        case HIO_CONFIG_TYPE_STRING:
          MSG("%s: Perf variable %s is %s, not comparable with float",
              A.desc, name, enum_name(MY_MSG_CTX, &etab_hcfg, vtype)); 
          hrc = HIO_ERR_BAD_PARAM;
          G.local_fails += S.hio_fail = 1;
          break;
      }
    }
    if (HIO_SUCCESS == hrc) {
      switch(op) {
        case HVAO_LT: if ( !(vdoub <  val) ) S.hio_fail = 1; break;
        case HVAO_LE: if ( !(vdoub <= val) ) S.hio_fail = 1; break;
        case HVAO_EQ: if ( !(vdoub == val) ) S.hio_fail = 1; break;
        case HVAO_GE: if ( !(vdoub >= val) ) S.hio_fail = 1; break;
        case HVAO_GT: if ( !(vdoub >  val) ) S.hio_fail = 1; break;
        case HVAO_NE: if ( !(vdoub != val) ) S.hio_fail = 1; break;
      }
      G.local_fails += S.hio_fail;
      if (S.hio_fail || MY_MSG_CTX->verbose_level >= 2) {
        MSG("%s: Assert %s.  Actual value: %f", A.desc, S.hio_fail ? "failed" : "succeeded", vdoub);
      }
    }
  }
}

ACTION_RUN(hvas_run) {
  enum hva_type type = (enum hva_type)V0.i;
  char * name = V1.s;
  enum hva_op op = (enum hva_op)V2.i;
  char * val = V3.s;
  hio_return_t hrc = HIO_SUCCESS;
  hio_object_t obj = NULL;
  hio_config_type_t vtype;
  union config_type pval;
  char * vstr;

  DBG4("hvaf: type: %s name: %s op: %s val: \"%s\"", enum_name(MY_MSG_CTX, &etab_hvat, type),
        name, enum_name(MY_MSG_CTX, &etab_hvao, op), val);
 
       if (HVAT_pc == type || HVAT_cc == type) obj = (hio_object_t) S.context;
  else if (HVAT_pd == type || HVAT_cd == type) obj = (hio_object_t) S.dataset;
  else if (HVAT_pe == type || HVAT_ce == type) obj = (hio_object_t) S.element;
  else ERRX("%s: internal error: invalid type %d", A.desc, type); 

  if (!obj) ERRX("%s: object not valid (type: %s)", A.desc, enum_name(MY_MSG_CTX, &etab_hvat, type));
 
  if (HVAT_pc == type || HVAT_pd == type || HVAT_pe == type) {
    hrc = get_perf_type(gptr, obj, name, &vtype, actionp);
    HRC_TEST("get_perf_type");
    if (HIO_SUCCESS == hrc) {
      hrc = hio_perf_get_value(obj, name, &pval, sizeof(pval));
      HRC_TEST("hio_get_perf_value");
      if (HIO_SUCCESS == hrc) {
        switch(vtype) {
          case HIO_CONFIG_TYPE_FLOAT:
          case HIO_CONFIG_TYPE_DOUBLE:
          case HIO_CONFIG_TYPE_BOOL:
          case HIO_CONFIG_TYPE_INT32:
          case HIO_CONFIG_TYPE_UINT32:
          case HIO_CONFIG_TYPE_INT64:
          case HIO_CONFIG_TYPE_UINT64:
            MSG("%s: Perf variable %s is %s, not comparable with string",
                A.desc, name, enum_name(MY_MSG_CTX, &etab_hcfg, vtype)); 
            hrc = HIO_ERR_BAD_PARAM;
            G.local_fails += S.hio_fail = 1;
            break; 
          case HIO_CONFIG_TYPE_STRING:
            vstr = pval.STRING;
            break;
        }
      }
    } 
   } else { // Must be config var
    hrc = hio_config_get_value(obj, name, &vstr);
    HRC_TEST("hio_config_get_value");
  }

  if (HIO_SUCCESS == hrc) {
    int cmp = strcmp(vstr, val);
    switch(op) {
      case HVAO_LT: if ( !(cmp <  0) ) S.hio_fail = 1; break;
      case HVAO_LE: if ( !(cmp <= 0) ) S.hio_fail = 1; break;
      case HVAO_EQ: if ( !(cmp == 0) ) S.hio_fail = 1; break;
      case HVAO_GE: if ( !(cmp >= 0) ) S.hio_fail = 1; break;
      case HVAO_GT: if ( !(cmp >  0) ) S.hio_fail = 1; break;
      case HVAO_NE: if ( !(cmp != 0) ) S.hio_fail = 1; break;
    }
    G.local_fails += S.hio_fail;
    if (S.hio_fail || MY_MSG_CTX->verbose_level >= 2) {
      MSG("%s: Assert %s.  Actual value: \"%s\"", A.desc, S.hio_fail ? "failed" : "succeeded", vstr);
    }
  }
}

//----------------------------------------------------------------------------
// hdsc - hio_dataset_should_checkpoint action handler
//----------------------------------------------------------------------------
ACTION_RUN(hdsc_run) {
  // char * name = V0.s;
  hio_recommendation_t expected = (enum hio_recommendation_t) V1.i;
  hio_recommendation_t retval;
  if (!S.context) ERRX("%s: hio context not established", A.desc);

  // retval = hio_datset_should_checkpoint(S.context, name);
  retval = HIO_SCP_NOT_NOW;
  //G.local_fails += (S.hio_fail = (retval != expected && expected != HIO_SCP_ANY) ? 1: 0);

  if (S.hio_fail || MY_MSG_CTX->verbose_level >= 3) {
    MSG("%s: hio_dataset_should_checkpoint %s; ret: %s exp: %s errno: %d(%s)", A.desc,
         S.hio_fail ? "FAIL": "OK", enum_name(MY_MSG_CTX, &etab_hcpr, retval),
         enum_name(MY_MSG_CTX, &etab_hcpr, expected), errno, strerror(errno));
  }
  if (S.hio_fail) hio_err_print_all(S.context, stderr, "[ hio_dataset_should_checkpoint error]");
  
}

//----------------------------------------------------------------------------
// DataWarp API action handlers
//----------------------------------------------------------------------------
#if HIO_USE_DATAWARP
ACTION_RUN(dsdo_run) {
  char * dw_dir = V0.s;
  char * pfs_dir = V1.s;
  enum dw_stage_type type = (enum dw_stage_type) V2.i;
  int rc = dw_stage_directory_out(dw_dir, pfs_dir, type);
  if (rc || MY_MSG_CTX->verbose_level >= 3) {                                          
    MSG("dw_stage_directory_out(%s, %s, %s) rc: %d", dw_dir, pfs_dir, enum_name(MY_MSG_CTX, &etab_dwst, type), rc);
  }
}

ACTION_RUN(dwds_run) {
  ETIMER tmr;
  ETIMER_START(&tmr);
  int rc = dw_wait_directory_stage(V0.s);
  if (rc) G.local_fails++;
  VERB1("dw_wait_directory_stage(%s) rc: %d (%s)  time: %f Sec", V0.s, rc, strerror(abs(rc)), ETIMER_ELAPSED(&tmr));
}

ACTION_RUN(dwss_run) {
  char * path = V0.s;
  int stripe_size = V1.u;
  int stripe_width = V2.u;
  int rc, quit = 0;
  struct stat stat;

  int fd = open(path, O_RDONLY);
  if (-1 == fd) {
    if (ENOENT == errno) {
      fd = open(path, O_CREAT, S_IRUSR + S_IWUSR + S_IRGRP + S_IWGRP + S_IROTH + S_IWOTH );
      if (-1 == fd) {
        VERB0("dwss: unable to create path \"%s\", errno: %d(%s)", path, errno, strerror(errno));
        quit = 1;
      }
    } else {  
      VERB0("dwss: unable to open path \"%s\", errno: %d(%s)", path, errno, strerror(errno));
      quit = 1;
    }
  }

  if (!quit) {
    rc = fstat(fd, &stat);
    if (rc != 0) {
      VERB0("dwss: fstat fails, errno: %d(%s)", errno, strerror(errno));
      quit = 1;
    }
  }

  if (!quit) {
    if (stat.st_mode & S_IFREG && stat.st_size != 0) {
      VERB0("dwss: cannot set striping on non-empty file %d", path);
      quit = 1;
    }
  }

  if (!quit) {
    rc = dw_set_stripe_configuration(fd, stripe_size, stripe_width);
    if (rc < 0) {
      VERB0("dwss: dw_set_stripe_configuration rc: %d(%s)", rc, strerror(-rc));
      quit = 1;
    }
  }

  if (!quit) {
    char * type;
    if (stat.st_mode & S_IFREG) type = "File";
    else if (stat.st_mode & S_IFDIR) type = "Dir";
    else type = "Other";

    VERB1("dwss: %s %s set stripe size: %d width: %d", 
          type, path, stripe_size, stripe_width);
  }
  
  if (-1 != fd) {
    rc = close(fd);
    if (rc < 0) {
      VERB0("dwss: close error, errno: %d(%s)", errno, strerror(errno));
      quit = 1;
    }
  } 

  if (quit) G.local_fails++;
 
}

ACTION_RUN(dwgs_run) {
  char * path = V0.s;
  int rc, quit = 0;
  struct stat stat;
  int stripe_size, stripe_width, starting_index;

  int fd = open(path, O_RDONLY);
  if (-1 == fd) {
    VERB0("dwgs: unable to open path \"%s\", errno: %d(%s)", path, errno, strerror(errno));
    quit = 1;
  }

  if (!quit) {
    rc = fstat(fd, &stat);
    if (rc != 0) {
      VERB0("dwgs: fstat fails, errno: %d(%s)", errno, strerror(errno));
      quit = 1;
    }
  }

  if (!quit) {
    rc = dw_get_stripe_configuration(fd, &stripe_size, &stripe_width, &starting_index);
    if (rc < 0) {
      VERB0("dwgs: dw_get_stripe_configuration rc: %d(%s)", rc, strerror(-rc));
      quit = 1;
    }
  }
  
  if (!quit) {
    char * type;
    if (stat.st_mode & S_IFREG) type = "File";
    else if (stat.st_mode & S_IFDIR) type = "Dir";
    else type = "Other";

    VERB1("dwgs: %s %s stripe size: %d width: %d start index: %d", 
          type, path, stripe_size, stripe_width, starting_index);
  }
  
  if (-1 != fd) {
    rc = close(fd);
    if (rc < 0) {
      VERB0("dwgs: close error, errno: %d(%s)", errno, strerror(errno));
      quit = 1;
    }
  } 

  if (quit) G.local_fails++;
}

ACTION_RUN(dwmp_run) {
  char * dw_root = V0.s;
  U64 ppn = V1.u;
  char * prefix;

  U64 key = G.myrank/ppn;
  ERRX("dwmp not yet supported"); 
  //prefix = dw_get_mds_path(dw_root, key);
  prefix="";
  VERB1("dw_get_mds_path(\"%s\", %lld) returns \"%s\"", dw_root, key, prefix);
  int rc = setenv("HIO_datawarp_root", prefix, 1);
  if (rc) ERRX("setenv(%s, %s) rc: %d errno: %d(%s)", "HIO_datawarp_root", prefix, rc, errno, strerror(errno));
}


#if DW_PH_2
ACTION_RUN(dwws_run) {
  char * filename = V0.s;
  ETIMER tmr;
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    MSG("dwws: open(%s) fails with errno:%d (%s)", filename, errno, strerror(errno));
    G.local_fails++;
  } else {
    ETIMER_START(&tmr);
    int rc = dw_wait_sync_complete(fd);
    if (rc != 0) {
      MSG("dwws dw_wait_sync_complete(%s) fails with rc: %d (%s)", filename, rc, strerror(abs(rc)));
      G.local_fails++;
    }
    VERB1("dw_wait_sync_complete(%s) rc: %d (%s)  time: %f Sec", filename, rc, strerror(abs(rc)), ETIMER_ELAPSED(&tmr));
    rc = close(fd);
    if (rc != 0) {
      MSG("dwws: close(%s) fails with errno:%d (%s)", filename, errno, strerror(errno));
      G.local_fails++;
    }
  }
}
#endif // DW_PH_2
#endif // HIO_USE_DATAWARP

#endif // HIO





//----------------------------------------------------------------------------
// xexec_hio - init libHIO commands
//----------------------------------------------------------------------------
MODULE_INSTALL(xexec_hio_install) {
#ifdef HIO
  struct xexec_act_parse parse[] = {
  // Command   V0    V1    V2    V3    V4     Check          Run
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
    {"hdu",   {STR,  HDSI, HULM, NONE, NONE}, NULL,          hdu_run     },
    {"heful", {NONE, NONE, NONE, NONE, NONE}, NULL,          heful_run   },
    {"hefum", {UINT, NONE, NONE, NONE, NONE}, NULL,          hefum_run   },
    {"hf",    {NONE, NONE, NONE, NONE, NONE}, NULL,          hf_run      },
    {"hxrc",  {HERR, NONE, NONE, NONE, NONE}, NULL,          hxrc_run    },
    {"hxct",  {SINT, NONE, NONE, NONE, NONE}, hxct_check,    hxct_run    },
    {"hxdi",  {HDSI, NONE, NONE, NONE, NONE}, NULL,          hxdi_run    },
    {"hvp",   {REGX, REGX, NONE, NONE, NONE}, NULL,          hvp_run     },
    {"hvp2",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvp2_run     },
    {"hvsc",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvsc_run    },
    {"hvsd",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvsd_run    },
    {"hvse",  {STR,  STR,  NONE, NONE, NONE}, NULL,          hvse_run    },
    {"hdsc",  {STR,  HCPR, NONE, NONE, NONE}, NULL,          hdsc_run    },
    {"hvai",  {HVAT, STR,  HVAO, SINT, NONE}, hvaif_check,   hvai_run    },
    {"hvaf",  {HVAT, STR,  HVAO, DOUB, NONE}, hvaif_check,   hvaf_run    },
    {"hvas",  {HVAT, STR,  HVAO, STR,  NONE}, NULL,          hvas_run    },
    #if HIO_USE_DATAWARP
    {"dsdo",  {STR,  STR,  DWST, NONE, NONE}, NULL,          dsdo_run    },
    {"dwds",  {STR,  NONE, NONE, NONE, NONE}, NULL,          dwds_run    },
    {"dwss",  {STR,  UINT, UINT, NONE, NONE}, NULL,          dwss_run    },
    {"dwgs",  {STR,  NONE, NONE, NONE, NONE}, NULL,          dwgs_run    },
    {"dwmp",  {STR,  UINT, NONE, NONE, NONE}, NULL,          dwmp_run    },
    #if DW_PH_2
    {"dwws",  {STR,  NONE, NONE, NONE, NONE}, NULL,          dwws_run    },
    #endif  // DW_PH_2
    #endif  // HIO_USE_DATAWARP
  };

  xexec_act_add(&G, parse, DIM1(parse), xexec_hio_init, &hio_state, xexec_hio_help);
#endif  // HIO

  return 0;
}
