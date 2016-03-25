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

#if !defined(HIO_TYPES_H)
#define HIO_TYPES_H

#include "hio_var.h"
#include "hio_component.h"

#if defined(HAVE_SYS_TIME_H)
#include <sys/time.h>
#endif

#if defined(HAVE_PTHREAD_H)
#include <pthread.h>
#endif

/**
 * Maximum number of data roots.
 */
#define HIO_MAX_DATA_ROOTS   64

/**
 * Simple lists
 */
typedef struct hio_list_t {
  struct hio_list_t *prev, *next;
} hio_list_t;

#define hioi_list_init(head)                    \
  (head).next = (head).prev = &(head)

#define hioi_list_item(list, type, member)              \
  (type *)((intptr_t) list - offsetof (type, member))
#define hioi_list_foreach(item, head, type, member)                     \
  for (item = hioi_list_item((head).next, type, member) ; &(item)->member != &(head) ; \
       item = hioi_list_item((item)->member.next, type, member))

#define hioi_list_foreach_safe(item, next_item, head, type, member)          \
  for (item = hioi_list_item((head).next, type, member), next_item = hioi_list_item((item)->member.next, type, member) ; \
       &(item)->member != &(head) ;                                     \
       item = next_item, next_item = hioi_list_item((item)->member.next, type, member))

#define hioi_list_remove(item, member)                        \
  do {                                                        \
    (item)->member.next->prev = (item)->member.prev;          \
    (item)->member.prev->next = (item)->member.next;          \
    (item)->member.next = (item)->member.prev = NULL;         \
  } while (0)

#define hioi_list_append(item, head, member)                  \
  do {                                                        \
    (item)->member.prev = (head).prev;                        \
    (item)->member.next = &(head);                            \
    (head).prev->next = &(item)->member;                      \
    (head).prev = &(item)->member;                            \
  } while (0)

#define hioi_list_prepend(item, head, member)                 \
  do {                                                        \
    (item)->member.next = (head).next;                        \
    (item)->member.prev = &(head);                            \
    (head).next->prev = &(item)->member;                      \
    (head).next = &(item)->member;                            \
  } while (0)


static inline bool hioi_list_empty (hio_list_t *list) {
  return list->prev == list->next;
}

/* dataset function types */

/**
 * Close a dataset and release any internal state
 *
 * @param[in] dataset dataset object
 *
 * @returns HIO_SUCCESS if the dataset was successfully closed
 * @returns hio error code on failure
 *
 * This function closes the dataset and releases any internal data
 * stored on the dataset. The function is not allowed to release
 * the dataset itself. If the dataset has been modified the module
 * should ensure that either the data is committed to the data root
 * or the appropriate error code is returned.
 *
 * @note The module should not attempt to unlink/delete the data
 *       set if an error occurs. Instead, the module should be able
 *       to detect the failure on a subsequent open call. It is up
 *       to the hio user to unlink failed datsets.
 */
typedef int (*hio_dataset_close_fn_t) (hio_dataset_t dataset);

/**
 * Open an element of the dataset
 *
 * @param[in]  dataset      hio dataset object
 * @param[out] element_out  new hio element object
 * @param[in]  element_name name of the hio dataset element
 * @param[in]  flags        element open flags
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_NOT_FOUND if the element does not exist (read-only datasets)
 * @returns HIO_ERR_OUT_OF_RESOURCE on resource exhaustion
 * @returns hio error on other failure
 *
 * This function opens a named element on an hio dataset.
 */
typedef int (*hio_dataset_element_open_fn_t) (hio_dataset_t dataset, hio_element_t element);

/**
 * Write to an hio dataset element
 *
 * @param[in]  element      hio dataset element object
 * @param[out] request      new request object if requested (can be NULL)
 * @param[in]  offset       element offset to write to
 * @param[in]  ptr          data to write
 * @param[in]  count        number of blocks to write
 * @param[in]  size         size of blocks
 * @param[in]  stride       number of bytes between blocks
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be written to
 * @returns hio error on other error
 *
 * This function schedules data to be written to a dataset element. Modules are free
 * to delay any updates to the dataset element until hio_element_close() or
 * hio_element_flush() at the latest.
 */
typedef int (*hio_element_write_strided_nb_fn_t) (hio_element_t element,
                                                  hio_request_t *request,
                                                  off_t offset, const void *ptr,
                                                  size_t count, size_t size,
                                                  size_t stride);

/**
 * Read from an hio dataset element
 *
 * @param[in]  element      hio dataset element object
 * @param[out] request      new request object if requested (can be NULL)
 * @param[in]  offset       element offset to read from
 * @param[in]  ptr          data to write
 * @param[in]  count        number of blocks to read
 * @param[in]  size         size of blocks
 * @param[in]  stride       number of bytes between blocks
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be written to
 * @returns hio error on other error
 *
 * This function schedules data to be read from a dataset element. Modules are free
 * to delay any reads from the dataset element until hio_element_complete().
 */
typedef int (*hio_element_read_strided_nb_fn_t) (hio_element_t element, hio_request_t *request,
                                                 off_t offset, void *ptr, size_t count,
                                                 size_t size, size_t stride);

/**
 * Flush writes to a dataset element
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  element      hio dataset element object
 * @param[in]  module       hio flush mode
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be written to
 * @returns hio error on other error
 *
 * This function flushes all outstanding writes to a dataset element. When this
 * call returns the user should be free to update any buffers associated with
 * writes on the dataset element.
 */
typedef int (*hio_element_flush_fn_t) (hio_element_t element, hio_flush_mode_t mode);

/**
 * Complete all reads from a dataset element
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  element      hio dataset element object
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be read from
 * @returns hio error on other error
 *
 * This function completes all outstanding reads from a dataset element. When this
 * call returns all requested data (if available) should be available in the
 * user's buffers.
 */
typedef int (*hio_element_complete_fn_t) (hio_element_t element);

/**
 * Close a dataset element
 *
 * @param[in] module       hio module associated with the dataset element
 * @param[in] element      element to close
 *
 * @returns HIO_SUCCESS on success
 * @returns hio error code if any error occurred on the element that has not
 *          already been returned
 *
 * This function closes and hio dataset element and frees any internal data
 * allocated by the backend module. hio modules are allowed to defer reporting
 * any errors until hio_element_close() or hio_dataset_close().
 */
typedef int (*hio_element_close_fn_t) (hio_element_t element);

typedef void (*hio_object_release_fn_t) (hio_object_t object);

struct hio_config_t;

typedef enum {
  HIO_OBJECT_TYPE_CONTEXT,
  HIO_OBJECT_TYPE_DATASET,
  HIO_OBJECT_TYPE_ELEMENT,
  HIO_OBJECT_TYPE_REQUEST,
  HIO_OBJECT_TYPE_ANY,
} hio_object_type_t;

/**
 * Base of all hio objects
 */
struct hio_object {
  hio_object_type_t type;

  /** identifer for this object (context, dataset, or element name) */
  char             *identifier;

  /** in hio configuration is done per context, dataset, or element.
   * this part of the object stores all the registered configuration
   * variables */
  hio_var_array_t   configuration;

  /** in hio performance is measured per context, dataset, or element.
   * this part of the object stores all the registered peformance
   * variables */
  hio_var_array_t   performance;

  /** econfigurations variables set by hio_set_config */
  hio_config_kv_list_t config_set;

  /** parent object */
  hio_object_t      parent;

  /** object thread protection */
  pthread_mutex_t   lock;

  /** object release function */
  hio_object_release_fn_t release_fn;
};

struct hio_context {
  struct hio_object c_object;

#if HIO_USE_MPI
  /** internal communicator for this context */
  MPI_Comm          c_comm;
  bool              c_use_mpi;
#endif

  /** my rank in the context */
  int               c_rank;
  /** number of ranks using this context */
  int               c_size;

  /** unreported errors on this context */
  void             *c_estack;
  /** comma-separated list of data roots available */
  char             *c_droots;
  /** print statistics on close */
  bool              c_print_stats;
  /** number of bytes written to this context (local) */
  uint64_t          c_bwritten;
  /** number of bytes read from this context (local) */
  uint64_t          c_bread;
  /** context verbosity */
  int32_t           c_verbose; // Signed to prevent Cray compiler error when comparing with 0
  /** file configuration for the context */
  hio_config_kv_list_t c_fconfig;

  /** io modules (one for each data root) */
  hio_module_t      *c_modules[HIO_MAX_DATA_ROOTS];
  /** number of data roots */
  int                c_mcount;
  /** current active data root */
  int                c_cur_module;

#if HIO_USE_DATAWARP
  /** path to datawarp root */
  char              *c_dw_root;
#endif

#if HAVE_MPI_COMM_SPLIT_TYPE
  MPI_Comm           c_shared_comm;
  int                c_shared_size;
  int                c_shared_rank;
  int               *c_shared_ranks;
#endif

  hio_list_t         c_ds_data;

  /** size of a dataset object */
  size_t             c_ds_size;
};

struct hio_dataset_data_t {
  /** dataset data list */
  hio_list_t  dd_list;

  /** name of this dataset */
  const char *dd_name;

  /** last complete dataset id */
  int64_t     dd_last_id;

  /** last time a write completed */
  time_t      dd_last_write_completion;

  /** weighted average write time for a member of this dataset */
  uint64_t    dd_average_write_time;

  /** average dataset size */
  size_t      dd_average_size;

  hio_list_t  dd_backend_data;
};
typedef struct hio_dataset_data_t hio_dataset_data_t;

struct hio_dataset_backend_data_t {
  hio_list_t  dbd_list;

  const char *dbd_backend_name;
};
typedef struct hio_dataset_backend_data_t hio_dataset_backend_data_t;

typedef enum hio_dataset_fmode {
  /** use basic mode. unique address space results in a single file per element per rank.
   * shared address space results in a single file per element */
  HIO_FILE_MODE_BASIC,
  /** use optimized mode. there is no guarantee about file structure in this mode */
  HIO_FILE_MODE_OPTIMIZED,
} hio_dataset_fmode_t;

struct hio_fs_attr_t;

typedef int (*hio_fs_open_fn_t) (const char *path, struct hio_fs_attr_t *fs_attr, int flags, int mode);

#define HIO_FS_SUPPORTS_STRIPING 1

enum {
  HIO_FS_TYPE_DEFAULT,
  HIO_FS_TYPE_LUSTRE,
  HIO_FS_TYPE_GPFS,
  HIO_FS_TYPE_MAX,
};

struct hio_fs_attr_t {
  /** filesystem type index */
  int32_t  fs_type;
  /** flags indicating filesystem features */
  int      fs_flags;
  /** available blocks on the filesystem */
  uint64_t fs_bavail;
  /** total blocks on the filesystem */
  uint64_t fs_btotal;
  /** size of a filesystem block */
  uint64_t fs_bsize;

  /** size of a stripe (0 if unsupported) */
  uint64_t fs_ssize;
  /** stripe unit (bytes) */
  uint64_t fs_sunit;
  /** stripe maximum size */
  uint64_t fs_smax_size;

  /** number of stripes (0 if unsupported) */
  uint32_t fs_scount;
  /** maximum stripe count */
  uint32_t fs_smax_count;

  /** raid level */
  uint32_t fs_raid_level;

  /** filesystem open function (for data) */
  hio_fs_open_fn_t fs_open;
};
typedef struct hio_fs_attr_t hio_fs_attr_t;

typedef struct hio_manifest_file_t {
  /** list item (e_flist) */
  hio_list_t f_list;
  /** file name */
  char *f_name;
} hio_manifest_file_t;


struct hio_dataset {
  /** allows for type detection */
  struct hio_object   ds_object;

  /** dataset identifier */
  uint64_t            ds_id;
  /** dataset identifier requested */
  uint64_t            ds_id_requested;
  /** flags used during creation of this dataset */
  int                 ds_flags;
  /** open mode */
  hio_dataset_mode_t  ds_mode;

  /** module in use */
  hio_module_t       *ds_module;

  /** block size to use for optimized file mode */
  uint64_t            ds_bs;

  /** list of elements */
  hio_list_t          ds_elist;

  /** list of files */
  hio_list_t          ds_flist;

  /** dataset file modes */
  hio_dataset_fmode_t ds_fmode;

  /** open time */
  struct timeval      ds_otime;

  /** relative open time (for statistics) */
  uint64_t            ds_rotime;

  struct {
    /** aggregate number of bytes read */
    uint64_t            s_bread;
    /** aggregate read time */
    uint64_t            s_rtime;

    /** aggregate number of bytes written */
    uint64_t            s_bwritten;
    /** aggregate write time */
    uint64_t            s_wtime;
  } ds_stat;

  /** data associated with this dataset */
  hio_dataset_data_t *ds_data;

  /** dataset status */
  int                 ds_status;

  /** dataset open function (data) */
  hio_fs_attr_t       ds_fsattr;

  /** close the dataset and free any internal resources */
  hio_dataset_close_fn_t ds_close;

  /** open an element in the dataset */
  hio_dataset_element_open_fn_t ds_element_open;
};

struct hio_request {
  struct hio_object req_object;
  /** completion indicator */
  bool              req_complete;
  /** number of bytes transferred */
  size_t            req_transferred;
  /** status of the request */
  int               req_status;
};

typedef struct hio_manifest_segment_t {
  /** list item (e_slist) */
  hio_list_t seg_list;
  /** file offset */
  uint64_t   seg_foffset;
  /** application offset */
  uint64_t   seg_offset;
  /** application rank (0 for shared) */
  uint64_t   seg_rank;
  /** length of segment */
  uint64_t   seg_length;
  /** file index */
  int        seg_file_index;
} hio_manifest_segment_t;

struct hio_element {
  struct hio_object e_object;

  /** rank this element belongs to (-1 for shared) */
  int               e_rank;

  /** elements are held in a list on the associated dataset */
  hio_list_t        e_list;

  /** segment list */
  hio_list_t        e_slist;

  /** element is currently open */
  int32_t           e_open_count;

  /** first invalid offset after the last valid block */
  int64_t           e_size;

  /** element file handle (not used by all backends) */
  FILE             *e_fh;

  /** function to write strided data */
  hio_element_write_strided_nb_fn_t e_write_strided_nb;

  /** function to read strided data */
  hio_element_read_strided_nb_fn_t e_read_strided_nb;

  /** function to flush pending element writes */
  hio_element_flush_fn_t e_flush;

  /** function to complete pending element reads */
  hio_element_complete_fn_t e_complete;

  /** function to close the element */
  hio_element_close_fn_t e_close;
};

struct hio_dataset_header_t {
  /** dataset identifier */
  int64_t  ds_id;
  /** dataset modification time */
  time_t   ds_mtime;
  /** dataset mode (unique, shared) */
  int      ds_mode;
  /** dataset file mode (optimized, basic) */
  int      ds_fmode;
  /** dataset status (set at close time) */
  int      ds_status;
};
typedef struct hio_dataset_header_t hio_dataset_header_t;

/**
 * Compare two headers
 *
 * Functions of this type return 1 if the first header is larger than the
 * second, and 0 otherwise.
 */
typedef int (*hioi_dataset_header_compare_t) (hio_dataset_header_t *, hio_dataset_header_t *);

#endif /* !defined(HIO_TYPES_H) */
