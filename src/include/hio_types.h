/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#if !defined(HIO_TYPES_H)
#define HIO_TYPES_H

#include "hio_internal.h"
#include "hio_var.h"
#include "hio_component.h"

#if defined(HAVE_SYS_TIME_H)
#include <sys/time.h>
#endif

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

struct hio_config_t;

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

  /** parent object */
  hio_object_t      parent;
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
  /** threading lock */
  pthread_mutex_t   c_lock;
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
  hio_config_kv_t  *c_fconfig;
  int               c_fconfig_count;
  int               c_fconfig_size;

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

  hio_list_t         c_ds_data;
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
  int      fs_type;
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

  /** filesystem open function (for data) */
  hio_fs_open_fn_t fs_open;
};
typedef struct hio_fs_attr_t hio_fs_attr_t;


struct hio_dataset {
  /** allows for type detection */
  struct hio_object   ds_object;

  /** dataset identifier */
  uint64_t            ds_id;
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

  /** dataset file modes */
  hio_dataset_fmode_t ds_fmode;

  /** open time */
  struct timeval      ds_otime;

  /** aggregate number of bytes read */
  uint64_t            ds_bread;
  /** aggregate read time */
  uint64_t            ds_rtime;

  /** aggregate number of bytes written */
  uint64_t            ds_bwritten;
  /** aggregate write time */
  uint64_t            ds_wtime;

  /** data associated with this dataset */
  hio_dataset_data_t *ds_data;

  /** dataset status */
  int                 ds_status;

  /** dataset open function (data) */
  hio_fs_attr_t       fs_fsattr;
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
} hio_manifest_segment_t;

struct hio_element {
  struct hio_object e_object;

  /** elements are held in a list on the associated dataset */
  hio_list_t        e_list;

  /** segment list */
  hio_list_t        e_slist;

  /** element is currently open */
  bool              e_is_open;

  /** (basic mode only) backing file for this element */
  char             *e_bfile;

  /** first invalid offset after the last valid block */
  int64_t           e_size;

  /** element file handle (not used by all backends) */
  FILE             *e_fh;
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

/**
 * Get the associated context for an hio object
 *
 * @param[in] object       hio object
 *
 * @returns hio context on success
 *
 * This function can be used to follow the parent pointers on any hio
 * object to get the hio_context_t the object was created under.
 */
hio_context_t hioi_object_context (hio_object_t object);

/**
 * Macro to get the dataset for an hio element
 */
#define hioi_element_dataset(e) (hio_dataset_t) (e)->e_object.parent

/**
 * Allocate a new dataset object and populate it with common data (internal)
 *
 * @param[in] name         dataset name
 * @param[in] id           id of this dataset instance
 * @param[in] flags        flags for this dataset instance
 * @param[in] mode         offset mode of this dataset
 * @param[in] dataset_size size of the dataset object to allocate
 *
 * @returns hio dataset object on success
 * @returns NULL on failure
 *
 * This function generates a generic dataset object and populates
 * the shared fields. The module should populate the private data
 * if needed.
 *
 * This function may or may not appear in the final release of
 * libhio. It may become the responsibility of the hio module to
 * allocate the memory it needs to implement a dataset (including
 * the shared bit above).
 */
hio_dataset_t hioi_dataset_alloc (hio_context_t context, const char *name, int64_t id,
                                  int flags, hio_dataset_mode_t mode,
                                  size_t dataset_size);

/**
 * @brief scatter dataset configuration to all processes
 *
 * @param[in] dataset     dataset to scatter
 * @param[in] rc          current return code
 */
int hioi_dataset_scatter (hio_dataset_t dataset, int rc);

/**
 * @brief gather dataset configuration from all processes
 *
 * @param[in] dataset     dataset to gather
 */
int hioi_dataset_gather (hio_dataset_t dataset);

/**
 * Release a dataset object (internal)
 */
void hioi_dataset_release (hio_dataset_t *set);

/**
 * Add an element to a dataset
 *
 * @param[in] dataset   dataset to modify
 * @param[in] element   element structure to add
 */
void hioi_dataset_add_element (hio_dataset_t dataset, hio_element_t element);

/* context dataset persistent data functions */

/**
 * Allocate new and store backend data structure
 *
 * @param[in] data         dataset persistent data structure
 * @param[in] backend_name name of the requesting backend
 * @param[in] size         size of backend data structure
 */
hio_dataset_backend_data_t *hioi_dbd_alloc (hio_dataset_data_t *data, const char *backend_name, size_t size);

/**
 * Retrieve stored backend data
 *
 * @param[in] data         dataset persistent data structure
 * @param[in] backend_name name of the requesting backend
 */
hio_dataset_backend_data_t *hioi_dbd_lookup_backend_data (hio_dataset_data_t *data, const char *backend_name);

/* element functions */

/**
 * Allocate and setup a new element object
 *
 * @param[in] dataset   dataset the element will be added to (see hioi_dataset_add_element)
 * @param[in] name      element identifier
 */
hio_element_t hioi_element_alloc (hio_dataset_t dataset, const char *name);


/**
 * Release an hio element
 *
 * @param[in] element  element to release
 */
void hioi_element_release (hio_element_t element);

hio_request_t hioi_request_alloc (hio_context_t context);

void hioi_request_release (hio_request_t request);

int hioi_element_add_segment (hio_element_t element, off_t file_offset, uint64_t app_offset,
                              int rank, size_t seg_length);

int hioi_element_find_offset (hio_element_t element, uint64_t app_offset, int rank,
                              off_t *offset, size_t *length);

/* manifest functions */

/**
 * @brief Serialize the manifest in the dataset
 *
 * @param[in]  dataset   dataset to serialize
 * @param[out] data      serialized data
 * @param[out] data_size size of serialized data
 *
 * This function serializes the local data associated with the dataset and returns a buffer
 * containing the serialized data.
 */
int hioi_manifest_serialize (hio_dataset_t dataset, unsigned char **data, size_t *data_size);

/**
 * @brief Serialize the manifest in the dataset and save it to the specified file
 *
 * @param[in]  dataset   dataset to serialize
 * @param[in]  path      file to save the manifest into
 *
 * This function serializes the local data associated with the dataset and saves it
 * to the specified file.
 */
int hioi_manifest_save (hio_dataset_t dataset, const char *path);

int hioi_manifest_deserialize (hio_dataset_t dataset, const unsigned char *data, size_t data_size);
int hioi_manifest_load (hio_dataset_t dataset, const char *path);
int hioi_manifest_merge_data (hio_dataset_t dataset, const unsigned char *data, size_t data_size);


/**
 * Read header data from a manifest
 *
 * @param[in]  context   hio context
 * @param[out] header    hio dataset header to fill in
 * @param[in]  path      hio manifest to read
 *
 * @returns HIO_SUCCESS on success
 * @returns hio error code on error
 *
 * This function reads the header data out of an hio manifest. This data includes
 * the dataset id, file status, and modification time.
 */
int hioi_manifest_read_header (hio_context_t context, hio_dataset_header_t *header, const char *path);


/* context functions */

static inline bool hioi_context_using_mpi (hio_context_t context) {
#if HIO_USE_MPI
  return context->c_use_mpi;
#endif

  return false;
}

hio_module_t *hioi_context_select_module (hio_context_t context);

/**
 * @brief Query filesystem attributes
 *
 * @param[in]  context    hio context
 * @param[in]  path       path on the filesystem to query (directory/file ok)
 * @param[out] attributes filesystem path attributes
 *
 * @returns HIO_SUCCESS on success
 * @returns hio error code on error
 *
 * This function queries a filesystem path and returns the attributes of that
 * path (block count, stripe count, etc). The query function also returns an
 * open function that should be used to open/create data files. The open function
 * takes an attributes structure as an extra argument. Any striping information
 * will be retreived from this function.
 */
int hioi_fs_query (hio_context_t context, const char *path, hio_fs_attr_t *attributes);

#endif /* !defined(HIO_TYPES_H) */
