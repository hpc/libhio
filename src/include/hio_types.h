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

#define hioi_list_foreach_safe(item, next, head, type, member)          \
  for (item = hioi_list_item((head).next, type, member), next = hioi_list_item((item)->member.next, type, member) ; \
       &(item)->member != &(head) ;                                     \
       item = next, next = hioi_list_item((item)->member.next, type, member))

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

struct hio_config_t;

/**
 * Base of all hio objects
 */
struct hio_object_t {
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
};

struct hio_context_t {
  struct hio_object_t context_object;

#if HIO_USE_MPI
  /** internal communicator for this context */
  MPI_Comm            context_comm;
  bool                context_use_mpi;
#endif

  /** my rank in the context */
  int                 context_rank;
  /** numner of ranks using this context */
  int                 context_size;

  /** unreported errors on this context */
  void               *context_error_stack;
  /** threading lock */
  pthread_mutex_t     context_lock;
  /** comma-separated list of data roots available */
  char               *context_data_roots;
  /** expected checkpoint size */
  uint64_t            context_checkpoint_size;
  /** print statistics on close */
  bool                context_print_statistics;
  /** number of bytes written to this context (local) */
  uint64_t            context_bytes_written;
  /** number of bytes read from this context (local) */
  uint64_t            context_bytes_read;
  /** context verbosity */
  uint32_t            context_verbose;
  /** time of last dataset completion */
  struct timeval      context_last_checkpoint;
  /** file configuration for the context */
  hio_config_kv_t    *context_file_configuration;
  int                 context_file_configuration_count;
  int                 context_file_configuration_size;

  /** io modules (one for each data root) */
  hio_module_t        *context_modules[HIO_MAX_DATA_ROOTS];
  /** number of data roots */
  int                  context_module_count;
  /** current active data root */
  int                  context_current_module;
};

struct hio_dataset_t {
  /** allows for type detection */
  struct hio_object_t dataset_object;

  /** dataset identifier */
  uint64_t            dataset_id;
  /** flags used during creation of this dataset */
  hio_flags_t         dataset_flags;
  /** open mode */
  hio_dataset_mode_t  dataset_mode;

  /** context used to create this dataset */
  hio_context_t       dataset_context;

  /** module in use */
  hio_module_t       *dataset_module;

  /** list of elements */
  hio_list_t          dataset_element_list;

  /** local process dataset backing file (relative to data root) */
  char               *dataset_backing_file;
};

struct hio_request_t {
  struct hio_object_t request_object;
  bool         request_complete;
  size_t       request_transferred;
};

typedef struct hio_manifest_segment_t {
  hio_list_t             segment_list;
  uint64_t               segment_file_offset;
  uint64_t               segment_app_offset0;
  uint64_t               segment_app_offset1;
  uint64_t               segment_length;
} hio_manifest_segment_t;

struct hio_element_t {
  struct hio_object_t element_object;

  hio_list_t          element_list;

  int                 element_segment_count;
  hio_list_t          element_segment_list;

  hio_dataset_t       element_dataset;

  bool                element_is_open;
};

/* context functions */

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
                                  hio_flags_t flags, hio_dataset_mode_t mode,
                                  size_t dataset_size);

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

int hioi_element_add_segment (hio_element_t element, off_t file_offset, uint64_t app_offset0,
                              uint64_t app_offset1, size_t segment_length);

int hioi_element_find_offset (hio_element_t element, uint64_t app_offset0, uint64_t app_offset1,
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

int hioi_manifest_deserialize (hio_dataset_t dataset, unsigned char *data, size_t data_size);
int hioi_manifest_load (hio_dataset_t dataset, const char *path);

/* context functions */

static inline bool hioi_context_using_mpi (hio_context_t context) {
#if HIO_USE_MPI
  return context->context_use_mpi;
#endif

  return false;
}

hio_module_t *hioi_context_select_module (hio_context_t context);


#endif /* !defined(HIO_TYPES_H) */
