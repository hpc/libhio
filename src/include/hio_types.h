/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#if !defined(HIO_TYPES_H)
#define HIO_TYPES_H

#include "config.h"
#include "hio_internal.h"
#include "hio_component.h"

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

int hioi_manifest_serialize (hio_dataset_t dataset, unsigned char **data, size_t *data_size);
int hioi_manifest_save (hio_dataset_t dataset, const char *path);

int hioi_manifest_deserialize (hio_dataset_t dataset, unsigned char *data, size_t data_size);
int hioi_manifest_load (hio_dataset_t dataset, const char *path);

#endif /* !defined(HIO_TYPES_H) */
