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

/**
 * @file context.h
 * @brief Internal hio context
 */

#if !defined(HIO_INTERNAL_H)
#define HIO_INTERNAL_H

#include "hio_config.h"

#include "hio_types.h"

#include <stddef.h>
#include <inttypes.h>

#if defined(HAVE_SYS_TIME_H)
#include <sys/time.h>
#endif

/**
 * Verbosity levels
 */
enum {
  HIO_VERBOSE_ERROR      = 0,
  HIO_VERBOSE_WARN       = 10,
  HIO_VERBOSE_DEBUG_LOW  = 20,
  HIO_VERBOSE_DEBUG_MED  = 50,
  HIO_VERBOSE_DEBUG_HIGH = 90,
  HIO_VERBOSE_MAX        = 100,
};

/**
 * @brief Push an hio error onto the hio error stack
 *
 * @param[in] hrc     hio error code
 * @param[in] context hio context
 * @param[in] object  hio object in use at the time of the error
 * @param[in] format  error string to push
 * @param[in] ...     error string arguments
 *
 * This function pushes the specified error string onto the error stack.
 */
void hioi_err_push (int hrc, hio_object_t object, char *format, ...);

/**
 * @brief Push an MPI error onto the hio error stack
 *
 * @param[in] mpirc   MPI error code
 * @param[in] context hio context
 * @param[in] object  hio object in use at the time of the error
 * @param[in] format  error string to push
 * @param[in] ...     error string arguments
 *
 * This function pushes the specified error string onto the error stack
 * and appends the MPI error string.
 */
void hioi_err_push_mpi (int mpirc, hio_object_t object, char *format, ...);

/**
 * @brief Return hio error code for MPI error code.
 *
 * @param[in] mpirc   MPI error code
 *
 * @returns hio error code that is equivalent to the mpi error code
 *
 * This is a helper function that will give the closest hio error code to
 * the provided mpi error code.
 */
int hioi_err_mpi (int mpirc);

/**
 * Log a message to stderr.  Don't invoke directly, use hioi_log macro.
 *
 * @param[in] context  current context
 * @param[in] level    message log level
 * @param[in] format   output format
 * @param[in] ...      format arguments
 */
void hioi_log_unconditional (hio_context_t context, int level, char *format, ...);
 
#define hioi_log(context, level,  ...)                            \
  if ((context)->c_verbose >= level) {                      \
    hioi_log_unconditional ( (context), (level), __VA_ARGS__);    \
  }

/**
 * Return an hio error code for the given errno
 *
 * @param[in] err   error code
 *
 * @returns hio error code
 */
int hioi_err_errno (int err);


/**
 * Create HIO dataset modules based on the current data roots
 *
 * @param[in] context  context
 *
 * @returns hio error code
 */
int hioi_context_create_modules (hio_context_t context);

/**
 * Get the current time (relative to system boot) in usec
 *
 * @returns monotonically increasing time in usec
 */
uint64_t hioi_gettime (void);

/**
 * Make the component directories of the specified path
 *
 * @param[in] context     context - used for logging
 * @param[in] path        path to make
 * @param[in] access_mode permissions
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERROR on error
 *
 * Additional information on failures can be read from the
 * errno global variable. See the man page for mkdir(2) for
 * more information.
 */
int hio_mkpath (hio_context_t context, const char *path, mode_t access_mode);

/**
 * Share a string with all processes in a context
 *
 * @param[in]     context hio context
 * @param[in,out] string  string pointer to share from/store in
 *
 * This function shares a string with all processes in a context. On processes
 * with rank other than 0 the existing string is freed (if *string is non-NULL)
 * and space is allocated for the new string.
 *
 * @note When using MPI this call will make two calls to MPI_Bcast so it is
 *       required that all processes in the context call this function.
 */
int hioi_string_scatter (hio_context_t context, char **string);

/**
 * Calculate CRC32 of buffer
 *
 * @param[in] buf     buffer to CRC
 * @param[in] length  length of buffer
 *
 * @return CRC32 checksum
 */
uint32_t hioi_crc32 (uint8_t *buf, size_t length);

/**
 * Calculate CRC64 of buffer
 *
 * @param[in] buf     buffer to CRC
 * @param[in] length  length of buffer
 *
 * @return CRC64 checksum
 */
uint64_t hioi_crc64 (uint8_t *buf, size_t length);

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

#define hioi_object_identifier(object) ((hio_object_t) object)->identifier

/**
 * Macro to get the dataset for an hio element
 */
#define hioi_element_dataset(e) (hio_dataset_t) (e)->e_object.parent


static inline void hioi_object_lock (hio_object_t object) {
  pthread_mutex_lock (&object->lock);
}

static inline void hioi_object_unlock (hio_object_t object) {
  pthread_mutex_unlock (&object->lock);
}

hio_object_t hioi_object_alloc (const char *name, hio_object_type_t type, hio_object_t parent,
                                size_t object_size, hio_object_release_fn_t);

void hioi_object_release (hio_object_t object);

/**
 * Allocate a new dataset object and populate it with common data (internal)
 *
 * @param[in] name         dataset name
 * @param[in] id           id of this dataset instance
 * @param[in] flags        flags for this dataset instance
 * @param[in] mode         offset mode of this dataset
 *
 * @returns hio dataset object on success
 * @returns NULL on failure
 *
 * This function generates a generic dataset object and populates
 * the shared fields. The module should populate private data if
 * needed.
 *
 * This function may or may not appear in the final release of
 * libhio. It may become the responsibility of the hio module to
 * allocate the memory it needs to implement a dataset (including
 * the shared bit above).
 */
hio_dataset_t hioi_dataset_alloc (hio_context_t context, const char *name, int64_t id,
                                  int flags, hio_dataset_mode_t mode);

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
hio_element_t hioi_element_alloc (hio_dataset_t dataset, const char *name, const int rank);

hio_request_t hioi_request_alloc (hio_context_t context);

void hioi_request_release (hio_request_t request);

int hioi_element_add_segment (hio_element_t element, int file_index, off_t file_offset, uint64_t app_offset,
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
int hioi_manifest_serialize (hio_dataset_t dataset, unsigned char **data, size_t *data_size, bool compress_data);

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

int hioi_dataset_open_internal (hio_module_t *module, hio_dataset_t dataset);
int hioi_dataset_close_internal (hio_dataset_t dataset);
int hioi_dataset_add_file (hio_dataset_t dataset, const char *filename);

int hioi_element_open_internal (hio_dataset_t dataset, hio_element_t *element_out, const char *element_name,
                                int flags, int rank);
int hioi_element_close_internal (hio_element_t element);

static inline bool hioi_dataset_doing_io (hio_dataset_t dataset) {
  return true;
}

#endif /* !defined(HIO_INTERNAL_H) */
