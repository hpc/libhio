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

#if !defined(HIO_MANIFEST_H)
#define HIO_MANIFEST_H

#include "hio_internal.h"

#include <json.h>

#define HIO_MANIFEST_KEY_VERSION     "hio_manifest_version"
#define HIO_MANIFEST_KEY_COMPAT      "hio_manifest_compat"
#define HIO_MANIFEST_KEY_IDENTIFIER  "identifier"
#define HIO_MANIFEST_KEY_DATASET_ID  "dataset_id"
#define HIO_MANIFEST_KEY_SIZE        "size"
#define HIO_MANIFEST_KEY_HIO_VERSION "hio_version"
#define HIO_MANIFEST_KEY_RANK        "rank"
#define HIO_MANIFEST_KEY_CONFIG      "config"
#define HIO_MANIFEST_KEY_PERF        "perf"

#define HIO_MANIFEST_KEY_DATASET_MODE "hio_dataset_mode"
#define HIO_MANIFEST_KEY_FILE_MODE    "hio_file_mode"
#define HIO_MANIFEST_KEY_BLOCK_SIZE   "block_size"
#define HIO_MANIFEST_KEY_FILE_COUNT   "file_count"
#define HIO_MANIFEST_KEY_MTIME        "hio_mtime"
#define HIO_MANIFEST_KEY_COMM_SIZE    "hio_comm_size"
#define HIO_MANIFEST_KEY_STATUS       "hio_status"
#define HIO_MANIFEST_KEY_ELEMENTS     "elements"
#define HIO_MANIFEST_KEY_SEGMENTS     "segments"
#define HIO_SEGMENT_KEY_FILE_OFFSET   "loff"
#define HIO_SEGMENT_KEY_APP_OFFSET0   "off"
#define HIO_SEGMENT_KEY_LENGTH        "len"
#define HIO_SEGMENT_KEY_FILE_INDEX    "findex"

struct hio_manifest {
  hio_context_t context;
  json_object *json_object;
};

/**
 * @brief Generate a new manifest object from an hio dataset object
 *
 * @param[in] dataset   libhio dataset
 * @param[in] simple    boolean indicating whether to generate a simple manifest
 *
 * This function prepares an hio manifest based on the data stored in an hio
 * dataset object. If simple is set to true then the resulting manifest will
 * not contain any data on the dataset's elements.
 */
int hioi_manifest_generate (hio_dataset_t dataset, bool simple, hio_manifest_t *manifest);

/**
 * @brief Create an empty manifest object
 *
 * @param[in] context  associated hio context
 */
hio_manifest_t hioi_manifest_create (hio_context_t context);

/**
 * @brief Generate a new manifest object from serialize manifest data
 *
 * @param[in] context   associated hio context
 * @param[in] data      serialized manifest data
 * @param[in] data_size length of the serialized manifest data
 */
int hioi_manifest_deserialize (hio_context_t context, const unsigned char *data, const size_t data_size, hio_manifest_t *manifest_out);

/**
 * @brief Generate a new manifest object from serialized manifest data in a file
 *
 * @param[in] context  associated hio context
 * @param[in] path     location of serialized manifest data
 */
int hioi_manifest_read (hio_context_t context, const char *path, hio_manifest_t *manifest_out);

/**
 * @brief Apply manifest data to an hio dataset
 *
 * @param[in] dataset  dataset to update
 * @param[in] manifest hio manifest object
 */
int hioi_manifest_load (hio_dataset_t dataset, hio_manifest_t manifest);

/**
 * @brief Release an hio manifest object
 *
 * @param[in] manifest  manifest to release
 *
 * This function releases the storage associated with a manifest object. After this
 * function returns the manifest object is no longer valid.
 */
void hioi_manifest_release (hio_manifest_t manifest);

/**
 * @brief Serialize the manifest in the dataset
 *
 * @param[in]  dataset       dataset to serialize
 * @param[out] data          serialized data
 * @param[out] data_size     size of serialized data
 * @param[in]  compress_data if true will use bzip2 compression
 * @param[in]  simple        return only the manifest header data
 *
 * This function serializes the local data associated with the dataset and returns a buffer
 * containing the serialized data.
 */
int hioi_manifest_serialize (hio_manifest_t manifest, unsigned char **data, size_t *data_size, bool compress_data);

/**
 * @brief Serialize the manifest in the dataset and save it to the specified file
 *
 * @param[in]  manifest      manifest to save
 * @param[in]  compress_data boolean indicating whether to bzip compress the manifest
 * @param[in]  path          file to save the manifest into
 *
 * This function serializes a manifest and saves it to a specific path.
 */
int hioi_manifest_save (hio_manifest_t manifest, bool compress_data, const char *path);

/**
 * @brief Merge hio manifests
 *
 * @param[in] manifest  manifest to update
 * @param[in] manifest2 manifest to merge
 */
int hioi_manifest_merge_data (hio_manifest_t manifest, hio_manifest_t manifest2);

/**
 * Determine what which ranks have data in the manifest
 *
 * @param[in]  manifest      hio manifest object
 * @param[out] ranks         ranks that have data in this manifest
 * @param[out] rank_count    number of elements in the ranks array
 */
int hioi_manifest_ranks (hio_manifest_t manifest, int **ranks, int *rank_count);

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
int hioi_manifest_read_header (hio_manifest_t manifest, hio_dataset_header_t *header);

int hioi_manifest_dump_file (hio_context_t context, const char *path, uint32_t flags, int rank, FILE *fh);
int hioi_manifest_dump (hio_manifest_t manifest, uint32_t flags, int rank, FILE *fh);

#if HIO_MPI_HAVE(1)
int hioi_manifest_gather_comm (hio_manifest_t *manifest, MPI_Comm comm);
int hioi_manifest_scatter_comm (hio_manifest_t manifest, MPI_Comm comm, int rc);
#endif /* HIO_MPI_HAVE(1) */

/* manifest internal functions */

int hioi_manifest_get_string (const json_object *parent, const char *name, const char **string);
int hioi_manifest_get_number (const json_object *parent, const char *name, unsigned long *value);
int hioi_manifest_get_signed_number (const json_object *parent, const char *name, long *value);

#endif /* !defined(HIO_MANIFEST_H) */
