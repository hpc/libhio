/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/**
 * @file H5FDhio.h
 * @brief API for HDF5 HIO plugin
 *
 * This file describes the purpose and API of the HDF5 HIO plugion.
 */

/**
 * @mainpage HDF5 HIO Plugin
 *
 * @section about About HDF5 HIO plugin
 *
 * libhio is a library intended for writing data to hierarchical data
 * store systems. These systems may be comprised of one or more logical layers
 * including parallel file systems, burst buffers, and local memory.
 * libhio provides support for automated fall-back on alternate destinations if
 * part of the storage hierarchy becomes unavailable.
 *
 * The HDF5 HIO plugin allows HDF5 to use HIO as the file driver.
 * The user does not have to use HIO directly. Instead HDF5 is used as normal
 * and is initialized to use the HIO plugin. The plugin then uses HIO to do the
 * actual IO for HDF5.
 *
 * @section api API
 * The API is used for setting options for the plugin. The settings structure needs
 * to first be initialized: @code                                                                                                                                H5FD_hio_settings_init(settings); 
   @endcode                     
 * Then the options can be set with the H5FD_hio_set functions. See src/hdf5_hio_example.c
 * for a full example.
 */

#include <hio.h>
#include <mpi.h>
/*
 * Programmer:  Robb Matzke <matzke@llnl.gov>
 *              Monday, August  2, 1999
 *
 * Purpose:	The public header file for the hio driver.
 */
#ifndef H5FDhio_H
#define H5FDhio_H

/* Macros */


#   define H5FD_HIO	(H5FD_hio_init())

/*Turn on H5FDhio_debug if H5F_DEBUG is on */
#ifdef H5F_DEBUG
#ifndef H5FDhio_DEBUG
#define H5FDhio_DEBUG
#endif
#endif

/* Global var whose value comes from environment variable */
/* (Defined in H5FDhio.c) */
H5_DLLVAR hbool_t H5FD_hio_opt_types_g;

/* Function prototypes */
#ifdef __cplusplus
extern "C" {
#endif
typedef enum H5FD_hio_io_t {
  H5FD_HIO_BLOCKING = 0,          /*zero is the default*/
  H5FD_HIO_NONBLOCKING,
  H5FD_HIO_CONTIGUOUS,
  H5FD_HIO_STRIDED,
  H5FD_HIO_DATASET_SHARED,
  H5FD_HIO_DATASET_UNIQUE
} H5FD_hio_io_t;

#define HIO_FILE_NAME_SIZE 1024
#define HIO_ELEM_NAME_SIZE 256
#define HIO_CONFIG_FILE_SIZE 128
#define HIO_CONFIG_PREFIX_SIZE 256
typedef struct hio_settings_t {
  int read_blocking;
  int write_blocking;
  int read_io_mode;
  int write_io_mode;
  int dataset_mode;
  size_t stride_size;
  hio_request_t *request;
  char name[HIO_FILE_NAME_SIZE];
  char element_name[HIO_ELEM_NAME_SIZE];
  char config_file[HIO_CONFIG_FILE_SIZE];
  char config_prefix[HIO_CONFIG_PREFIX_SIZE];
  MPI_Comm comm;
  int64_t setid;
  int flags;
} hio_settings_t;  

H5_DLL hid_t H5FD_hio_init(void);
H5_DLL void H5FD_hio_term(void);
  /** Set the HIO file access properties
   * \param settings a settings structure set with the hio_settings* functions
   * \return SUCCESS or FAIL
   */
H5_DLL herr_t H5Pset_fapl_hio(hid_t fapl_id, hio_settings_t *settings);

  /** Get the HIO file access properties
   * \param fapl_id the file acccess properties id
   * \param settings a pointer to the settings structure
   * \return SUCCESS or FAIL
   */
H5_DLL herr_t H5Pget_fapl_hio(hid_t fapl_id, hio_settings_t *settings/*out*/);
H5_DLL void H5FD_hio_settings_init(hio_settings_t *);
  /** Set the HIO read blocking mode
   * \param settings a pointer to the settings structure
   * \param read_blocking H5FD_HIO_BLOCKING or H5FD_HIO_NONBLOCKING
   */
H5_DLL void H5FD_hio_set_read_blocking(hio_settings_t *settings, H5FD_hio_io_t read_blocking);
  /** Set the HIO write blocking mode
   * \param settings a pointer to the settings structure
   * \param write_blocking H5FD_HIO_BLOCKING or H5FD_HIO_NONBLOCKING
   */
H5_DLL void H5FD_hio_set_write_blocking(hio_settings_t *settings, H5FD_hio_io_t write_blocking);
  /** Set the HIO read IO mode
   * \param settings a pointer to the settings structure
   * \param read_io_mode H5FD_HIO_STRIDED or H5FD_HIO_CONTIGUOUS
   */
H5_DLL void H5FD_hio_set_read_io(hio_settings_t *settings, H5FD_hio_io_t read_io_mode);
  /** Set the HIO write IO mode
   * \param settings a pointer to the settings structure
   * \param write_io_mode H5FD_HIO_STRIDED or H5FD_HIO_CONTIGUOUS
   */
H5_DLL void H5FD_hio_set_write_io(hio_settings_t *settings, H5FD_hio_io_t write_io_mode);
  /** Set the HIO request
   * \param settings a pointer to the settings structure
   * \param request a pointer to an HIO request for async operations
   */
H5_DLL void H5FD_hio_set_request(hio_settings_t *settings, hio_request_t *request);
  /** Set the HIO element name
   * \param settings a pointer to the settings structure
   * \param elem_name The element name to use
   */
H5_DLL void H5FD_hio_set_elem_name(hio_settings_t *settings, char *elem_name);
  /** Set the HIO MPI communicator
   * \param settings a pointer to the settings structure
   * \param comm The MPI communicator to use if using MPI
   */
H5_DLL void H5FD_hio_set_comm(hio_settings_t *settings, MPI_Comm comm);
  /** Set the HIO stride size
   * \param settings a pointer to the settings structure
   * \param stride the stride size
   */
H5_DLL void H5FD_hio_set_stride(hio_settings_t *settings, size_t stride_size);
  /** Set the HIO setid
   * \param settings a pointer to the settings structure
   * \param set_id the HIO setid to use
   */
H5_DLL void H5FD_hio_set_setid(hio_settings_t *settings, int64_t set_id);
  /** Set the HIO dataset mode
   * \param settings a pointer to the settings structure
   * \param dataset_mode H5FD_HIO_SHARED or H5FD_HIO_UNIQUE
   */
H5_DLL void H5FD_hio_set_dataset_mode(hio_settings_t *settings, H5FD_hio_io_t dataset_mode);
  /** Set the HIO config file name
   * \param settings a pointer to the settings structure
   * \param config_file the config file name
   */
H5_DLL void H5FD_hio_set_config(hio_settings_t *settings, char *config_file);
  /** Set the HIO config file directory prefix
   * \param settings a pointer to the settings structure
   * \param config_prefix the config file directory prefix
   */
H5_DLL void H5FD_hio_set_config_prefix(hio_settings_t *settings, char *config_prefix);
#ifdef __cplusplus
}
#endif


#endif
