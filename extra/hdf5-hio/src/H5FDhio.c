/*
 * Programmer:  Hugh Greenberg <hng@lanl.gov> 5/2016
 * Based on HIO-2 I/O driver by Robb Matzke <matzke@llnl.gov>
 *
 * Purpose:  This is the HIO I/O driver
 *
 */

/* Interface initialization */
#define H5_INTERFACE_INIT_FUNC  H5FD_hio_init_interface

#include "H5private.h"    /* Generic Functions      */
#include "H5Dprivate.h"    /* Dataset functions      */
#include "H5Eprivate.h"    /* Error handling        */
#include "H5Fprivate.h"    /* File access        */
#include "H5FDprivate.h"  /* File drivers        */
#include "H5Iprivate.h"    /* IDs            */
#include "H5MMprivate.h"  /* Memory management      */
#include "H5Pprivate.h"         /* Property lists                       */
#include "H5FDhio.h"

char    *HIO_error_str;
size_t  HIO_error_str_len = 256;

#define HHIO_GOTO_ERROR(context) {      				\
  hio_err_get_last ( context, &HIO_error_str );                         \
  HERROR(H5E_INTERNAL, H5E_SYSERRSTR, HIO_error_str);                   \
}

/**
 * The driver identification number, initialized at runtime
 * is defined. This allows applications to still have the H5FD_HIO
 * "constants" in their source code.
 */
static hid_t H5FD_HIO_g = 0;

/**
 * The description of a file belonging to this driver.
 * The EOF value is only used just after the file is opened in order for the
 * library to determine whether the file is empty, truncated, or okay. The HIO
 * driver doesn't bother to keep it updated since it's an expensive operation.
 */
typedef struct H5FD_hio_t {
    H5FD_t  pub;    /*public stuff, must be first    */
    hio_context_t context;    /* hio element */
    hio_element_t element;    /* hio element */
    hio_dataset_t dataset;    /* file information */
    haddr_t       eof;    /*end-of-file marker      */
    haddr_t       eoa;    /*end-of-address marker      */
    haddr_t       last_eoa;  /* Last known end-of-address marker  */
    hio_settings_t *settings;
} H5FD_hio_t;

/* Private Prototypes */

/* Callbacks */
static void *H5FD_hio_fapl_get(H5FD_t *_file);
static void *H5FD_hio_fapl_copy(const void *_old_fa);
static herr_t H5FD_hio_fapl_free(void *_fa);
static H5FD_t *H5FD_hio_open(const char *name, unsigned flags, hid_t fapl_id,
            haddr_t maxaddr);
static herr_t H5FD_hio_close(H5FD_t *_file);
static herr_t H5FD_hio_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD_hio_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t H5FD_hio_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static herr_t H5FD_hio_set_eof(H5FD_hio_t *file);
static haddr_t H5FD_hio_get_eof(const H5FD_t *_file);
static herr_t  H5FD_hio_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void** file_handle);
static herr_t H5FD_hio_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr,
            size_t size, void *buf);
static herr_t H5FD_hio_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr,
            size_t size, const void *buf);
static herr_t H5FD_hio_flush(H5FD_t *_file, hid_t dxpl_id, unsigned closing);
static herr_t H5FD_hio_haddr_to_HIOOff(haddr_t addr, off_t *hio_off);

/** 
 * HIO-specific file access properties 
 */
typedef struct H5FD_hio_fapl_t {
  hio_settings_t *settings;
} H5FD_hio_fapl_t;

/** 
 * The HIO file driver information 
 */
static const H5FD_class_t H5FD_hio_g = {
    "hio",                                /*name         */
    HADDR_MAX,                            /*maxaddr      */
    H5F_CLOSE_SEMI,                       /*fc_degree    */
    NULL,                                 /*sb_size      */
    NULL,                                 /*sb_encode    */
    NULL,                                 /*sb_decode    */
    sizeof(H5FD_hio_fapl_t),              /*fapl_size    */
    H5FD_hio_fapl_get,                    /*fapl_get     */
    H5FD_hio_fapl_copy,                   /*fapl_copy    */
    H5FD_hio_fapl_free,                   /*fapl_free    */
    0,                                    /*dxpl_size    */
    NULL,                                 /*dxpl_copy    */
    NULL,                                 /*dxpl_free    */
    H5FD_hio_open,                        /*open         */
    H5FD_hio_close,                       /*close        */
    NULL,                                 /*cmp          */
    H5FD_hio_query,                       /*query        */
    NULL,                                 /*get_type_map */
    NULL,                                 /*alloc        */
    NULL,                                 /*free         */
    H5FD_hio_get_eoa,                     /*get_eoa      */
    H5FD_hio_set_eoa,                     /*set_eoa      */
    H5FD_hio_get_eof,                     /*get_eof      */
    H5FD_hio_get_handle,                  /*get_handle   */
    H5FD_hio_read,                        /*read         */
    H5FD_hio_write,                       /*write        */
    H5FD_hio_flush,                       /*flush        */
    NULL,                                 /*truncate     */
    NULL,                                 /*lock         */
    NULL,                                 /*unlock       */
    H5FD_FLMAP_DICHOTOMY                  /*fl_map       */
};

/** Initializes any interface-specific data or routines.  (Just calls
 *  H5FD_hio_init currently).
 \return SUCCESS or FAIL
*/
static herr_t
H5FD_hio_init_interface(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(H5FD_hio_init())
} /* H5FD_hio_init_interface() */


/**  Initialize this driver by registering the driver with the
 *   library.
 *
 \return Success:  The driver ID for the hio driver. Failure:  Negative.
 */
hid_t
H5FD_hio_init(void)
{
    hid_t ret_value;        	/* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Register the HIO VFD, if it isn't already */
    if(!H5FD_HIO_g)
        H5FD_HIO_g = H5FD_register((const H5FD_class_t *)&H5FD_hio_g, sizeof(H5FD_class_t), FALSE);

    HIO_error_str = (char *) malloc(HIO_error_str_len);
    /* Set return value */
    ret_value = H5FD_HIO_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_hio_init() */


/**---------------------------------------------------------------------------
 * Function:  H5FD_hio_term
 *
 * Purpose:  Shut down the VFD
 *
 * Return:  <none>
 *
 *---------------------------------------------------------------------------
 */
void
H5FD_hio_term(void)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Reset VFL ID */
    H5FD_HIO_g=0;
    free(HIO_error_str);

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FD_hio_term() */


/**-------------------------------------------------------------------------
 * Function:  H5Pset_fapl_hio
 *
 * Purpose:  Store the user supplied HIO context in
 *    the file access property list FAPL_ID which can then be used
 *    to create and/or open the file.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_hio(hid_t fapl_id, hio_settings_t *settings)
{
    H5FD_hio_fapl_t  fa;
    H5P_genplist_t *plist;      /* Property list pointer */
    herr_t ret_value;

    FUNC_ENTER_API(FAIL)

    if(fapl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list")

    /* Check arguments */
    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access list")

    if (settings) {
	fa.settings = settings;
    }

    /* duplication is done during driver setting. */
    ret_value= H5P_set_driver(plist, H5FD_HIO_g, &fa);

done:
    FUNC_LEAVE_API(ret_value)
}


/**-------------------------------------------------------------------------
 * Function:  H5Pget_fapl_hio
 *
 * Purpose:  If the file access property list is set to the H5FD_HIO
 *    driver then this function returns the HIO context object previously 
 *    set with H5Pset_fapl_hio
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_hio(hid_t fapl_id, hio_settings_t *settings/*out*/)
{
    H5FD_hio_fapl_t  *fa;
    H5P_genplist_t *plist;      /* Property list pointer */
    herr_t      ret_value=SUCCEED;      /* Return value */

    FUNC_ENTER_API(FAIL)

    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access list")
    if(H5FD_HIO_g != H5P_get_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver")
    if(NULL == (fa = (H5FD_hio_fapl_t *)H5P_get_driver_info(plist)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info")

    *settings = *fa->settings;

done:

    FUNC_LEAVE_API(ret_value)
}

/**-------------------------------------------------------------------------                                                                                
 * Function:  H5FD_hio_fapl_get
 *                                                                                                                                                         
 * Purpose:  Returns a file access property list which could be used to                                                                                    
 *    create another file the same as this one.                                                                                                            
 *                                                                                                                                                         
 * Return:  Success:  Ptr to new file access property list with all                                                                                        
 *        fields copied from the file pointer.                                                                                                             
 *                                                                                                                                                         
 *    Failure:  NULL                                                                                                                                       
 *-------------------------------------------------------------------------                                                                                
 */
static void *
H5FD_hio_fapl_get(H5FD_t *_file)
{
  H5FD_hio_t    *file = (H5FD_hio_t*)_file;
  H5FD_hio_fapl_t  *fa = NULL;
  void      *ret_value;       /* Return value */
  
  FUNC_ENTER_NOAPI_NOINIT
    
  HDassert(file);
  HDassert(H5FD_HIO_g == file->pub.driver_id);
  
  if(NULL == (fa = (H5FD_hio_fapl_t *)H5MM_calloc(sizeof(H5FD_hio_fapl_t))))
    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed")
      
  /* Set return value */
  ret_value = fa;

 done:
  FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------                                                                                 
 * Function:  H5FD_hio_fapl_copy                                                                                                                            
 *                                                                                                                                                          
 * Purpose:  Copies the hio-specific file access properties.                                                                                                
 *                                                                                                                                                          
 * Return:  Success:  Ptr to a new property list                                                                                                            
 *                                                                                                                                                          
 *    Failure:  NULL                                                                                                                                        
 *                                                                                                                                                         
 *-------------------------------------------------------------------------                                                                                 
 */
static void *
H5FD_hio_fapl_copy(const void *_old_fa)
{
  void    *ret_value = NULL;
  const H5FD_hio_fapl_t *old_fa = (const H5FD_hio_fapl_t*)_old_fa;
  H5FD_hio_fapl_t  *new_fa = NULL;

  FUNC_ENTER_NOAPI_NOINIT

    if(NULL == (new_fa = (H5FD_hio_fapl_t *)H5MM_malloc(sizeof(H5FD_hio_fapl_t))))
      HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed")

    /* Copy the general information */
    HDmemcpy(new_fa, old_fa, sizeof(H5FD_hio_fapl_t));
    ret_value = new_fa;

 done:
    if (NULL == ret_value){
      /* cleanup */
      if (new_fa)
	H5MM_xfree(new_fa);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_hio_fapl_copy() */

/*-------------------------------------------------------------------------                                                                                 
 * Function:  H5FD_hio_fapl_free                                                                                                                            
 *                                                                                                                                                          
 * Purpose:  Frees the hio-specific file access properties.                                                                                                 
 *                                                                                                                                                          
 * Return:  Success:  0                                                                                                                                     
 *                                                                                                                                                          
 *    Failure:  -1                                                                                                                                          
 *                                                                                                                                                          
 *-------------------------------------------------------------------------                                                                                 
 */
static herr_t
H5FD_hio_fapl_free(void *_fa)
{
    herr_t    ret_value = SUCCEED;
    H5FD_hio_fapl_t  *fa = (H5FD_hio_fapl_t*)_fa;
    
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(fa);
    
    H5MM_xfree(fa);
    
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_hio_fapl_free() */


void H5FD_hio_settings_init(hio_settings_t *settings) {
    memset(settings, 0, sizeof(hio_settings_t));
    settings->read_blocking = H5FD_HIO_BLOCKING;
    settings->write_blocking = H5FD_HIO_BLOCKING;
    settings->dataset_mode = H5FD_HIO_DATASET_SHARED;
    settings->read_io_mode = H5FD_HIO_CONTIGUOUS;
    settings->write_io_mode = H5FD_HIO_CONTIGUOUS;
    settings->stride_size = 1;
    settings->comm = MPI_COMM_NULL;
    settings->setid = 0;
    settings->flags = 0;
}

/*
 * Set the read blocking mode
 * H5FD_HIO_BLOCKING or H5FD_HIO_NONBLOCKING
 */
void H5FD_hio_set_read_blocking(hio_settings_t *settings, H5FD_hio_io_t rb) {
    settings->read_blocking = rb;
}

/*
 * Set the write blocking mode
 * H5FD_HIO_BLOCKING or H5FD_HIO_NONBLOCKING
 */

void H5FD_hio_set_write_blocking(hio_settings_t *settings, H5FD_hio_io_t wb) {
    settings->write_blocking = wb;
}

/*
 * Set the Read IO Mode
 * H5FD_HIO_CONTIGUOUS or H5FD_HIO_STRIDED
 */
void H5FD_hio_set_read_io(hio_settings_t *settings, H5FD_hio_io_t rio) {
    settings->read_io_mode = rio;
}

/*
 * Set the Write IO Mode
 * H5FD_HIO_CONTIGUOUS or H5FD_HIO_STRIDED
 */
void H5FD_hio_set_write_io(hio_settings_t *settings, H5FD_hio_io_t wio) {
    settings->write_io_mode = wio;
}

/*
 * Set the active hio request
 */
void H5FD_hio_set_request(hio_settings_t *settings, hio_request_t *req) {
    settings->request = req;
}

/*
 * Set the MPI communicator to use
 */
void H5FD_hio_set_comm(hio_settings_t *settings, MPI_Comm comm) {
    settings->comm = comm;
}

/*
 * Set the element name
 */
void H5FD_hio_set_elem_name(hio_settings_t *settings, char *en) {
    snprintf(settings->element_name, HIO_ELEM_NAME_SIZE, "%s", en);
}

/*
 * Set the stride size
 */
void H5FD_hio_set_stride(hio_settings_t *settings, size_t stride_size) {
   settings->stride_size = stride_size;
}

/*
 * Set the config file
 */
void H5FD_hio_set_config(hio_settings_t *settings, char *config_file) {
   snprintf(settings->config_file, HIO_CONFIG_FILE_SIZE, "%s", config_file);
}

/*
 * Set the stride size
 */
void H5FD_hio_set_config_prefix(hio_settings_t *settings, char *config_prefix) {
   snprintf(settings->config_prefix, HIO_CONFIG_PREFIX_SIZE, "%s", config_prefix);
}

/*
 * Set the setid
 */
void H5FD_hio_set_setid(hio_settings_t *settings, int64_t setid) {
   settings->setid = setid;
}

/*
 * Set the dataset mode
 * HDF5_HIO_DATASET_SHARED or HDF5_HIO_DATASET_UNIQUE
 * SHARED is the default
 * UNIQUE is faster in my tests, but then 
 * you can't restart with a different number of procs
 */
void H5FD_hio_set_dataset_mode(hio_settings_t *settings, H5FD_hio_io_t mode) {
   settings->dataset_mode = mode;
}


hio_return_t hio_open(const char *name, const char *element_name, 
		      int hio_flags, int dataset_mode, 
		      int64_t set_id, hio_context_t *context, 
		      hio_dataset_t *dataset, hio_element_t *elem) {
    int new_flags = hio_flags;
    hio_return_t ret;

    if(HIO_SUCCESS != (ret = hio_dataset_alloc (*context, dataset, "hdf5hio", set_id, 
						new_flags, dataset_mode))) {
        HHIO_GOTO_ERROR(*context)
        goto error;
    }

    ret = hio_dataset_open (*dataset);
    if (HIO_ERR_EXISTS == ret) {
	  hio_dataset_unlink (*context, "hdf5hio", set_id, HIO_UNLINK_MODE_FIRST);
	  ret = hio_dataset_open (*dataset);
    }
    if(HIO_SUCCESS != ret) {
        HHIO_GOTO_ERROR(*context)
        hio_dataset_free(dataset);
	goto error;
    }
    if (HIO_SUCCESS != (ret = hio_element_open(*dataset, elem, element_name, hio_flags))) {
	HHIO_GOTO_ERROR(*context)
        hio_dataset_close(*dataset);
        hio_dataset_free(dataset);
	goto error;
    }

    return HIO_SUCCESS;

 error:
    return HIO_ERROR;
}

hio_return_t hio_reopen(H5FD_t *_file, int flag) {
  H5FD_hio_t  *file = (H5FD_hio_t*)_file;
  hio_return_t hio_code = HIO_SUCCESS;

  HDassert(file);
  
  if (file->settings->flags & flag)
      goto done;

  if (flag == HIO_FLAG_WRITE) {
      file->settings->flags = HIO_FLAG_WRITE;
  } else {
      file->settings->flags = HIO_FLAG_READ;
  }
  /* Close the element if it exists */
  if (file->element != HIO_OBJECT_NULL &&
      HIO_SUCCESS != (hio_code=hio_element_close(&(file->element)/*in,out*/)))
      HHIO_GOTO_ERROR(file->context)

  if (HIO_SUCCESS != (hio_code=hio_dataset_close(file->dataset/*in,out*/)))
      HHIO_GOTO_ERROR(file->context)
  if (HIO_SUCCESS != (hio_code=hio_dataset_free(&(file->dataset)/*in,out*/)))
      HHIO_GOTO_ERROR(file->context)

  hio_code = hio_open(file->settings->name, file->settings->element_name, 
		      file->settings->flags, file->settings->dataset_mode, 
		      file->settings->setid, &file->context, 
		      &file->dataset, &file->element);

  /* Set the size of the file */
  H5FD_hio_set_eof(file);

 done:
  return hio_code;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_hio_open
 *
 * Purpose:     Opens a file with name NAME.  The FLAGS are a bit field with
 *    purpose similar to the second argument of open(2) and which
 *    are defined in H5Fpublic.h. The file access property list
 *    FAPL_ID contains the properties driver properties and MAXADDR
 *    is the largest address which this file will be expected to
 *    access.  This is collective.
 *
 * Return:      Success:        A new file pointer.
 *
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD_hio_open(const char *name, unsigned flags, hid_t fapl_id,
	       haddr_t H5_ATTR_UNUSED maxaddr)
{
    H5FD_hio_t      *file=NULL;
    hio_dataset_t   dataset;
    hio_element_t   element;
    int             hio_flags;
    int64_t         set_id;
    unsigned        file_opened=0;  /* Flag to indicate that the file was successfully opened */
    const H5FD_hio_fapl_t  *fa=NULL;
    H5FD_hio_fapl_t    _fa;
    H5P_genplist_t *plist;      /* Property list pointer */
    H5FD_t      *ret_value=NULL;     /* Return value */
    hio_return_t rc;
    char *config_file;
    char *config_prefix;
    hio_context_t context;
    int dataset_mode;

    FUNC_ENTER_NOAPI_NOINIT

    /* Obtain a pointer to hio-specific file access properties */
    if(NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list")
    if(H5P_FILE_ACCESS_DEFAULT == fapl_id || H5FD_HIO_g != H5P_get_driver(plist)) {
        fa = &_fa;
    } else {
        if(NULL == (fa = (const H5FD_hio_fapl_t *)H5P_get_driver_info(plist)))
	    HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, NULL, "bad VFL driver info")
    }

    if (strlen(fa->settings->config_file) && strlen(fa->settings->config_prefix)) {
	config_file = fa->settings->config_file;
	config_prefix = fa->settings->config_prefix;
    } else {
	config_file = NULL;
	config_prefix = NULL;
    }

    if (fa->settings->comm != MPI_COMM_NULL) {
      MPI_Barrier(fa->settings->comm);
      if (HIO_SUCCESS != (rc = hio_init_mpi(&context, &fa->settings->comm, 
					    config_file, config_prefix, name))) {
	    HGOTO_ERROR(H5E_RESOURCE, H5E_BADVALUE, NULL, "Error intitializing hio")
      }
    } else if (HIO_SUCCESS != (rc = hio_init_single(&context, 
						    config_file, config_prefix, name))) {
	    HGOTO_ERROR(H5E_RESOURCE, H5E_BADVALUE, NULL, "Error intitializing hio")
    }
    /* convert HDF5 flags to HIO flags */
    /* some combinations are illegal; let HIO figure it out */
    hio_flags  = (flags&H5F_ACC_RDWR) ? HIO_FLAG_WRITE : HIO_FLAG_READ;
    if (flags&H5F_ACC_RDONLY)
	hio_flags |= HIO_FLAG_READ;

    if (hio_flags&HIO_FLAG_WRITE) { 
	hio_flags |= HIO_FLAG_CREAT;
	hio_flags |= HIO_FLAG_TRUNC;
    }

    //Use the user defined setid if it was set. Otherwise, use the fapl_id as the set_id
    if (fa->settings->setid)
      set_id = fa->settings->setid;
    else {
      set_id = fapl_id;
      fa->settings->setid = set_id;
    }

    dataset = HIO_OBJECT_NULL;
    dataset_mode = (fa->settings->dataset_mode == H5FD_HIO_DATASET_SHARED) ?
	HIO_SET_ELEMENT_SHARED : HIO_SET_ELEMENT_UNIQUE;
    if (HIO_SUCCESS != (rc = hio_open(name, fa->settings->element_name,
				      hio_flags, dataset_mode,
				      set_id, &context, &dataset, &element)))
	goto done;

    fa->settings->flags = hio_flags;
    snprintf(fa->settings->name, HIO_FILE_NAME_SIZE, "%s", name);
    file_opened=1;

    /* Build the return value and initialize it */
    if(NULL == (file = (H5FD_hio_t *)H5MM_calloc(sizeof(H5FD_hio_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed")
    file->dataset = dataset;
    file->element = element;
    file->context = context;

    /* Set the hio options */
    file->settings = fa->settings;

    /* Set the size of the file */
    H5FD_hio_set_eof(file);

    /* Set return value */
    ret_value=(H5FD_t*)file;

done:
    if(ret_value==NULL) {
	if(file_opened) {
            hio_dataset_close(dataset);
            hio_dataset_free(&dataset);
            hio_element_close(&element);
	}
	if (file)
	    H5MM_xfree(file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_hio_close
 *
 * Purpose:     Closes a file.  This is collective.
 *
 * Return:      Success:  Non-negative
 *
 *     Failure:  Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_hio_close(H5FD_t *_file)
{
    H5FD_hio_t  *file = (H5FD_hio_t*)_file;
    hio_return_t    hio_code;          /* HIO return code */
    herr_t      ret_value=SUCCEED;      /* Return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(file);
    HDassert(H5FD_HIO_g == file->pub.driver_id);

    /* Close the element if it exists */
    if (file->element != HIO_OBJECT_NULL &&
	HIO_SUCCESS != (hio_code=hio_element_close(&(file->element)/*in,out*/)))
        HHIO_GOTO_ERROR(file->context)

    if (HIO_SUCCESS != (hio_code=hio_dataset_close(file->dataset/*in,out*/)))
        HHIO_GOTO_ERROR(file->context)
    if (HIO_SUCCESS != (hio_code=hio_dataset_free(&(file->dataset)/*in,out*/)))
        HHIO_GOTO_ERROR(file->context)

    if (HIO_SUCCESS != (hio_fini (&file->context)))
        HHIO_GOTO_ERROR(file->context)

    H5MM_xfree(file);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_hio_query
 *
 * Purpose:  Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:  Success:  non-negative
 *
 *    Failure:  negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_hio_query(const H5FD_t H5_ATTR_UNUSED *_file, unsigned long *flags /* out */)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Set the VFL feature flags that this driver supports */
    if(flags) {
        *flags=0;
        *flags|=H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations */
        *flags|=H5FD_FEAT_AGGREGATE_SMALLDATA; /* OK to aggregate "small" raw data allocations */
        *flags|=H5FD_FEAT_ALLOCATE_EARLY;      /* Allocate space early instead of late */
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_hio_get_eoa
 *
 * Purpose:  Gets the end-of-address marker for the file. The EOA marker
 *    is the first address past the last byte allocated in the
 *    format address space.
 *
 * Return:  Success:  The end-of-address marker.
 *
 *    Failure:  HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_hio_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_hio_t  *file = (const H5FD_hio_t*)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(file);
    HDassert(H5FD_HIO_g == file->pub.driver_id);

    FUNC_LEAVE_NOAPI(file->eoa)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_hio_set_eoa
 *
 * Purpose:  Set the end-of-address marker for the file. This function is
 *    called shortly after an existing HDF5 file is opened in order
 *    to tell the driver where the end of the HDF5 data is located.
 *
 * Return:  Success:  0
 *
 *    Failure:  -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_hio_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_hio_t  *file = (H5FD_hio_t*)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(file);
    HDassert(H5FD_HIO==file->pub.driver_id);

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
}

static herr_t
H5FD_hio_set_eof(H5FD_hio_t *file)
{
    int num_ranks;
    int64_t *recvbuf, recvsize;
    int64_t elem_size;
    int64_t total_size;
    hio_return_t hio_code;
    herr_t ret_value=SUCCEED;
    int mpi_code, i;

    FUNC_ENTER_NOAPI_NOINIT

    if (HIO_SUCCESS != (hio_code = hio_element_size(file->element, &elem_size)))
        HHIO_GOTO_ERROR(file->context)

    if (file->settings->comm != MPI_COMM_NULL) {
	MPI_Comm_size(file->settings->comm, &num_ranks);
	if (file->settings->dataset_mode == H5FD_HIO_DATASET_SHARED) {
	    if ((mpi_code = 
		 MPI_Allreduce(&elem_size, &total_size, 1, MPI_LONG_LONG_INT, MPI_MAX, 
			       file->settings->comm)) != MPI_SUCCESS) {
		HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Did not receive a valid element size from all ranks")
		    ret_value = FAIL;
		goto done;
	    }
	} else {
	    recvsize = num_ranks * sizeof(int64_t);
	    if (NULL == (recvbuf = (int64_t *)H5MM_calloc(recvsize)))
		HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed")
		    
	    if ((mpi_code = MPI_Allgather(&elem_size, 1, MPI_LONG_LONG_INT, recvbuf, 1,
					  MPI_LONG_LONG_INT, file->settings->comm)) != MPI_SUCCESS) {
		HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Did not receive a valid element size from all ranks")
		    ret_value = FAIL;
		goto done;
	    }

	    total_size = 0;
	    for (i = 0; i < num_ranks; i++) {
		total_size += recvbuf[i];
	    }
    
	    H5MM_xfree(recvbuf);
	}

	file->eof = total_size;
    } else {
	file->eof = elem_size;
    }

 done:

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_hio_get_eof
 *
 * Purpose:  Gets the end-of-file marker for the file. The EOF marker
 *    is the real size of the file.
 *
 *    The HIO driver doesn't bother keeping this field updated
 *    since that's a relatively expensive operation. Fortunately
 *    the library only needs the EOF just after the file is opened
 *    in order to determine whether the file is empty, truncated,
 *    or okay.  Therefore, any HIO I/O function will set its value
 *    to HADDR_UNDEF which is the error return value of this
 *    function.
 *
 *              Keeping the EOF updated (during write calls) is expensive
 *              because any process may extend the physical end of the
 *              file. -QAK
 *
 * Return:  Success:  The end-of-address marker.
 *
 *    Failure:  HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_hio_get_eof(const H5FD_t *_file)
{
    const H5FD_hio_t  *file = (const H5FD_hio_t*)_file;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(file);
    HDassert(H5FD_HIO_g == file->pub.driver_id);

    FUNC_LEAVE_NOAPI(file->eof)
}

/*-------------------------------------------------------------------------
 * Function:       H5FD_hio_get_handle
 *
 * Purpose:        Returns the file handle of HIO file driver.
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 *-------------------------------------------------------------------------
*/

static herr_t
H5FD_hio_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void** file_handle)
{
    H5FD_hio_t         *file = (H5FD_hio_t *)_file;
    herr_t              ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    if(!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid")

    *file_handle = &(file->element);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:       H5FD_hio_haddr_toHIOOff
 *
 * Purpose:        Converts the HDF5 address into an HIO offset
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 *-------------------------------------------------------------------------
*/

herr_t
H5FD_hio_haddr_to_HIOOff(haddr_t addr, off_t *hio_off/*out*/)
{
    herr_t ret_value=FAIL;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(hio_off);

    /* Convert the HDF5 address into an HIO offset */
    *hio_off = (off_t)addr;

    if (addr != (haddr_t)((off_t)addr))
        ret_value=FAIL;
    else
        ret_value=SUCCEED;

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_hio_read
 *
 * Purpose:  Reads SIZE bytes of data from FILE beginning at address ADDR
 *    into buffer BUF according to data transfer properties in
 *    DXPL_ID
 *
 * Return:  Success:  Zero. Result is stored in caller-supplied
 *        buffer BUF.
 *
 *    Failure:  -1, Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_hio_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t dxpl_id, haddr_t addr, size_t size,
         void *buf/*out*/)
{
    H5FD_hio_t      *file = (H5FD_hio_t*)_file;
    int             size_i;         /* Integer copy of 'size' to read */
    ssize_t         bytes_read;     /* Number of bytes read in */
    herr_t                ret_value = SUCCEED;
    off_t hio_off;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file);
    HDassert(H5FD_HIO_g == file->pub.driver_id);
    /* Make certain we have the correct type of property list */
    HDassert(H5I_GENPROP_LST==H5I_get_type(dxpl_id));
    HDassert(TRUE==H5P_isa_class(dxpl_id,H5P_DATASET_XFER));
    HDassert(buf);

    hio_reopen(_file, HIO_FLAG_READ);
    /* some numeric conversions */
    hio_off = (off_t) addr;
    if (H5FD_hio_haddr_to_HIOOff(addr, &hio_off/*out*/)<0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to HIO off")
    size_i = (int)size;
    if ((hsize_t)size_i != size)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from size to size_i")

    if (file->settings->read_blocking == H5FD_HIO_BLOCKING) {
	if (file->settings->read_io_mode == H5FD_HIO_CONTIGUOUS && 
	    (bytes_read=hio_element_read(file->element, hio_off, 0, buf, 1, size)) < 0) {
	    HHIO_GOTO_ERROR(file->context)
	} else if (file->settings->read_io_mode == H5FD_HIO_STRIDED && 
		   (bytes_read=hio_element_read_strided(file->element, hio_off, 0, buf, 1, 
							size, file->settings->stride_size)) < 0) {
	    HHIO_GOTO_ERROR(file->context)
      }
    } else {
	if (file->settings->read_io_mode == H5FD_HIO_CONTIGUOUS && 
	    file->settings->request && 
	    (bytes_read = 
	     hio_element_read_nb(file->element, file->settings->request, hio_off, 0, buf, 1, size)) < 0) {
	    HHIO_GOTO_ERROR(file->context)
	} else if (file->settings->read_io_mode == H5FD_HIO_STRIDED && 
		   file->settings->request && 
		   (bytes_read = 
		    hio_element_read_strided_nb(file->element, file->settings->request, hio_off, 0, buf, 1, size, 
						file->settings->stride_size)) < 0) {
	    HHIO_GOTO_ERROR(file->context)
	} else if (!file->settings->request) {
	    HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "HIO Request not set")
	}
    }

    /* Check for read failure */
    if (bytes_read<0)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed")

done:

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:  H5FD_hio_write
 *
 * Purpose:  Writes SIZE bytes of data to FILE beginning at address ADDR
 *    from buffer BUF according to data transfer properties in
 *    DXPL_ID
 *
 * Return:  Success:  Zero. USE_TYPES and OLD_USE_TYPES in the
 *        access params are altered.
 *
 *    Failure:  -1, USE_TYPES and OLD_USE_TYPES in the
 *        access params may be altered.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_hio_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr,
    size_t size, const void *buf)
{
    H5FD_hio_t      *file = (H5FD_hio_t*)_file;
    off_t           hio_off;
    ssize_t         bytes_written;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    HDassert(file);
    HDassert(H5FD_HIO_g == file->pub.driver_id);
    /* Make certain we have the correct type of property list */
    HDassert(H5I_GENPROP_LST==H5I_get_type(dxpl_id));
    HDassert(TRUE==H5P_isa_class(dxpl_id,H5P_DATASET_XFER));
    HDassert(buf);

    hio_reopen(_file, HIO_FLAG_WRITE);
    /* some numeric conversions */
    if(H5FD_hio_haddr_to_HIOOff(addr, &hio_off) < 0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to HIO off")

    if (file->settings->write_blocking == H5FD_HIO_BLOCKING) {
      if ((bytes_written=hio_element_write(file->element, hio_off, 0, buf, 1, size)) < 0) {
	  HHIO_GOTO_ERROR(file->context)
	}
    } else {
	if (file->settings->request && 
	    (bytes_written = 
	     hio_element_write_nb(file->element, file->settings->request, hio_off, 0, buf, 1, size)) < 0) {
	      HHIO_GOTO_ERROR(file->context)
	} else if (!file->settings->request) {
	  HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "HIO Request not set")
	}
    }
      
done:

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_hio_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_hio_flush
 *
 * Purpose:     Makes sure that all data is on disk.
 *
 * Return:      Success:  Non-negative
 *
 *     Failure:  Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_hio_flush(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, unsigned closing)
{
    H5FD_hio_t    *file = (H5FD_hio_t*)_file;
    herr_t              ret_value = SUCCEED;
    hio_return_t        hio_code;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    HDassert(file);
    HDassert(H5FD_HIO_g == file->pub.driver_id);

    hio_reopen(_file, HIO_FLAG_WRITE);

    if(HIO_SUCCESS != (hio_code = hio_element_flush(file->element, HIO_FLUSH_MODE_COMPLETE)))
	HHIO_GOTO_ERROR(file->context)
    if(HIO_SUCCESS != (hio_code = hio_dataset_flush (file->dataset, HIO_FLUSH_MODE_COMPLETE )))
	HHIO_GOTO_ERROR(file->context)

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_hio_flush() */
