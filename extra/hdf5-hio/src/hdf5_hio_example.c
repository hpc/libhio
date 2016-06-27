/*
 * Example of using the HIO HDF5 library to access datasets.
 * Last revised: June 13, 2016.
 *
 * This program contains two parts.  In the first part, HIO
 * collectively create a new HDF5 file and create two fixed
 * dimension datasets in it.  Then each process writes a hyperslab into
 * each dataset in an independent mode.  All processes collectively
 * close the datasets and the file.
 * In the second part, the processes collectively open the created file
 * and the two datasets in it.  Then each process reads a hyperslab from
 * each dataset in an independent mode and prints them out.
 * All processes collectively close the datasets and the file.
 *
 */

#include <assert.h>
#include <hdf5.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include "H5FDhio.h"

/* Temporary source code */
#define FAIL -1
/* temporary code end */

/* Define some handy debugging shorthands, routines, ... */
/* debugging tools */
#define MESG(x)\
	if (verbose) printf("%s\n", x);\

#define MPI_BANNER(mesg)\
    {printf("--------------------------------\n");\
    printf("Proc %d: ", mpi_rank); \
    printf("*** %s\n", mesg);\
    printf("--------------------------------\n");}

/* End of Define some handy debugging shorthands, routines, ... */

/* Constants definitions */
/* 24 is a multiple of 2, 3, 4, 6, 8, 12.  Neat for parallel tests. */
#define SPACE1_DIM1	24
#define SPACE1_DIM2	24
#define SPACE1_RANK	2
#define DATASETNAME1	"Data1"
#define DATASETNAME2	"Data2"
#define DATASETNAME3	"Data3"
/* hyperslab layout styles */
#define BYROW		1	/* divide into slabs of rows */
#define BYCOL		2	/* divide into blocks of columns */

#define PARAPREFIX	"HDF5_PARAPREFIX"	/* file prefix environment variable name */


/* dataset data type.  Int's can be easily octo dumped. */
typedef int DATATYPE;

/* global variables */
int nerrors = 0;				/* errors count */
#ifndef PATH_MAX
#define PATH_MAX    512
#endif  /* !PATH_MAX */

int mpi_size, mpi_rank;				/* mpi variables */

hio_settings_t settings;

/* option flags */
int verbose = 0;			/* verbose, default as no. */
int single = 0;			/* hio single mode, default as no. */
/* Prototypes */
void slab_set(hsize_t start[], hsize_t count[], hsize_t stride[], int mode);
void dataset_fill(hsize_t start[], hsize_t count[], hsize_t stride[], DATATYPE * dataset);
void dataset_print(hsize_t start[], hsize_t count[], hsize_t stride[], DATATYPE * dataset);
int dataset_vrfy(hsize_t start[], hsize_t count[], hsize_t stride[], DATATYPE *dataset, DATATYPE *original);
void phdf5write1(char *filename, hio_context_t context);
void phdf5read1(char *filename, hio_context_t context);
void phdf5write2(char *filename, hio_context_t context);
void phdf5read2(char *filename, hio_context_t context);
int parse_options(int argc, char **argv);
void usage(void);
int mkfilenames(char *prefix);
void cleanup(void);


/*
 * Setup the dimensions of the hyperslab.
 * Two modes--by rows or by columns.
 * Assume dimension rank is 2.
 */
void
slab_set(hsize_t start[], hsize_t count[], hsize_t stride[], int mode)
{
    switch (mode){
    case BYROW:
	/* Each process takes a slabs of rows. */
	stride[0] = 1;
	stride[1] = 1;
	count[0] = SPACE1_DIM1/mpi_size;
	count[1] = SPACE1_DIM2;
	start[0] = mpi_rank*count[0];
	start[1] = 0;
	break;
    case BYCOL:
	/* Each process takes a block of columns. */
	stride[0] = 1;
	stride[1] = 1;
	count[0] = SPACE1_DIM1;
	count[1] = SPACE1_DIM2/mpi_size;
	start[0] = 0;
	start[1] = mpi_rank*count[1];
	break;
    default:
	/* Unknown mode.  Set it to cover the whole dataset. */
	printf("unknown slab_set mode (%d)\n", mode);
	stride[0] = 1;
	stride[1] = 1;
	count[0] = SPACE1_DIM1;
	count[1] = SPACE1_DIM2;
	start[0] = 0;
	start[1] = 0;
	break;
    }
}


/*
 * Fill the dataset with trivial data for testing.
 * Assume dimension rank is 2 and data is stored contiguous.
 */
void
dataset_fill(hsize_t start[], hsize_t count[], hsize_t stride[], DATATYPE * dataset)
{
    DATATYPE *dataptr = dataset;
    hsize_t i, j;

    /* put some trivial data in the data_array */
    for (i=0; i < count[0]; i++){
	for (j=0; j < count[1]; j++){
	    *dataptr++ = (i*stride[0]+start[0])*100 + (j*stride[1]+start[1]+1);
	}
    }
}


/*
 * Print the content of the dataset.
 */
void dataset_print(hsize_t start[], hsize_t count[], hsize_t stride[], DATATYPE * dataset)
{
    DATATYPE *dataptr = dataset;
    hsize_t i, j;

    /* print the slab read */
    for (i=0; i < count[0]; i++){
	printf("Row %lu: ", (unsigned long)(i*stride[0]+start[0]));
	for (j=0; j < count[1]; j++){
	    printf("%03d ", *dataptr++);
	}
	printf("\n");
    }
}


/*
 * Print the content of the dataset.
 */
int dataset_vrfy(hsize_t start[], hsize_t count[], hsize_t stride[], DATATYPE *dataset, DATATYPE *original)
{
#define MAX_ERR_REPORT	10		/* Maximum number of errors reported */

    hsize_t i, j;
    int nerr;

    /* print it if verbose */
    if (verbose)
	dataset_print(start, count, stride, dataset);

    nerr = 0;
    for (i=0; i < count[0]; i++){
	for (j=0; j < count[1]; j++){
	    if (*dataset++ != *original++){
		nerr++;
		if (nerr <= MAX_ERR_REPORT){
		    printf("Dataset Verify failed at [%lu][%lu](row %lu, col %lu): expect %d, got %d\n",
			(unsigned long)i, (unsigned long)j,
			(unsigned long)(i*stride[0]+start[0]), (unsigned long)(j*stride[1]+start[1]),
			*(dataset-1), *(original-1));
		}
	    }
	}
    }
    if (nerr > MAX_ERR_REPORT)
	printf("[more errors ...]\n");
    if (nerr)
	printf("%d errors found in dataset_vrfy\n", nerr);
    return(nerr);
}


/*
 * Example of using the HIO HDF5 library to create two datasets
 * in one HDF5 files with HIO access support.
 * The Datasets are of sizes (number-of-mpi-processes x DIM1) x DIM2.
 * Each process controls only a slab of size DIM1 x DIM2 within each
 * dataset.
 */

void
phdf5write1(char *filename, hio_context_t context)
{
    hid_t fid1;			/* HDF5 file IDs */
    hid_t acc_tpl1;		/* File access templates */
    hid_t sid1;   		/* Dataspace ID */
    hid_t file_dataspace;	/* File dataspace ID */
    hid_t mem_dataspace;	/* memory dataspace ID */
    hid_t dataset1, dataset2;	/* Dataset ID */
    hsize_t dims1[SPACE1_RANK] =
	{SPACE1_DIM1,SPACE1_DIM2};	/* dataspace dim sizes */
    DATATYPE data_array1[SPACE1_DIM1][SPACE1_DIM2];	/* data buffer */

    hsize_t start[SPACE1_RANK];			/* for hyperslab setting */
    hsize_t count[SPACE1_RANK], stride[SPACE1_RANK];	/* for hyperslab setting */

    herr_t ret;         	/* Generic return value */

    if (verbose)
	printf("Independent write test on file %s\n", filename);

    /* -------------------
     * START AN HDF5 FILE
     * -------------------*/
    /* setup file access template with hio IO access. */
    acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
    assert(acc_tpl1 != FAIL);
    MESG("H5Pcreate access succeed");

    /* set HIO context */
    H5FD_hio_set_write_io(&settings, H5FD_HIO_CONTIGUOUS);
    H5FD_hio_set_write_blocking(&settings, H5FD_HIO_BLOCKING);
    ret = H5Pset_fapl_hio(acc_tpl1, &settings);
    assert(ret != FAIL);
    MESG("H5Pset_fapl_hio succeed");

    /* create the file/dataset collectively */
    fid1 = H5Fcreate(filename, 0, H5P_DEFAULT, acc_tpl1);
    assert(fid1 != FAIL);
    MESG("H5Fcreate succeed");

    /* Release file-access template */
    ret = H5Pclose(acc_tpl1);
    assert(ret != FAIL);


    /* --------------------------
     * Define the dimensions of the overall datasets
     * and the slabs local to the MPI process.
     * ------------------------- */
    /* setup dimensionality object */
    sid1 = H5Screate_simple(SPACE1_RANK, dims1, NULL);
    assert (sid1 != FAIL);
    MESG("H5Screate_simple succeed");


    /* create a dataset collectively */
    dataset1 = H5Dcreate2(fid1, DATASETNAME1, H5T_NATIVE_INT, sid1,
			H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    assert(dataset1 != FAIL);
    MESG("H5Dcreate2 succeed");

    /* create another dataset collectively */
    dataset2 = H5Dcreate2(fid1, DATASETNAME2, H5T_NATIVE_INT, sid1,
			H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    assert(dataset2 != FAIL);
    MESG("H5Dcreate2 succeed");



    /* set up dimensions of the slab this process accesses */
    start[0] = mpi_rank*SPACE1_DIM1/mpi_size;
    start[1] = 0;
    count[0] = SPACE1_DIM1/mpi_size;
    count[1] = SPACE1_DIM2;
    stride[0] = 1;
    stride[1] =1;
if (verbose)
    printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
	(unsigned long)start[0], (unsigned long)start[1],
        (unsigned long)count[0], (unsigned long)count[1],
        (unsigned long)(count[0]*count[1]));

    /* put some trivial data in the data_array */
    dataset_fill(start, count, stride, &data_array1[0][0]);
    MESG("data_array initialized");

    /* create a file dataspace independently */
    file_dataspace = H5Dget_space (dataset1);
    assert(file_dataspace != FAIL);
    MESG("H5Dget_space succeed");
    ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
	    count, NULL);
    assert(ret != FAIL);
    MESG("H5Sset_hyperslab succeed");

    /* create a memory dataspace independently */
    mem_dataspace = H5Screate_simple (SPACE1_RANK, count, NULL);
    assert (mem_dataspace != FAIL);

    /* write data independently */
    ret = H5Dwrite(dataset1, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);
    MESG("H5Dwrite succeed");

    /* write data independently */
    ret = H5Dwrite(dataset2, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);
    MESG("H5Dwrite succeed");

    /* release dataspace ID */
    H5Sclose(file_dataspace);

    /* close dataset collectively */
    ret=H5Dclose(dataset1);
    assert(ret != FAIL);
    MESG("H5Dclose1 succeed");
    ret=H5Dclose(dataset2);
    assert(ret != FAIL);
    MESG("H5Dclose2 succeed");

    /* release all IDs created */
    H5Sclose(sid1);

    /* close the file collectively */
    H5Fclose(fid1);
}

/* Example of using the hio HDF5 library to read a dataset */
void
phdf5read1(char *filename, hio_context_t context)
{
    hid_t fid1;			/* HDF5 file IDs */
    hid_t acc_tpl1;		/* File access templates */
    hid_t file_dataspace;	/* File dataspace ID */
    hid_t mem_dataspace;	/* memory dataspace ID */
    hid_t dataset1, dataset2;	/* Dataset ID */
    DATATYPE data_array1[SPACE1_DIM1][SPACE1_DIM2];	/* data buffer */
    DATATYPE data_origin1[SPACE1_DIM1][SPACE1_DIM2];	/* expected data buffer */

    hsize_t start[SPACE1_RANK];			/* for hyperslab setting */
    hsize_t count[SPACE1_RANK], stride[SPACE1_RANK];	/* for hyperslab setting */

    herr_t ret;         	/* Generic return value */

    if (verbose)
	printf("Independent read test on file %s\n", filename);

    /* setup file access template */
    acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
    assert(acc_tpl1 != FAIL);
    /* set Hio access with communicator */
    H5FD_hio_set_read_io(&settings, H5FD_HIO_CONTIGUOUS);
    ret = H5Pset_fapl_hio(acc_tpl1, &settings);

    assert(ret != FAIL);

    /* open the file collectively */
    fid1=H5Fopen(filename,H5F_ACC_RDONLY,acc_tpl1);
    assert(fid1 != FAIL);

    /* Release file-access template */
    ret=H5Pclose(acc_tpl1);
    assert(ret != FAIL);

    /* open the dataset1 collectively */
    dataset1 = H5Dopen2(fid1, DATASETNAME1, H5P_DEFAULT);
    assert(dataset1 != FAIL);

    /* open another dataset collectively */
    dataset2 = H5Dopen2(fid1, DATASETNAME1, H5P_DEFAULT);
    assert(dataset2 != FAIL);


    /* set up dimensions of the slab this process accesses */
    start[0] = mpi_rank*SPACE1_DIM1/mpi_size;
    start[1] = 0;
    count[0] = SPACE1_DIM1/mpi_size;
    count[1] = SPACE1_DIM2;
    stride[0] = 1;
    stride[1] =1;
if (verbose)
    printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
        (unsigned long)start[0], (unsigned long)start[1],
        (unsigned long)count[0], (unsigned long)count[1],
        (unsigned long)(count[0]*count[1]));

    /* create a file dataspace independently */
    file_dataspace = H5Dget_space (dataset1);
    assert(file_dataspace != FAIL);
    ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
	    count, NULL);
    assert(ret != FAIL);

    /* create a memory dataspace independently */
    mem_dataspace = H5Screate_simple (SPACE1_RANK, count, NULL);
    assert (mem_dataspace != FAIL);

    /* fill dataset with test data */
    dataset_fill(start, count, stride, &data_origin1[0][0]);

    /* read data blocking and contiguous */
    ret = H5Dread(dataset1, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);

    /* verify the read data with original expected data */
    ret = dataset_vrfy(start, count, stride, &data_array1[0][0], &data_origin1[0][0]);
    assert(ret != FAIL);

    ret = H5Dread(dataset2, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);

    /* verify the read data with original expected data */
    ret = dataset_vrfy(start, count, stride, &data_array1[0][0], &data_origin1[0][0]);
    assert(ret == 0);

    /* close dataset collectively */
    ret=H5Dclose(dataset1);
    assert(ret != FAIL);
    ret=H5Dclose(dataset2);
    assert(ret != FAIL);

    /* release all IDs created */
    H5Sclose(file_dataspace);

    /* close the file collectively */
    H5Fclose(fid1);
}


/*
 * Example of using the HIO library to create two datasets
 * in one HDF5 file.
 * The Datasets are of sizes (number-of-mpi-processes x DIM1) x DIM2.
 * Each process controls only a slab of size DIM1 x DIM2 within each
 * dataset. [Note: not so yet.  Datasets are of sizes DIM1xDIM2 and
 * each process controls a hyperslab within.]
 */

void
phdf5write2(char *filename, hio_context_t context)
{
    hid_t fid1;			/* HDF5 file IDs */
    hid_t acc_tpl1;		/* File access templates */
    hid_t sid1;   		/* Dataspace ID */
    hid_t file_dataspace;	/* File dataspace ID */
    hid_t mem_dataspace;	/* memory dataspace ID */
    hid_t dataset1, dataset2;	/* Dataset ID */
    hsize_t dims1[SPACE1_RANK] =
	{SPACE1_DIM1,SPACE1_DIM2};	/* dataspace dim sizes */
    DATATYPE data_array1[SPACE1_DIM1][SPACE1_DIM2];	/* data buffer */

    hsize_t start[SPACE1_RANK];			/* for hyperslab setting */
    hsize_t count[SPACE1_RANK], stride[SPACE1_RANK];	/* for hyperslab setting */

    herr_t ret;         	/* Generic return value */
    ssize_t bytes_written;
    hio_request_t req;

    if (verbose)
	printf("Collective write test on file %s\n", filename);

    /* -------------------
     * START AN HDF5 FILE
     * -------------------*/
    /* setup file access template with hio IO access. */
    acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
    assert(acc_tpl1 != FAIL);
    MESG("H5Pcreate access succeed");
    H5FD_hio_set_write_io(&settings, H5FD_HIO_STRIDED);
    H5FD_hio_set_write_blocking(&settings, H5FD_HIO_NONBLOCKING);
    H5FD_hio_set_request(&settings, &req);
    ret = H5Pset_fapl_hio(acc_tpl1, &settings);
    assert(ret != FAIL);
    MESG("H5Pset_fapl_hio succeed");

    /* create the file collectively */
    fid1=H5Fcreate(filename,0,H5P_DEFAULT,acc_tpl1);
    assert(fid1 != FAIL);
    MESG("H5Fcreate succeed");

    /* Release file-access template */
    ret=H5Pclose(acc_tpl1);
    assert(ret != FAIL);


    /* --------------------------
     * Define the dimensions of the overall datasets
     * and create the dataset
     * ------------------------- */
    /* setup dimensionality object */
    sid1 = H5Screate_simple (SPACE1_RANK, dims1, NULL);
    assert (sid1 != FAIL);
    MESG("H5Screate_simple succeed");

    /* create a dataset collectively */
    dataset1 = H5Dcreate2(fid1, DATASETNAME1, H5T_NATIVE_INT, sid1, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    assert(dataset1 != FAIL);
    MESG("H5Dcreate2 succeed");

    /* create another dataset collectively */
    dataset2 = H5Dcreate2(fid1, DATASETNAME2, H5T_NATIVE_INT, sid1, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    assert(dataset2 != FAIL);
    MESG("H5Dcreate2 2 succeed");

    /*
     * Set up dimensions of the slab this process accesses.
     */

    /* Dataset1: each process takes a block of rows. */
    slab_set(start, count, stride, BYROW);
if (verbose)
    printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
	(unsigned long)start[0], (unsigned long)start[1],
        (unsigned long)count[0], (unsigned long)count[1],
        (unsigned long)(count[0]*count[1]));

    /* create a file dataspace independently */
    file_dataspace = H5Dget_space (dataset1);
    assert(file_dataspace != FAIL);
    MESG("H5Dget_space succeed");
    ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
	    count, NULL);
    assert(ret != FAIL);
    MESG("H5Sset_hyperslab succeed");

    /* create a memory dataspace independently */
    mem_dataspace = H5Screate_simple (SPACE1_RANK, count, NULL);
    assert (mem_dataspace != FAIL);

    /* fill the local slab with some trivial data */
    dataset_fill(start, count, stride, &data_array1[0][0]);
    MESG("data_array initialized");
    if (verbose){
	MESG("data_array created");
	dataset_print(start, count, stride, &data_array1[0][0]);
    }

    /* write data strided and blocking*/
    ret = H5Dwrite(dataset1, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);
    MESG("H5Dwrite succeed");

    //Wait for the request to finish
    hio_request_wait(&req, 1, &bytes_written);

    /* release all temporary handles. */
    /* Could have used them for dataset2 but it is cleaner */
    /* to create them again.*/
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);

    /* Dataset2: each process takes a block of columns. */
    slab_set(start, count, stride, BYCOL);
if (verbose)
    printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
	(unsigned long)start[0], (unsigned long)start[1],
        (unsigned long)count[0], (unsigned long)count[1],
        (unsigned long)(count[0]*count[1]));

    /* put some trivial data in the data_array */
    dataset_fill(start, count, stride, &data_array1[0][0]);
    MESG("data_array initialized");
    if (verbose){
	MESG("data_array created");
	dataset_print(start, count, stride, &data_array1[0][0]);
    }

    /* create a file dataspace independently */
    file_dataspace = H5Dget_space (dataset1);
    assert(file_dataspace != FAIL);
    MESG("H5Dget_space succeed");
    ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
	    count, NULL);
    assert(ret != FAIL);
    MESG("H5Sset_hyperslab succeed");

    /* create a memory dataspace independently */
    mem_dataspace = H5Screate_simple (SPACE1_RANK, count, NULL);
    assert (mem_dataspace != FAIL);

    /* fill the local slab with some trivial data */
    dataset_fill(start, count, stride, &data_array1[0][0]);
    MESG("data_array initialized");
    if (verbose){
	MESG("data_array created");
	dataset_print(start, count, stride, &data_array1[0][0]);
    }

    /* write data contiguous and blocking */
    ret = H5Dwrite(dataset2, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);
    MESG("H5Dwrite succeed");

    /* release all temporary handles. */
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);

    /*
     * All writes completed.  Close datasets collectively
     */
    ret=H5Dclose(dataset1);
    assert(ret != FAIL);
    MESG("H5Dclose1 succeed");
    ret=H5Dclose(dataset2);
    assert(ret != FAIL);
    MESG("H5Dclose2 succeed");

    /* release all IDs created */
    H5Sclose(sid1);

    /* close the file collectively */
    H5Fclose(fid1);
}

/*
 * Example of using the hio HDF5 library to read two datasets
 * in one HDF5 file with collective hio access support.
 * The Datasets are of sizes (number-of-mpi-processes x DIM1) x DIM2.
 * Each process controls only a slab of size DIM1 x DIM2 within each
 * dataset. [Note: not so yet.  Datasets are of sizes DIM1xDIM2 and
 * each process controls a hyperslab within.]
 */

void
phdf5read2(char *filename, hio_context_t context)
{
    hid_t fid1;			/* HDF5 file IDs */
    hid_t acc_tpl1;		/* File access templates */
    hid_t file_dataspace;	/* File dataspace ID */
    hid_t mem_dataspace;	/* memory dataspace ID */
    hid_t dataset1, dataset2;	/* Dataset ID */
    DATATYPE data_array1[SPACE1_DIM1][SPACE1_DIM2];	/* data buffer */
    DATATYPE data_origin1[SPACE1_DIM1][SPACE1_DIM2];	/* expected data buffer */

    hsize_t start[SPACE1_RANK];			/* for hyperslab setting */
    hsize_t count[SPACE1_RANK], stride[SPACE1_RANK];	/* for hyperslab setting */

    herr_t ret;         	/* Generic return value */

    if (verbose)
	printf("Collective read test on file %s\n", filename);

    /* -------------------
     * OPEN AN HDF5 FILE
     * -------------------*/
    /* setup file access template with hio IO access. */
    acc_tpl1 = H5Pcreate (H5P_FILE_ACCESS);
    assert(acc_tpl1 != FAIL);
    MESG("H5Pcreate access succeed");
    /* set Hio access with communicator */
    H5FD_hio_set_read_io(&settings, H5FD_HIO_STRIDED);
    ret = H5Pset_fapl_hio(acc_tpl1, &settings);

    assert(ret != FAIL);
    MESG("H5Pset_fapl_hio succeed");

    /* open the file collectively */
    fid1=H5Fopen(filename,H5F_ACC_RDONLY, acc_tpl1);
    assert(fid1 != FAIL);
    MESG("H5Fopen succeed");

    /* Release file-access template */
    ret=H5Pclose(acc_tpl1);
    assert(ret != FAIL);


    /* --------------------------
     * Open the datasets in it
     * ------------------------- */
    /* open the dataset1 collectively */
    dataset1 = H5Dopen2(fid1, DATASETNAME1, H5P_DEFAULT);
    assert(dataset1 != FAIL);
    MESG("H5Dopen2 succeed");

    /* open another dataset collectively */
    dataset2 = H5Dopen2(fid1, DATASETNAME2, H5P_DEFAULT);
    assert(dataset2 != FAIL);
    MESG("H5Dopen2 2 succeed");

    /*
     * Set up dimensions of the slab this process accesses.
     */

    /* Dataset1: each process takes a block of columns. */
    slab_set(start, count, stride, BYROW);
if (verbose)
    printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
	(unsigned long)start[0], (unsigned long)start[1],
        (unsigned long)count[0], (unsigned long)count[1],
        (unsigned long)(count[0]*count[1]));

    /* create a file dataspace independently */
    file_dataspace = H5Dget_space (dataset1);
    assert(file_dataspace != FAIL);
    MESG("H5Dget_space succeed");
    ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
	    count, NULL);
    assert(ret != FAIL);
    MESG("H5Sset_hyperslab succeed");

    /* create a memory dataspace independently */
    mem_dataspace = H5Screate_simple (SPACE1_RANK, count, NULL);
    assert (mem_dataspace != FAIL);

    /* fill dataset with test data */
    dataset_fill(start, count, stride, &data_origin1[0][0]);
    MESG("data_array initialized");
    if (verbose){
	MESG("data_array created");
	dataset_print(start, count, stride, &data_origin1[0][0]);
    }

    /* read data collectively */

    ret = H5Dread(dataset1, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);
    MESG("H5Dread succeed");
    
    /* verify the read data with original expected data */
    ret = dataset_vrfy(start, count, stride, &data_array1[0][0], &data_origin1[0][0]);
    assert(ret != FAIL);

    /* release all temporary handles. */
    /* Could have used them for dataset2 but it is cleaner */
    /* to create them again.*/
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);

    /* Dataset2: each process takes a block of rows. */
    slab_set(start, count, stride, BYCOL);
if (verbose)
    printf("start[]=(%lu,%lu), count[]=(%lu,%lu), total datapoints=%lu\n",
	(unsigned long)start[0], (unsigned long)start[1],
        (unsigned long)count[0], (unsigned long)count[1],
        (unsigned long)(count[0]*count[1]));

    /* create a file dataspace independently */
    file_dataspace = H5Dget_space (dataset1);
    assert(file_dataspace != FAIL);
    MESG("H5Dget_space succeed");
    ret=H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride,
	    count, NULL);
    assert(ret != FAIL);
    MESG("H5Sset_hyperslab succeed");

    /* create a memory dataspace independently */
    mem_dataspace = H5Screate_simple (SPACE1_RANK, count, NULL);
    assert (mem_dataspace != FAIL);

    /* fill dataset with test data */
    dataset_fill(start, count, stride, &data_origin1[0][0]);
    MESG("data_array initialized");
    if (verbose){
	MESG("data_array created");
	dataset_print(start, count, stride, &data_array1[0][0]);
    }

    /* read data strided non-blocking */
    ret = H5Dread(dataset2, H5T_NATIVE_INT, mem_dataspace, file_dataspace,
	    H5P_DEFAULT, data_array1);
    assert(ret != FAIL);
    MESG("H5Dread succeed");

    /* verify the read data with original expected data */
    ret = dataset_vrfy(start, count, stride, &data_array1[0][0], &data_origin1[0][0]);
    assert(ret != FAIL);

    /* release all temporary handles. */
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);

    /*
     * All reads completed.  Close datasets collectively
     */
    ret=H5Dclose(dataset1);
    assert(ret != FAIL);
    MESG("H5Dclose1 succeed");
    ret=H5Dclose(dataset2);
    assert(ret != FAIL);
    MESG("H5Dclose2 succeed");

    /* close the file collectively */
    H5Fclose(fid1);
}

/*
 * Show command usage
 */
void
usage(void)
{
    printf("Usage: hdf5_hio_example [-v] [-s]\n");
    printf("\t-v\tverbose on\n");
    printf("\t-v\tUse hio without MPI\n");
    printf("\n");
}


/*
 * parse the command line options
 */
int
parse_options(int argc, char **argv){
    while (--argc){
	if (**(++argv) != '-'){
	    break;
	}else{
	    switch(*(*argv+1)){
		case 'v':   verbose = 1;
			    break;
		case 's':   single = 1;
			    break;
		default:    usage();
			    nerrors++;
			    return(1);
	    }
	}
    }

    return(0);
}


/* Main Program */
int
main(int argc, char **argv)
{
    int mpi_namelen;
    char mpi_name[MPI_MAX_PROCESSOR_NAME];
    MPI_Comm comm = MPI_COMM_WORLD;
    hio_context_t context = HIO_OBJECT_NULL;
    char *filename = "test_file";

    MPI_Init(&argc,&argv);
    MPI_Comm_size(comm,&mpi_size);
    MPI_Comm_rank(comm,&mpi_rank);
    MPI_Get_processor_name(mpi_name,&mpi_namelen);
    /* Make sure datasets can be divided into equal chunks by the processes */
    if ((SPACE1_DIM1 % mpi_size) || (SPACE1_DIM2 % mpi_size)){
	printf("DIM1(%d) and DIM2(%d) must be multiples of processes (%d)\n",
	    SPACE1_DIM1, SPACE1_DIM2, mpi_size);
	nerrors++;
	goto finish;
    }

    if (parse_options(argc, argv) != 0)
	goto finish;


    H5FD_hio_settings_init(&settings);
    H5FD_hio_set_elem_name(&settings, "elem");
    H5FD_hio_set_setid(&settings, 23888221L);
    H5FD_hio_set_dataset_mode(&settings, H5FD_HIO_DATASET_SHARED);
    if (!single)
	H5FD_hio_set_comm(&settings, comm);


    MPI_BANNER("testing PHDF5 dataset write test 1 ...");
    phdf5write1(filename, context);
    MPI_BANNER("testing PHDF5 dataset write test 2...");
    phdf5write2(filename, context);

    MPI_BANNER("testing PHDF5 dataset read test 1 ...");
    phdf5read1(filename, context);
    MPI_BANNER("testing PHDF5 dataset read test 2 ...");
    phdf5read2(filename, context);
    
finish:
    if (mpi_rank == 0){		/* only process 0 reports */
	if (nerrors)
	    printf("***PHDF5 tests detected %d errors***\n", nerrors);
	else{
	    printf("===================================\n");
	    printf("PHDF5 tests finished with no errors\n");
	    printf("===================================\n");
	}
    }
    
    MPI_Finalize();

    return(nerrors);
}
