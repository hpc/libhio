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

#include "hio_internal.h"

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

#include <assert.h>

#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <ctype.h>

typedef struct hio_error_stack_item_t {
  struct hio_error_stack_item_t *next;
  hio_object_t                   object;
  int                            hrc;
  char                          *error_string;
} hio_error_stack_item_t;

static hio_error_stack_item_t *hio_error_stack_head = NULL;
static pthread_mutex_t hio_error_stack_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * @file Internal hio functions
 */

char * hioi_msg_time(char * time_buf, size_t len) {
  time_t current_time;
  current_time = time (NULL);
  strftime(time_buf, len, "%Y-%m-%d %H:%M:%S", localtime(&current_time));
  return time_buf;
}

/**
 * hioi_dump_writer - in collaboration with macro hioi_dump will dump memory
 * to stderr with output lines like: 
 * YYYY-MM-DD hh:mm:ss [<msg_id>] [0000] 75 6E 6B 6E 6F 77 6E 20   30 FF 00 00 00 00 39 00   unknown 0.....9.
 */ 
void hioi_dump_writer(hio_context_t context, const char * header, const void * data, size_t size) {
    char time_buf[32];
    char hdr_buf[128];
    const unsigned char *p = data;
    unsigned char c;
    int n;
    char bytestr[4] = {0};
    char addrstr[10] = {0};
    char hexstr[ 16*3 + 5] = {0};
    char hexprev[ 16*3 + 5] = {0};
    char charstr[16*1 + 5] = {0};
    int skipped = 0;

    hioi_msg_time(time_buf, sizeof(time_buf)); 
    snprintf(hdr_buf, sizeof(hdr_buf), header, time_buf, (context) ? context->c_msg_id: "No Context");

    for(n=1;n<=size;n++) {
        if (n%16 == 1) {
            /* store address for this line */
            snprintf(addrstr, sizeof(addrstr), "%.4lx", p-(unsigned char *)data);
        }

        c = *p;
        if (isalnum(c) == 0) {
            c = '.';
        }

        /* store hex str (for left side) */
        snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
        strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);

        /* store char str (for right side) */
        snprintf(bytestr, sizeof(bytestr), "%c", c);
        strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

        if(n%16 == 0) {
            /* line completed */
            if (!strcmp(hexstr, hexprev) && n< size) {
              skipped++;
            } else {
              if (skipped > 0) {
                fprintf(stderr, "%s        %d identical lines skipped\n", hdr_buf,skipped);
                skipped = 0;
              }
              fprintf(stderr, "%s[%4.4s]   %-50.50s  %s\n", hdr_buf, addrstr, hexstr, charstr);
              strcpy(hexprev, hexstr);
            }
            hexstr[0] = 0;
            charstr[0] = 0;
        } else if(n%8 == 0) {
            /* half line: add whitespaces */
            strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
            strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
        }
        p++; /* next byte */
    }

    if (strlen(hexstr) > 0) {
        if (skipped > 0) {
           fprintf(stderr, "%s        %d identical lines skipped\n", hdr_buf, skipped);
        }
        /* print rest of buffer if not empty */
        fprintf(stderr, "%s[%4.4s]   %-50.50s  %s\n", hdr_buf, addrstr, hexstr, charstr);
    }
}

int hioi_err_errno (int err) {
  switch (err) {
  case 0:
    return HIO_SUCCESS;
  case EPERM:
  case EACCES:
    return HIO_ERR_PERM;
  case ENOMEM:
    return HIO_ERR_OUT_OF_RESOURCE;
  case ENOENT:
    return HIO_ERR_NOT_FOUND;
  case EIO:
    return HIO_ERR_IO_PERMANENT;
  case EEXIST:
    return HIO_ERR_EXISTS;
  default:
    return HIO_ERROR;
  }
}

void hioi_err_push (int hrc, hio_object_t object, char *format, ...) {
  hio_context_t context = object ? hioi_object_context (object) : NULL;
  hio_error_stack_item_t *new_item;
  va_list vargs;
  int rc;

  new_item = calloc (1, sizeof (hio_error_stack_item_t));
  if (NULL == new_item) {
    /* not much can be done here. we are just plain OOM. */
    return;
  }

  va_start (vargs, format);

  rc = vasprintf (&new_item->error_string, format, vargs);

  va_end (vargs);

  if (0 >= rc) {
    /* couldn't allocate error string */
    free (new_item);
    return;
  }

  if (context) {
    hioi_log (context, HIO_VERBOSE_ERROR, "%s", new_item->error_string);
  }

  new_item->hrc = hrc;

  /* push the error message onto the stack */
  if (NULL == context) {
    pthread_mutex_lock (&hio_error_stack_mutex);
    new_item->next = hio_error_stack_head;
    hio_error_stack_head = new_item;
    pthread_mutex_unlock (&hio_error_stack_mutex);
  } else {
    hioi_object_lock (&context->c_object);
    new_item->next = (hio_error_stack_item_t *) context->c_estack;
    context->c_estack = (void *) new_item;
    hioi_object_unlock (&context->c_object);
   }
}

#if HIO_MPI_HAVE(1)
void hioi_err_push_mpi (int mpirc, hio_object_t object, char *format, ...) {
  hio_context_t context = object ? hioi_object_context (object) : NULL;
  hio_error_stack_item_t *new_item;
  char mpi_error[MPI_MAX_ERROR_STRING] = "Unknown error";
  int resultlen = MPI_MAX_ERROR_STRING;
  va_list vargs;
  char *temp;
  int rc;

  va_start (vargs, format);

  rc = vasprintf (&temp, format, vargs);

  va_end (vargs);

  if (0 >= rc) {
    /* couldn't allocate error string */
    return;
  }

  /* ignore the error code for this */
  (void) MPI_Error_string (mpirc, mpi_error, &resultlen);

  new_item = calloc (1, sizeof (hio_error_stack_item_t));
  if (NULL == new_item) {
    /* not much can be done here. we are just plain OOM. */
    return;
  }

  new_item->hrc = hioi_err_mpi(mpirc);

  /* TODO -- Should probably do something smarter here */
  new_item->error_string = malloc (strlen (temp) + 3 + resultlen);
  if (NULL == temp) {
    free (new_item);
    free (temp);
    return;
  }

  /* append the mpi error to the hio error string */
  strcpy (new_item->error_string, temp);
  strcat (new_item->error_string, ": ");
  strcat (new_item->error_string, mpi_error);

  /* done with this now */
  free (temp);

  /* push the error message onto the stack */
  if (NULL == context) {
    pthread_mutex_lock (&hio_error_stack_mutex);
    new_item->next = hio_error_stack_head;
    hio_error_stack_head = new_item;
    pthread_mutex_unlock (&hio_error_stack_mutex);
  } else {
    hioi_object_lock (&context->c_object);
    new_item->next = (hio_error_stack_item_t *) context->c_estack;
    context->c_estack = (void *) new_item;
    hioi_object_unlock (&context->c_object);
  }
}

int hioi_err_mpi (int mpirc) {
  /* TODO: implement this */
  if (MPI_SUCCESS == mpirc) {
    return HIO_SUCCESS;
  }

  return HIO_ERROR;
}
#endif

int hio_err_get_last (hio_context_t context, char **error) {
  hio_error_stack_item_t *stack_error;
  int hrc;

  if (NULL == context) {
    pthread_mutex_lock (&hio_error_stack_mutex);
    stack_error = hio_error_stack_head;
    if (NULL != stack_error) {
      hio_error_stack_head = stack_error->next;
    }
    pthread_mutex_unlock (&hio_error_stack_mutex);
  } else {
    hioi_object_lock (&context->c_object);
    stack_error = (hio_error_stack_item_t *) context->c_estack;
    if (NULL != stack_error) {
      context->c_estack = (void *) stack_error->next;
    }
    hioi_object_unlock (&context->c_object);
  }

  if (NULL == stack_error) {
    /* no error */
    *error = NULL;
    return HIO_SUCCESS;
  }

  *error = stack_error->error_string;
  hrc = stack_error->hrc;
  free (stack_error);

  return hrc;
}

static int hio_err_print_last_vargs (hio_context_t context, FILE *output, char *format, va_list vargs) {
  char hostname[256] = "unknown";
  char datetime[30] = "unknown\n";
  char *hio_error;
  time_t timeval;
  int hrc, rc;

  /* dequeue the last error */
  hrc = hio_err_get_last (context, &hio_error);
  if (NULL == hio_error) {
    return 0;
  }

  /* try to get the hostname */
  (void) gethostname (hostname, 256);

  /* try to get the time */
  timeval = time (NULL);
  (void) ctime_r (&timeval, datetime);

  /* remove newline */
  datetime[strlen(datetime) - 1] = '\0';

  /* NTH: the following code prints a series of messages to the specified output
   * file handle. the code as is will probably not work properly if this function
   * is being called from multiple threads. in a future update this code should
   * be updated to buffer the error message before printing it out to the file
   * handle. */

  /* print out the timestamp */
  if (NULL == context) {
    rc = fprintf (output, "HIO %s <%s>: error code (%d) ", hostname, datetime, hrc);
  } else {
    rc = fprintf (output, "HIO %s <%s>: error code (%d) context (%s) ", hostname, datetime,
                  hrc, context->c_object.identifier);
  }

  /* print the user's error message */
  rc += vfprintf (output, format, vargs);

  /* finally, print out the hio error message */
  rc += fprintf (output, ": %s\n", hio_error);

  /* free the error message */
  free (hio_error);

  return rc;
}

int hio_err_print_last (hio_context_t ctx, FILE *output, char *format, ...) {
  va_list vargs;
  int rc;

  va_start (vargs, format);
  rc = hio_err_print_last_vargs (ctx, output, format, vargs);
  va_end (vargs);

  return rc;
}

int hio_err_print_all (hio_context_t ctx, FILE *output, char *format, ...)
{
  va_list vargs;
  int rc;

  /* loop until all error messages have been printed */
  do {
    va_start (vargs, format);
    rc = hio_err_print_last_vargs (ctx, output, format, vargs);
    va_end (vargs);

    if (0 == rc) {
      break;
    }
  } while (1);

  return HIO_SUCCESS;
}

uint64_t hioi_gettime (void) {
  struct timeval tv;
  gettimeofday (&tv, NULL);
  return 1000000 * tv.tv_sec + tv.tv_usec;
}

int hio_mkpath (hio_context_t context, const char *path, mode_t access_mode) {
  char *tmp = strdup (path);
  int rc;

  if (NULL == tmp) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  for (char *sep = strchr (tmp, '/') ; sep ; sep = strchr (sep + 1, '/')) {
    if (sep == tmp) {
      continue;
    }

    *sep = '\0';
    errno = 0;

    if (access (tmp, F_OK)) {
      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "creating directory %s with permissions 0%o", tmp, access_mode);

      rc = mkdir (tmp, access_mode);
      if (0 != rc && (EEXIST != errno)) {
        hioi_log (context, HIO_VERBOSE_WARN, "could not create directory %s. errno: %d", tmp, errno);
        free (tmp);
        return HIO_ERROR;
      }
    } else {
      errno = EEXIST;
    }

    *sep = '/';
  }

  errno = 0;
  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "creating directory %s with permissions 0%o", tmp, access_mode);
  rc = mkdir (tmp, access_mode);
  free (tmp);
  return (rc && errno != EEXIST) ? HIO_ERROR : HIO_SUCCESS;
}

hio_object_t hioi_object_alloc (const char *name, hio_object_type_t type, hio_object_t parent,
                                size_t object_size, hio_object_release_fn_t release_fn) {
  pthread_mutexattr_t mutex_attr;
  hio_object_t new_object;
  int rc;

  new_object = calloc (1, object_size);
  if (NULL == new_object) {
    return NULL;
  }

  new_object->identifier = strdup (name);
  if (NULL == new_object->identifier) {
    free (new_object);
    return NULL;
  }

  rc = hioi_var_init (new_object);
  if (HIO_SUCCESS != rc) {
    free (new_object->identifier);
    free (new_object);

    return NULL;
  }

  new_object->type = type;
  new_object->parent = parent;
  new_object->release_fn = release_fn;
  pthread_mutexattr_init (&mutex_attr);
  pthread_mutexattr_settype (&mutex_attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init (&new_object->lock, &mutex_attr);
  pthread_mutexattr_destroy (&mutex_attr);

  return new_object;
}

void hioi_object_release (hio_object_t object) {
  if (HIO_OBJECT_NULL == object) {
    return;
  }

  if (NULL != object->release_fn) {
    object->release_fn (object);
  }

  hioi_var_fini (object);

  free (object->identifier);
  free (object);
}

hio_context_t hioi_object_context (hio_object_t object) {
  if (NULL == object->parent) {
    /* all objects have a context at the root */
    assert (HIO_OBJECT_TYPE_CONTEXT == object->type);
    return (hio_context_t) object;
  }

  return hioi_object_context (object->parent);
}

int hioi_string_scatter (hio_context_t context, char **string) {
#if HIO_MPI_HAVE(1)
  if (hioi_context_using_mpi (context)) {
    int string_len;

    if (0 == context->c_rank) {
      string_len = strlen (*string);
    }

    MPI_Bcast (&string_len, 1, MPI_INT, 0, context->c_comm);

    if (0 != context->c_rank) {
      free (*string);
      *string = malloc (string_len + 1);
      assert (NULL != *string);
    }

    MPI_Bcast (*string, string_len + 1, MPI_BYTE, 0, context->c_comm);
  }
#endif

  return HIO_SUCCESS;
}

int hioi_file_close (hio_file_t *file) {
  int rc = 0;

  if (file->f_hndl) {
    rc = fclose (file->f_hndl);
  } else if (-1 != file->f_fd) {
    rc = close (file->f_fd);
  }

  if (0 != rc) {
    rc = hioi_err_errno (errno);
  }

  file->f_fd = -1;
  file->f_hndl = NULL;

  return rc;
}

int64_t hioi_file_seek (hio_file_t *file, int64_t offset, int whence) {
  if (SEEK_SET == whence && offset == file->f_offset) {
    return file->f_offset;
  }

  if (-1 != file->f_fd) {
    file->f_offset = lseek (file->f_fd, offset, whence);
  } else {
    (void) fseek (file->f_hndl, offset, whence);
    file->f_offset = ftell (file->f_hndl);
  }

  return file->f_offset;
}

ssize_t hioi_file_write (hio_file_t *file, const void *ptr, size_t count) {
  ssize_t actual, total = 0;

  do {
    if (-1 != file->f_fd) {
      actual = write (file->f_fd, ptr, count);
    } else {
      actual = fwrite (ptr, 1, count, file->f_hndl);
    }

    if (actual > 0) {
      total += actual;
      count -= actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    }
  } while (count > 0 && (actual > 0 || (-1 == actual && EINTR == errno)) );

  if (total > 0) {
    file->f_offset += total;
  }

  return (actual < 0) ? actual: total;
}

ssize_t hioi_file_read (hio_file_t *file, void *ptr, size_t count) {
  ssize_t actual, total = 0;

  do {
    if (-1 != file->f_fd) {
      actual = read (file->f_fd, ptr, count);
    } else {
      actual = fread (ptr, 1, count, file->f_hndl);
    }

    if (actual > 0) {
      total += actual;
      count -= actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    }
  } while (count > 0 && (actual > 0 || (-1 == actual && EINTR == errno)) );

  if (total > 0) {
    file->f_offset += total;
  }

  return (actual < 0) ? actual: total;
}

void hioi_file_flush (hio_file_t *file) {
  if (-1 != file->f_fd) {
    fsync (file->f_fd);
  } else if (NULL != file->f_hndl) {
    fflush (file->f_hndl);
    fsync (fileno (file->f_hndl));
  }
}
