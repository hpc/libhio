/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file hio_fs.c
 * @brief hio filesystem query functions
 */

#include "hio_types.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#if HAVE_LUSTRE_LUSTREAPI_H
#include <lustre/lustreapi.h>
#elif HAVE_LUSTRE_LIBLUSTREAPI_H
#include <lustre/liblustreapi.h>
#endif

#if HAVE_LUSTRE_LUSTRE_USER_H
#include <lustre/lustre_user.h>
#endif

#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#if HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#if HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#if HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

static int hioi_fs_open_posix (const char *path, hio_fs_attr_t *fs_attr, int flags, int mode);

static int hioi_fs_open_posix (const char *path, hio_fs_attr_t *fs_attr, int flags, int mode) {
#pragma unused(fs_attr)
  return open (path, flags, mode);
}

static int hioi_fs_open_lustre_old (const char *path, hio_fs_attr_t *fs_attr, int flags, int mode) {
#if defined(LL_SUPER_MAGIC)
  bool use_posix_open = false;
  int ret;

  if (0 == access (path, F_OK)) {
    /* can't set the striping on an existing file */
    use_posix_open = true;
  }

  if (!use_posix_open) {
    /* NTH: use the default layout/starting index for now */
    ret = llapi_file_open (path, flags, mode, fs_attr->fs_ssize, -1,
                           fs_attr->fs_scount, 0);

    if (0 > ret && EALREADY == errno) {
      use_posix_open = true;
    }
  }

  if (use_posix_open) {
    ret = open (path, flags, mode);
  }

  if (0 < ret) {
    return ret;
  }

  return hioi_err_errno (errno);
#else
  return hioi_fs_open_posix (path, fs_attr, flags, mode);
#endif
}

hio_fs_open_fn_t hio_fs_open_fns[HIO_FS_TYPE_MAX] = {
  hioi_fs_open_posix,
  hioi_fs_open_lustre_old,
  hioi_fs_open_posix,
};

#if defined(LL_SUPER_MAGIC)

#if !defined(LOV_MAX_STRIPE_COUNT)
/* NTH: workaround for broken lustre versions  */
#define LOV_MAX_STRIPE_COUNT 256
#endif

static struct lov_user_md *hioi_alloc_lustre_data (void) {
  size_t struct_size;

  if (sizeof (struct lov_user_md_v1) > sizeof (struct lov_user_md_v3)) {
    struct_size = sizeof (struct lov_user_md_v1);
  } else {
    struct_size = sizeof (struct lov_user_md_v3);
  }

  struct_size += LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);

#if HAVE_STRUCT_LOV_USER_MD_JOIN
  size_t join_size;

  join_size = sizeof (struct lov_user_md_join) +
    LOV_MAX_STRIPE_COUNT * sizeof (struct lov_user_ost_data_join);

  if (struct_size < join_size) {
    struct_size = join_size;
  }
#endif

  return malloc (struct_size);
}

static int hioi_fs_query_lustre (const char *path, hio_fs_attr_t *fs_attr) {
  struct lov_user_md *lum_file;
  int rc;

  lum_file = hioi_alloc_lustre_data ();
  assert (NULL != lum_file);

  fs_attr->fs_flags |= HIO_FS_SUPPORTS_STRIPING;

  rc = llapi_file_get_stripe (path, lum_file);
  if (0 == rc) {
    fs_attr->fs_scount = lum_file->lmm_stripe_count;
    fs_attr->fs_ssize  = lum_file->lmm_stripe_size;

    free (lum_file);
    return HIO_SUCCESS;
  }

  if (ENODATA != rc) {
    return HIO_SUCCESS;
  }

  return hioi_err_errno (errno);
}

#endif


int hioi_fs_query (hio_context_t context, const char *path, hio_fs_attr_t *fs_attr) {
  struct stat statinfo;
  struct statfs fsinfo;
  char *statfs_path = (char *) path;
  int ret;

  if (NULL == path) {
    return HIO_ERR_BAD_PARAM;
  }

  do {
    if (0 != context->context_rank) {
      break;
    }

    ret = stat (path, &statinfo);
    if (0 != ret) {
      fs_attr->fs_type = hioi_err_errno (errno);
      break;
    }


    if (S_ISDIR(statinfo.st_mode)) {
      /* checking directory fs_attr. create a test file instead */
      ret = asprintf (&statfs_path, "%s/.hio_test", path);
      assert (0 < ret);
      ret = open (statfs_path, O_CREAT | O_RDONLY, 0600);
      if (0 > ret) {
        free (statfs_path);
        fs_attr->fs_type = hioi_err_errno (errno);
        break;
      }

      close (ret);
    }

    /* get general filesystem data */
    ret = statfs (statfs_path, &fsinfo);
    if (0 > ret) {
      fs_attr->fs_type = hioi_err_errno (errno);
      break;
    }

    fs_attr->fs_bavail  = fsinfo.f_bavail;
    fs_attr->fs_btotal  = fsinfo.f_blocks;
    fs_attr->fs_bsize   = fsinfo.f_bsize;

    /* set default striping information (unsupported) */
    fs_attr->fs_scount  = 0;
    fs_attr->fs_ssize   = 0;
    fs_attr->fs_flags   = 0;

    /* get filesytem specific data */
    switch (fsinfo.f_type) {
#if defined(LL_SUPER_MAGIC)
    case LL_SUPER_MAGIC:
      hioi_fs_query_lustre (statfs_path, fs_attr);
      fs_attr->fs_type = HIO_FS_TYPE_LUSTRE;
      break;
#endif
#if defined(GPFS_SUPER_MAGIC)
    case GPFS_SUPER_MAGIC:
      /* gpfs */
#endif

#if defined(PAN_FS_CLIENT_MAGIC)
    case PAN_FS_CLIENT_MAGIC:
      /* panfs */
#endif
    default:
      fs_attr->fs_type = HIO_FS_TYPE_DEFAULT;
    }

    if (path != statfs_path) {
      /* remove temporary file */
      unlink (statfs_path);
      free (statfs_path);
    }
  } while (0);

#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (fs_attr, sizeof (*fs_attr), MPI_BYTE, 0, context->context_comm);
  }
#endif
  if (0 > fs_attr->fs_type) {
    return fs_attr->fs_type;
  }

  fs_attr->fs_open = hio_fs_open_fns[fs_attr->fs_type];

  return HIO_SUCCESS;
}
