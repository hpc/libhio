/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC.  All rights
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

#include "hio_internal.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>

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

#if HAVE_DATAWARP_H

#include <datawarp.h>

/* datawarp does not yet provide a define for the filesystem magic so it
 * is hard-coded here. remove this line if cray ever defines this. */
#define DW_SUPER_MAGIC 0x3e3f

#endif

static int hioi_fs_open_posix (hio_context_t context, const char *path, hio_fs_attr_t *fs_attr,
			       int flags, int mode) {
#pragma unused(fs_attr)
  int fd;

  fd = open (path, flags, mode);
  if (-1 == fd) {
    return hioi_err_errno (errno);
  }

  return fd;
}

#if defined(LL_SUPER_MAGIC)
static int hioi_fs_luste_set_locking (int fd, hio_fs_attr_t *fs_attr) {
  int rc;

  switch (fs_attr->fs_lock_strategy) {
#if defined(LL_IOC_GROUP_LOCK)
  case HIO_FS_LOCK_GROUP:
    rc = ioctl (fd,  LL_IOC_GROUP_LOCK, 1);
    break;
#endif
#if defined(LL_FILE_IGNORE_LOCK)
  case HIO_FS_LOCK_DISABLE:
    rc = ioctl (fd, LL_IOC_SETFLAGS, &(int){LL_FILE_IGNORE_LOCK});
    break;
#endif
#if defined(LL_IOC_REQUEST_ONLY)
  case HIO_FS_LOCK_NOEXPAND:
    rc = ioctl (fd, LL_IOC_REQUEST_ONLY);
    break;
#endif
  default:
    return HIO_ERR_BAD_PARAM;
  }

  return rc;
}
#endif

static int hioi_fs_open_lustre (hio_context_t context, const char *path, hio_fs_attr_t *fs_attr,
				int flags, int mode) {
#if defined(LL_SUPER_MAGIC)
  struct lov_user_md lum;
  int rc, fd;

  /* use a combination of posix open and luster ioctls to avoid issues with llapi_file_open */
  if (flags & O_CREAT) {
    flags |= O_LOV_DELAY_CREATE;
  }

  fd = open (path, flags, mode);
  if (-1 == fd && EEXIST == errno) {
    flags &= ~(O_LOV_DELAY_CREATE | O_CREAT);
    fd = open (path, flags);
    if (fd < 0) {
      return hioi_err_errno (errno);
    }

    hioi_fs_luste_set_locking (fd, fs_attr);

    return fd;
  }

  if (fd >= 0) {
    if (flags & O_CREAT) {
      /* NTH: use the default layout/starting index for now */
      lum.lmm_magic = LOV_USER_MAGIC;
      switch (fs_attr->fs_raid_level) {
      case 0:
        lum.lmm_pattern = LOV_PATTERN_RAID0;
        break;
#if defined(LOV_PATTERN_RAID1)
      case 1:
        lum.lmm_pattern = LOV_PATTERN_RAID1;
        break;
#endif
      default:
        lum.lmm_pattern = 0;
      }

      lum.lmm_pattern = fs_attr->fs_raid_level;
      lum.lmm_stripe_size = fs_attr->fs_ssize;
      lum.lmm_stripe_count = fs_attr->fs_scount;
      lum.lmm_stripe_offset = -1;

      rc = ioctl (fd, LL_IOC_LOV_SETSTRIPE, &lum);
    }

    hioi_fs_luste_set_locking (fd, fs_attr);

    return fd;
  }

  return hioi_err_errno (errno);
#else
  return hioi_fs_open_posix (context, path, fs_attr, flags, mode);
#endif
}

static int hioi_fs_open_datawarp (hio_context_t context, const char *path, hio_fs_attr_t *fs_attr,
				  int flags, int mode) {
#if defined(DW_SUPER_MAGIC)
  int rc, fd;

  fd = open (path, flags, mode);
  if (-1 == fd && EEXIST == errno) {
    flags &= ~O_CREAT;
    fd = open (path, flags);
    if (fd < 0) {
      return hioi_err_errno (errno);
    }

    return fd;
  }

  if (fd >= 0) {
    if (flags & O_CREAT) {
      rc = dw_set_stripe_configuration (fd, fs_attr->fs_ssize, fs_attr->fs_scount);
      if (0 != rc) {
	hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "datawarp: could not set file striping parameters: "
		  "errno = %d", -rc);
      }
    }

    return fd;
  }

  return hioi_err_errno (errno);
#else
  return hioi_fs_open_posix (context, path, fs_attr, flags, mode);
#endif
}


hio_fs_open_fn_t hio_fs_open_fns[HIO_FS_TYPE_MAX] = {
  hioi_fs_open_posix,
  hioi_fs_open_lustre,
  hioi_fs_open_posix,
  hioi_fs_open_datawarp,
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

  return calloc (1, struct_size);
}

static int hioi_fs_query_lustre (const char *path, hio_fs_attr_t *fs_attr) {
  struct lov_user_md *lum;
  struct find_param param;
  char mountdir[PATH_MAX];
  int rc, fd, obd_count;

  rc = llapi_search_mounts (path, 0, mountdir, NULL);
  if (0 != rc) {
    return hioi_err_errno (-rc);
  }

#if !HAVE_LLAPI_GET_OBD_COUNT
  fd = open (mountdir, O_RDONLY);
  if (-1 == fd) {
    return hioi_err_errno (errno);
  }

  rc = llapi_lov_get_uuids (fd, NULL, &obd_count);
  close (fd);
#else
  rc = llapi_get_obd_count (mountdir, &obd_count, 0);
#endif

  if (0 != rc) {
    return hioi_err_errno (errno);
  }

  fs_attr->fs_flags |= HIO_FS_SUPPORTS_STRIPING | HIO_FS_SUPPORTS_RAID | HIO_FS_SUPPORTS_BLOCK_LOCKING;
  fs_attr->fs_type        = HIO_FS_TYPE_LUSTRE;
  fs_attr->fs_sunit       = 64 * 1024;
  fs_attr->fs_smax_size   = 0x100000000ul;
  fs_attr->fs_smax_count  = obd_count;

  lum = hioi_alloc_lustre_data ();
  assert (NULL != lum);

  rc = llapi_file_get_stripe (path, lum);
  if (0 != rc) {
    /* assuming this is a directory try reading the default for the directory. this
     * should be updated to check the parent directory if path is a file. */
    fd = open (path, O_RDONLY);
    if (-1 == fd) {
      free (lum);
      return hioi_err_errno (errno);
    }

    rc = ioctl (fd, LL_IOC_LOV_GETSTRIPE, lum);
    close (fd);
    if (0 > rc) {
      free (lum);
      return hioi_err_errno (errno);
    }
  }

  fs_attr->fs_scount = lum->lmm_stripe_count ? lum->lmm_stripe_count : 1;
  fs_attr->fs_ssize  = lum->lmm_stripe_size;

  switch (lum->lmm_pattern) {
  case LOV_PATTERN_RAID0:
    fs_attr->fs_raid_level = 0;
    break;
#if defined(LOV_PATTERN_RAID1)
  case LOV_PATTERN_RAID1:
    fs_attr->fs_raid_level = 1;
    break;
#endif
  default:
    fs_attr->fs_raid_level = -1;
    break;
  }

  free (lum);

  return HIO_SUCCESS;
}

static int hioi_fs_set_stripe_lustre (const char *path, hio_fs_attr_t *fs_attr) {
  struct lov_user_md lum;
  int fd, rc;

  fd = open (path, O_RDONLY);
  if (0 > fd) {
    return hioi_err_errno (errno);
  }

  errno = 0;

  /* NTH: use the default layout/starting index for now */
  lum.lmm_magic = LOV_USER_MAGIC;
  switch (fs_attr->fs_raid_level) {
  case 0:
    lum.lmm_pattern = LOV_PATTERN_RAID0;
    break;
#if defined(LOV_PATTERN_RAID1)
  case 1:
    lum.lmm_pattern = LOV_PATTERN_RAID1;
    break;
#endif
  default:
    lum.lmm_pattern = 0;
  }

  lum.lmm_pattern = fs_attr->fs_raid_level;
  lum.lmm_stripe_size = fs_attr->fs_ssize;
  lum.lmm_stripe_count = fs_attr->fs_scount;
  lum.lmm_stripe_offset = -1;

  rc = ioctl (fd, LL_IOC_LOV_SETSTRIPE, &lum);

  close (fd);

  return hioi_err_errno (errno);
}
#endif

#if HIO_USE_DATAWARP
static int hioi_fs_query_datawarp (const char *path, hio_fs_attr_t *fs_attr) {
  int stripe_size, stripe_width, rc, fd, start;

  fd = open (path, O_RDONLY);
  if (-1 == fd) {
    return hioi_err_errno (errno);
  }

  rc = dw_get_stripe_configuration (fd, &stripe_size, &stripe_width, &start);
  if (0 != rc) {
    close (fd);
    return hioi_err_errno (errno);
  }

  fs_attr->fs_flags |= HIO_FS_SUPPORTS_STRIPING;
  fs_attr->fs_type        = HIO_FS_TYPE_DATAWARP;
  fs_attr->fs_sunit       = 4096;
  fs_attr->fs_smax_size   = 1ul << 34;
  /* query returns the max stripe width not the current */
  fs_attr->fs_smax_count  = stripe_width;
  fs_attr->fs_scount      = stripe_width;
  fs_attr->fs_ssize       = stripe_size;

  return HIO_SUCCESS;
}

static int hioi_fs_set_stripe_datawarp (const char *path, hio_fs_attr_t *fs_attr) {
  int fd, rc;

  fd = open (path, O_RDONLY);
  if (-1 == fd) {
    return hioi_err_errno (errno);
  }

  rc = dw_set_stripe_configuration (fd, fs_attr->fs_ssize, fs_attr->fs_scount);

  close (fd);

  return hioi_err_errno (-rc);
}
#endif

int hioi_fs_set_stripe (const char *path, hio_fs_attr_t *fs_attr) {
  switch (fs_attr->fs_type) {
#if HIO_USE_DATAWARP
  case HIO_FS_TYPE_DATAWARP:
    return hioi_fs_set_stripe_datawarp (path, fs_attr);
#endif
#if defined(LL_SUPER_MAGIC)
  case HIO_FS_TYPE_LUSTRE:
    return hioi_fs_set_stripe_lustre (path, fs_attr);
#endif
  default:
    return HIO_ERR_NOT_AVAILABLE;
  }

  return HIO_ERR_NOT_AVAILABLE;
}

int hioi_fs_query_single (hio_context_t context, const char *path, hio_fs_attr_t *fs_attr) {
  struct statfs fsinfo;
  char tmp[4096];
  int rc;

  if (NULL == path) {
    return HIO_ERR_BAD_PARAM;
  }

  do {
    if (NULL == realpath (path, tmp)) {
      fs_attr->fs_type = hioi_err_errno (errno);
      break;
    }

    /* get general filesystem data */
    rc = statfs (tmp, &fsinfo);
    if (0 > rc) {
      hioi_log(context, HIO_VERBOSE_DEBUG_LOW, "statfs path:%s rc:%d errno:%d(%s)", tmp, rc, errno, strerror(errno));  
      fs_attr->fs_type = hioi_err_errno (errno);
      break;
    }

    memset (fs_attr, 0, sizeof (*fs_attr));

    fs_attr->fs_bavail  = fsinfo.f_bavail;
    fs_attr->fs_btotal  = fsinfo.f_blocks;
    fs_attr->fs_bsize   = fsinfo.f_bsize;

    /* set some reasonable defaults for striping parameters */
    fs_attr->fs_scount = 1;
    fs_attr->fs_ssize = fs_attr->fs_bsize;

    /* get filesytem specific data */
    switch (fsinfo.f_type) {
#if defined(LL_SUPER_MAGIC)
    case LL_SUPER_MAGIC:
      hioi_fs_query_lustre (tmp, fs_attr);
      break;
#endif

#if defined(GPFS_SUPER_MAGIC)
    case GPFS_SUPER_MAGIC:
      /* gpfs */
      break;
#endif

#if defined(PAN_FS_CLIENT_MAGIC)
    case PAN_FS_CLIENT_MAGIC:
      /* panfs */
      break;
#endif
#if HIO_USE_DATAWARP
    case DW_SUPER_MAGIC:
      hioi_fs_query_datawarp (tmp, fs_attr);
      break;
#endif
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "filesystem query: path: %s, type: %d, flags: 0x%x, block size: %" PRIu64
              " block count: %" PRIu64 " blocks free: %" PRIu64 " stripe count: %" PRIu32 " stripe max count: %" PRIu32
              " stripe unit: %" PRIu64 " stripe size: %" PRIu64 " stripe max size: %" PRIu64, tmp, fs_attr->fs_type,
              fs_attr->fs_flags, fs_attr->fs_bsize, fs_attr->fs_btotal, fs_attr->fs_bavail, fs_attr->fs_scount,
              fs_attr->fs_smax_count, fs_attr->fs_sunit, fs_attr->fs_ssize, fs_attr->fs_smax_size);

  } while (0);

  if (0 > fs_attr->fs_type) {
    return fs_attr->fs_type;
  }

  fs_attr->fs_open = hio_fs_open_fns[fs_attr->fs_type];
  /* if this assert is hit the above array needs to be updated */
  assert (NULL != fs_attr->fs_open);

  return HIO_SUCCESS;
}

int hioi_fs_query (hio_context_t context, const char *path, hio_fs_attr_t *fs_attr) {
  if (0 == context->c_rank) {
    hioi_fs_query_single (context, path, fs_attr);
  }

#if HIO_MPI_HAVE(1)
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (fs_attr, sizeof (*fs_attr), MPI_BYTE, 0, context->c_comm);
  }
#endif
  if (0 > fs_attr->fs_type) {
    return fs_attr->fs_type;
  }

  fs_attr->fs_open = hio_fs_open_fns[fs_attr->fs_type];
  /* if this assert is hit the above array needs to be updated */
  assert (NULL != fs_attr->fs_open);

  return HIO_SUCCESS;
}
