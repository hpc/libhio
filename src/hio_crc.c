/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2003-2015 Nathan Hjelm <hjelmn@cs.unm.edu>. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_internal.h"

#define CRC32POLY 0x04C11DB7l
#define CRC64POLY 0xd800000000000000ll

static uint32_t crc32_table[256];
static uint64_t crc64_table[256];

static bool crc_initialized = false;

pthread_mutex_t crc_init_lock = PTHREAD_MUTEX_INITIALIZER;

static void crc_init_tables (void) {
  pthread_mutex_lock (&crc_init_lock);
  if (crc_initialized) {
    pthread_mutex_unlock (&crc_init_lock);
    return;
  }

  for (int i = 0 ; i < 256 ; i++) {
    uint32_t r = i;

    for (int j = 0; j < 8; j++)  {
      if (r & 1)
        r = (r >> 1) ^ CRC32POLY;
      else
        r >>= 1;
    }

    crc32_table[i] = r;
  }

  for (int i = 0 ; i < 256 ; i++) {
    uint64_t r = i;

    for (int j = 0; j < 8; j++)  {
      if (r & 1)
        r = (r >> 1) ^ CRC64POLY;
      else
        r >>= 1;
    }

    crc64_table[i] = r;
  }

  crc_initialized = true;
  pthread_mutex_unlock (&crc_init_lock);
}

uint32_t hioi_crc32 (uint8_t *buf, size_t length) {
  uint32_t crc = 0;
  
  if (!crc_initialized)
    crc_init_tables ();
  
  for (int i = 0 ; i < length ; i++)
    crc = (crc >> 8) ^ crc32_table[(crc ^ buf[i]) & 0xff];
  
  return crc;
}

uint64_t hioi_crc64 (uint8_t *buf, size_t length) {
  uint64_t crc = 0;
  
  if (!crc_initialized)
    crc_init_tables ();
  
  for (int i = 0 ; i < length ; ++i)
    crc = (crc >> 8) ^ crc64_table[(crc ^ buf[i]) & 0xff];

  return crc;
}
