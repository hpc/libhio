/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <stdlib.h>
#include <getopt.h>
#include <errno.h>
#include <string.h>

#include "hio.h"
#include "hio_internal.h"

static void print_usage (void) {
  printf ("Usage: hio_ds_info [options] <data root> <context name> [dataset name [dataset id]]\n");
  printf ("Dump metadata associated with libhio dataset(s).\n");

  exit (EXIT_FAILURE);
}

static void print_version (void) {
  printf ("hio_ds_info %s", VERSION);
  printf ("Copyright © 2016 Los Alamos National Security, LLC\n\n");

  printf("libhio comes with NO WARRANTY.\n");
  printf("You may redistribute copies of libhio under the terms\n");
  printf("of the BSD 3-Clause License.\n");
  printf("For more information about these issues\n");
  printf("see the file named COPYING in the libhio distribution.\n");

  exit (EXIT_SUCCESS);
}

int main (int argc, char *argv[]) {
  const char *data_root, *context = NULL, *dataset = NULL;
  int rc, opt, opt_index, list_rank = -1;
  int64_t dataset_id = -1;
  uint32_t flags = 0;

  struct option long_options[] = {
    {.name = "all", .has_arg = no_argument, .flag = NULL, .val = 'a'},
    {.name = "config", .has_arg = no_argument, .flag = NULL, .val = 'c'},
    {.name = "elements", .has_arg = no_argument, .flag = NULL, .val = 'e'},
    {.name = "help", .has_arg = no_argument, .flag = NULL, .val = 'h'},
    {.name = "perform", .has_arg = no_argument, .flag = NULL, .val = 'p'},
    {.name = "rank", .has_arg = required_argument, .flag = NULL, .val = 'r'},
    {.name = "version", .has_arg = no_argument, .flag = NULL, .val = 'v'},
  };

  do {
    opt = getopt_long (argc, argv, "aer:hvcp", long_options, &opt_index);
    if (-1 == opt) {
      break;
    }

    switch (opt) {
    case 'a':
      flags = HIO_DUMP_FLAG_MASK;
      break;
    case 'c':
      flags |= HIO_DUMP_FLAG_CONFIG;
      break;
    case 'e':
      flags |= HIO_DUMP_FLAG_ELEMENTS;
      break;
    case 'h':
      print_usage ();
      break;
    case 'p':
      flags |= HIO_DUMP_FLAG_PERF;
      break;
    case 'r':
      list_rank = strtol (optarg, NULL, 0);
      break;
    case 'v':
      print_version ();
      break;
    default:
      fprintf (stderr, "Invalid option \'%c\' specified.\n", opt);
      print_usage ();
    }
  } while (1);

  if (argc - optind < 2) {
    print_usage ();
  }

  data_root = argv[optind++];
  context = argv[optind++];
  if (argc - optind) {
    dataset = argv[optind++];
    if (argc - optind) {
      errno = 0;
      dataset_id = strtol (argv[optind], NULL, 10);
      if (0 != errno) {
        if (0 == strcasecmp (argv[optind], "newest")) {
          dataset_id = HIO_DATASET_ID_NEWEST;
        } else if (0 == strcasecmp (argv[optind], "highest_id")) {
          dataset_id = HIO_DATASET_ID_HIGHEST;
        } else {
          fprintf (stderr, "Invalid dataset id: %s. Valid values: positive integer, newest, highest_id\n", argv[optind]);
        }
      }
    }
  }

  hio_dataset_dump (data_root, context, dataset, dataset_id, flags, list_rank, stdout);

  return 0;
}
