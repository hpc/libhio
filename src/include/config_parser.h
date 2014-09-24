/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

/**
 * @file context.c
 * @brief hio config file parser
 */

#if !defined(CONFIG_PARSER_H)
#define CONFIG_PARSER_H

#include "hio_internal.h"

/**
 * Set the configuration file parser prefix
 *
 * @param[in] prefix   prefix to use when parsing the configuration
 */
void hioi_config_parser_set_file_prefix (const char *prefix);

enum {
  /** parser hit valid key value */
  HIOI_CONFIG_PARSER_PARSE_KV,
  /** no new parser data */
  HIOI_CONFIG_PARSER_PARSE_EMPTY,
  /** parsing error */
  HIOI_CONFIG_PARSER_PARSE_ERROR,
};

/**
 * Parse a line from a configuration file
 *
 * @param[in]  line          line to parse
 * @param[out] keyp          parsed key
 * @param[out] valuep        parsed value
 * @param[out] object_namep  key/value context
 * @param[out] object_typep  type of object
 *
 * @returns HIOI_CONFIG_PARSER_PARSE_KV if a valid key and value were found on the
 *          line.
 * @returns HIOI_CONFIG_PARSER_PARSE_EMPTY if the line does not contain a key value
 *          pair.
 * @returns HIOI_CONFIG_PARSER_PARSE_ERROR if a parse error occurred
 *
 * All strings returned by this function are owned by the key/value parser. Do
 * not attempt to free or modify any returned string.
 */
int hioi_config_parser_parse_line (char *line, char **keyp, char **valuep, char **contextp,
				   hio_object_type_t *context_typep);

void hioi_config_parser_reset (void);

#endif /* !defined(CONFIG_PARSER_H) */
