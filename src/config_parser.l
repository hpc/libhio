%option nounput
%option noinput


%{/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "config_parser.h"

int   config_parser_section_type = HIO_OBJECT_TYPE_ANY;
char *config_parser_section_identifier = NULL;
const char *config_parser_prefix = NULL;
int   config_parser_prefix_length;
char *config_parser_value = NULL;
char *config_parser_key = NULL;

static void hioi_config_parse_set_section_identifier (char *identifier) {
  if (config_parser_section_identifier) {
    free (config_parser_section_identifier);
    config_parser_section_identifier = 0;
  }

  if (identifier) {
    config_parser_section_identifier = strdup(identifier);
  }
}

static void hioi_config_parse_set_key (const char *key) {
  char *tmp;

  if (config_parser_key) {
    free (config_parser_key);
  }

  config_parser_key = strdup (key);
  if (NULL != (tmp = strchr (key, ' '))) {
    *tmp = '\0';
  }
}

static void hioi_config_parse_set_value (const char *value) {
  if (config_parser_value) {
    free (config_parser_value);
  }

  config_parser_value = strdup (value);
}

void hioi_config_parser_set_file_prefix (const char *prefix) {
  if (NULL == prefix) {
    prefix = "";
  }

  config_parser_prefix = prefix;
  config_parser_prefix_length = strlen (prefix);
}

static int hioi_config_parserwrap (void) {
  return 1;
}
%}

WHITE        [\f\t\v ]
CHAR         [A-Za-z0-9_\-\.]
ANYCHAR      [^\n \f\t\v]
WHOLELINE    [^\n]*

%x section
%x context
%x dataset
%x element
%x valid
%x key
%x value

%%

<*>{WHITE}+      /* squash whitespace */
<valid,INITIAL>\n {
  BEGIN(INITIAL);
  return HIOI_CONFIG_PARSER_PARSE_EMPTY;
}

<section,context,dataset,element,key>\n {
  return HIOI_CONFIG_PARSER_PARSE_ERROR;
}

^{WHOLELINE}\n {
  if ((config_parser_prefix_length && !strncmp (config_parser_prefix, yytext, config_parser_prefix_length)) ||
      (!config_parser_prefix_length && yytext[0] != '#')) {
    yyless (config_parser_prefix_length);
    BEGIN(valid);
  } else {
    BEGIN(INITIAL);
    return HIOI_CONFIG_PARSER_PARSE_EMPTY;
  }
}

<valid>"[global]"{WHOLELINE} {
  config_parser_section_type = HIO_OBJECT_TYPE_ANY;
  hioi_config_parse_set_section_identifier (NULL);
}

<valid>"[" {
  BEGIN(section);
}

<section>context: {
  config_parser_section_type = HIO_OBJECT_TYPE_CONTEXT;
  BEGIN(context);
}
<section>dataset: {
  config_parser_section_type = HIO_OBJECT_TYPE_DATASET;
  BEGIN(dataset);
}
<section>element: {
  config_parser_section_type = HIO_OBJECT_TYPE_ELEMENT;
  BEGIN(element);
}

<context,dataset,element>{CHAR}+ {
  hioi_config_parse_set_section_identifier (yytext);
}

<context,dataset,element>"]"{WHOLELINE}\n {
  BEGIN(INITIAL);
  return HIOI_CONFIG_PARSER_PARSE_EMPTY;
}

<valid>{CHAR}+ {
  hioi_config_parse_set_key (yytext);
  BEGIN(key);
}

<key>"="{WHITE}* {
  BEGIN(value);
}

<value>{ANYCHAR}* {
  hioi_config_parse_set_value (yytext);
  BEGIN(INITIAL);
  return HIOI_CONFIG_PARSER_PARSE_KV;
}

<value>\n {
  hioi_config_parse_set_value ("");
  BEGIN(INITIAL);
  return HIOI_CONFIG_PARSER_PARSE_KV;
}

<key>[^=]+\n {
  return HIOI_CONFIG_PARSER_PARSE_ERROR;
}

. {
  return HIOI_CONFIG_PARSER_PARSE_ERROR;
}

%%

int hioi_config_parser_parse_line (char *line, char **keyp, char **valuep, char **contextp,
				   hio_object_type_t *context_typep) {
  int rc;
  YY_BUFFER_STATE buffer;
  char *line_buffer = malloc (strlen (line) + 2);

  sprintf (line_buffer, "%s\n", line);

  buffer = hioi_config_parser_scan_string (line_buffer);

  rc = hioi_config_parserlex ();
  if (HIOI_CONFIG_PARSER_PARSE_KV == rc) {
    *keyp = config_parser_key;
    *valuep = config_parser_value;
    *contextp = config_parser_section_identifier;
    *context_typep = config_parser_section_type;
  }

  free (line_buffer);

  hioi_config_parser_delete_buffer (buffer);

  return rc;
}

void hioi_config_parser_reset (void) {
  config_parser_section_type = HIO_OBJECT_TYPE_ANY;
  hioi_config_parse_set_section_identifier (NULL);
  hioi_config_parse_set_key (NULL);
  hioi_config_parse_set_value (NULL);
}
