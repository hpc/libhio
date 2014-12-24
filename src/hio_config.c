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

#include "hio_internal.h"
#include "hio_types.h"
#include "config_parser.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

#include <string.h>
#include <inttypes.h>

#include <errno.h>
#include <sys/stat.h>

static const char *hio_config_prefix = "HIO_";

/**
 * Check if the variable exists in the context.
 *
 * @param[in] context   context to search
 * @param[in] name      variable name to lookup
 *
 * This does a linear search of the context configuration. Ideally,
 * there will never be a large number of configuration variables. If
 * that isn't the case then this should be updated to keep a hash
 * table for O(1) lookup.
 */
int hioi_config_lookup (hio_config_t *config, const char *name) {
  int config_index = -1;

  for (int i = 0 ; i < config->config_var_count ; ++i) {
    if (0 == strcmp (name, config->config_var[i].var_name)) {
      config_index = i;
      break;
    }
  }

  return config_index;
}

static uint64_t hioi_string_to_int (const char *strval) {
  uint64_t value = 0;
  char *tmp;

  value = strtol (strval, &tmp, 0);
  if (tmp == strval) {
    return 0;
  }

  if (*tmp) {
    switch (*tmp) {
    case 'G':
    case 'g':
      value <<= 10;
    case 'M':
    case 'm':
      value <<= 10;
    case 'K':
    case 'k':
      value <<= 10;
    }
  }

  return value;
}

static int hioi_config_set_value_internal (hio_config_var_t *var, const char *strval) {
  uint64_t intval = hioi_string_to_int(strval);

  if (NULL == strval) {
    /* empty value. nothing to do */
    return HIO_SUCCESS;
  }

  switch (var->var_type) {
  case HIO_CONFIG_TYPE_BOOL:
    if (0 == strcmp (strval, "true") || 0 == strcmp (strval, "t") ||
        0 == strcmp (strval, "1")) {
      var->var_storage->boolval = true;
    } else if (0 == strcmp (strval, "false") || 0 == strcmp (strval, "f") ||
               0 == strcmp (strval, "0")) {
      var->var_storage->boolval = false;
    } else {
      var->var_storage->boolval = !!intval;
    }

    break;
  case HIO_CONFIG_TYPE_STRING:
    if (var->var_storage->strval) {
      free (var->var_storage->strval);
    }

    var->var_storage->strval = strdup (strval);

    break;
  case HIO_CONFIG_TYPE_INT32:
    var->var_storage->int32val = (int32_t) (intval & 0xffffffff);
    break;
  case HIO_CONFIG_TYPE_UINT32:
    var->var_storage->uint32val = (uint32_t) (intval & 0xffffffffu);
    break;
  case HIO_CONFIG_TYPE_INT64:
    var->var_storage->int64val = (int64_t) intval;
    break;
  case HIO_CONFIG_TYPE_UINT64:
    var->var_storage->uint64val = intval;
    break;
  case HIO_CONFIG_TYPE_FLOAT:
    var->var_storage->floatval = strtof (strval, NULL);
    break;
  case HIO_CONFIG_TYPE_DOUBLE:
    var->var_storage->doubleval = strtod (strval, NULL);
    break;
  }

  return HIO_SUCCESS;
}

/**
 * Search for a matching value in the configuration file.
 *
 * @param[in] context  context to search
 * @param[in] object   associated object
 * @param[in] var      variable to set
 *
 * This function currently does a linear search of the configuration
 * file. In the future this should be replaced with a hash table or
 * similar structure.
 */
static int hioi_config_set_from_file (hio_context_t context, hio_object_t object,
				      hio_config_var_t *var) {
  for (int i = 0 ; i < context->context_file_configuration_count ; ++i) {
    hio_config_kv_t *kv = context->context_file_configuration + i;
    if ((HIO_OBJECT_TYPE_ANY == kv->object_type || object->type == kv->object_type) &&
        (NULL == kv->object_identifier || !strcmp (object->identifier, kv->object_identifier)) &&
        !strcmp (var->var_name, kv->key)) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Setting value for %s to %s from file",
                var->var_name, kv->value);
      return hioi_config_set_value_internal (var, kv->value);
    }
  }

  return HIO_SUCCESS;
}

static int hioi_config_set_from_env (hio_context_t context, hio_object_t object,
				     hio_config_var_t *var) {
  char *string_value;
  char env_name[256];

  if (HIO_OBJECT_TYPE_DATASET == object->type) {
    /* check for dataset specific variables */
    snprintf (env_name, 256, "%sdataset_%s_%s_%s", hio_config_prefix, context->context_object.identifier,
              object->identifier, var->var_name);

    string_value = getenv (env_name);
    if (NULL != string_value) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Setting value for %s to %s from ENV %s",
                var->var_name, string_value, env_name);
      return hioi_config_set_value_internal (var, string_value);
    }

    snprintf (env_name, 256, "%sdataset_%s_%s", hio_config_prefix, object->identifier, var->var_name);

    string_value = getenv (env_name);
    if (NULL != string_value) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Setting value for %s to %s from ENV %s",
                var->var_name, string_value, env_name);
      return hioi_config_set_value_internal (var, string_value);
    }
  }

  snprintf (env_name, 256, "%scontext_%s_%s", hio_config_prefix, context->context_object.identifier,
            var->var_name);

  string_value = getenv (env_name);
  if (NULL != string_value) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Setting value for %s to %s from ENV %s",
              var->var_name, string_value, env_name);
    return hioi_config_set_value_internal (var, string_value);
  }

  snprintf (env_name, 256, "%s%s_%s", hio_config_prefix, context->context_object.identifier,
            var->var_name);

  string_value = getenv (env_name);
  if (NULL != string_value) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Setting value for %s to %s from ENV %s",
              var->var_name, string_value, env_name);
    return hioi_config_set_value_internal (var, string_value);
  }

  return HIO_SUCCESS;
}

int hioi_config_add (hio_context_t context, hio_object_t object, void *addr, const char *name,
		     hio_config_type_t type, void *reserved0, const char *description, int flags) {
  hio_config_t *config = &object->configuration;
  hio_config_var_t *new_var;
  int config_index;

  config_index = hioi_config_lookup (config, name);
  if (config_index >= 0) {
    /* do not allow duplicate configuration registration for now */
    return HIO_ERROR;
  }

  config_index = config->config_var_count++;

  if (config->config_var_size <= config_index) {
    size_t new_size;
    void *tmp;

    /* grow the configuration array by a little */
    new_size = (config->config_var_size + 16) * sizeof (hio_config_var_t);

    tmp = realloc (config->config_var, new_size);
    if (NULL == tmp) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    config->config_var_size += 16;
    config->config_var = tmp;
  }

  new_var = config->config_var + config_index;

  new_var->var_name = strdup (name);
  if (NULL == new_var->var_name) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  new_var->var_type        = type;
  new_var->var_description = description;
  new_var->var_flags       = flags;
  new_var->var_storage     = (hio_var_value_t *) addr;

  hioi_config_set_from_file (context, object, new_var);
  hioi_config_set_from_env (context, object, new_var);

  return HIO_SUCCESS;
}

static int hioi_config_kv_push (hio_context_t context, const char *identifier,
                                hio_object_type_t type, const char *key, const char *value) {
  int new_index, value_length;
  hio_config_kv_t *kv;
  void *tmp;

  for (int i = 0 ; i < context->context_file_configuration_count ; ++i) {
    kv = context->context_file_configuration + i;

    if (!strcmp (kv->key, key) && (kv->object_type == type ||
                                   HIO_OBJECT_TYPE_ANY == kv->object_type)) {
      break;
    }
    kv = NULL;
  }

  if (NULL == kv) {
    if (context->context_file_configuration_count == context->context_file_configuration_size) {
      int new_size = context->context_file_configuration_size + 16;

      tmp = realloc (context->context_file_configuration, new_size * sizeof (hio_config_kv_t));
      if (NULL == tmp) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      context->context_file_configuration = tmp;
    }

    new_index = context->context_file_configuration_count++;
    kv = context->context_file_configuration + new_index;

    kv->key = strdup (key);
  } else {
    if (kv->value) {
      free (kv->value);
    }
    if (kv->object_identifier) {
      free (kv->object_identifier);
    }
  }

  value_length = strlen (value);
  if (('"' == value[0] || '\'' == value[0]) && value[0] == value[value_length - 1]) {
    value++;
    value_length -= 2;
  }

  kv->value = strdup (value);

  kv->value[value_length] = '\0';

  if (identifier) {
    kv->object_identifier = strdup (identifier);
  } else {
    kv->object_identifier = NULL;
  }

  kv->object_type = type;

  return HIO_SUCCESS;
}

int hioi_config_parse (hio_context_t context, const char *config_file, const char *config_file_prefix) {
  char *key, *value, *default_file = NULL, *buffer, *line, *lastl;
  int data_size = 0, fd, rc = HIO_SUCCESS;
  struct stat statinfo;

  if (NULL == config_file) {
    /* nothing to do */
    return HIO_SUCCESS;
  }

  if (!hioi_context_using_mpi (context) || 0 == context->context_rank) {
    if (HIO_CONFIG_FILE_DEFAULT == config_file) {
      asprintf (&default_file, "%s.cfg", context->context_object.identifier);
      config_file = default_file;
    }

    if (stat (config_file, &statinfo)) {
      data_size = 0;
    } else {
      data_size = statinfo.st_size;
    }

    fd = open (config_file, O_RDONLY);
    if (0 > fd) {
      hio_err_push (HIO_ERR_NOT_FOUND, context, NULL, "Could not open configuration file %s for reading. "
                    "errno: %d", config_file, errno);
      return HIO_ERR_NOT_FOUND;
    }

    if (default_file) {
      free (default_file);
    }
  }

#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (&data_size, 1, MPI_UNSIGNED, 0, context->context_comm);
  }
#endif

  if (0 == data_size) {
    close (fd);
    return HIO_ERR_NOT_FOUND;
  }

  buffer = calloc (data_size, 1);
  if (NULL == buffer) {
    close (fd);
    return HIO_ERR_OUT_OF_RESOURCE;
  }


  if (!hioi_context_using_mpi (context) || 0 == context->context_rank) {
    rc = read (fd, buffer, data_size);
    if (data_size != rc) {
      hio_err_push (HIO_ERR_TRUNCATE, context, NULL, "Read from configuration file %s trucated",
                    config_file);
    }

    close (fd);
  }

#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (buffer, data_size, MPI_BYTE, 0, context->context_comm);
  }
#endif

  if (config_file_prefix && 0 == strlen (config_file_prefix)) {
      config_file_prefix = NULL;
  }

  line = strtok_r (buffer, "\n", &lastl);

  rc = HIO_SUCCESS;

  hioi_config_parser_set_file_prefix (config_file_prefix);

  do {
    char *identifier;
    hio_object_type_t type;

    rc = hioi_config_parser_parse_line (line, &key, &value, &identifier, &type);
    if (HIOI_CONFIG_PARSER_PARSE_ERROR == rc) {
      hio_err_push (HIO_ERROR, context, NULL, "Error parsing input file");
      rc = HIO_ERROR;
      break;
    }

    if (HIOI_CONFIG_PARSER_PARSE_KV == rc) {
      if (HIO_OBJECT_TYPE_CONTEXT == type && strcmp (identifier, context->context_object.identifier)) {
        continue;
      }

      hioi_config_kv_push (context, identifier, type, key, value);
    }
    rc = HIO_SUCCESS;
  } while (NULL != (line = strtok_r (NULL, "\n", &lastl)));

  free (buffer);

  return rc;
}


int hioi_config_init (hio_object_t object) {
  hio_config_t *config = &object->configuration;

  config->config_var = NULL;
  config->config_var_count = 0;
  config->config_var_size = 0;

  return HIO_SUCCESS;
}

void hioi_config_fini (hio_object_t object) {
  hio_config_t *config = &object->configuration;

  if (config->config_var) {
    for (int i = 0 ; i < config->config_var_count ; ++i) {
      hio_config_var_t *var = config->config_var + i;

      if (var->var_name) {
	free (var->var_name);
      }
    }

    free (config->config_var);
    config->config_var = NULL;
    config->config_var_count = 0;
    config->config_var_size = 0;
  }
}

int hio_config_set_value (hio_object_t object, char *variable, char *value) {
  hio_config_var_t *var;
  int config_index;

  config_index = hioi_config_lookup (&object->configuration, variable);
  if (0 > config_index) {
    return HIO_ERR_NOT_FOUND;
  }

  var = object->configuration.config_var + config_index;

  if (HIO_VAR_FLAG_READONLY & var->var_flags) {
    return HIO_ERR_PERM;
  }

  return hioi_config_set_value_internal (var, value);
}

int hio_config_get_value (hio_object_t object, char *variable, char **value) {
  hio_config_var_t *var;
  int config_index, rc = HIO_SUCCESS;

  config_index = hioi_config_lookup (&object->configuration, variable);
  if (0 > config_index) {
    return HIO_ERR_NOT_FOUND;
  }

  var = object->configuration.config_var + config_index;

  switch (var->var_type) {
  case HIO_CONFIG_TYPE_BOOL:
    rc = asprintf (value, "%s", var->var_storage->boolval ? "true" : "false");
    break;
  case HIO_CONFIG_TYPE_STRING:
    *value = strdup (var->var_storage->strval);
    if (NULL == *value) {
      rc = -1;
    }
    break;
  case HIO_CONFIG_TYPE_INT32:
    rc = asprintf (value, "%i", var->var_storage->int32val);
    break;
  case HIO_CONFIG_TYPE_UINT32:
    rc = asprintf (value, "%u", var->var_storage->uint32val);
    break;
  case HIO_CONFIG_TYPE_INT64:
    rc = asprintf (value, "%lli", var->var_storage->int64val);
    break;
  case HIO_CONFIG_TYPE_UINT64:
    rc = asprintf (value, "%llu", var->var_storage->uint64val);
    break;
  case HIO_CONFIG_TYPE_FLOAT:
    rc = asprintf (value, "%f", var->var_storage->floatval);
    break;
  case HIO_CONFIG_TYPE_DOUBLE:
    rc = asprintf (value, "%lf", var->var_storage->doubleval);
    break;
  }

  if (rc < 0) {
    return HIO_ERROR;
  }

  return HIO_SUCCESS;
}

int hio_config_get_count (hio_object_t object, int *count) {
  *count = object->configuration.config_var_count;
  return HIO_SUCCESS;
}

int hio_config_get_info (hio_object_t object, int index, char **name, hio_config_type_t *type,
                         bool *read_only) {
  hio_config_var_t *var;

  if (index >= object->configuration.config_var_count) {
    return HIO_ERR_NOT_FOUND;
  }

  var = object->configuration.config_var + index;

  if (name) {
    *name = strdup (var->var_name);
  }

  if (type) {
    *type = var->var_type;
  }

  if (read_only) {
    *read_only = !!(var->var_flags & HIO_VAR_FLAG_READONLY);
  }

  return HIO_SUCCESS;
}
