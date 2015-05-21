/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include <libxml/parser.h>
#include <libxml/xmlsave.h>

#define HIO_MANIFEST_VERSION "0.4"
#define HIO_MANIFEST_COMPAT  "1.0"

#define HIO_MANIFEST_PROP_VERSION     (const xmlChar *) "hio_version"
#define HIO_MANIFEST_PROP_COMPAT      (const xmlChar *) "hio_compat"
#define HIO_MANIFEST_PROP_IDENTIFIER  (const xmlChar *) "identifier"
#define HIO_MANIFEST_PROP_DATASET_ID  (const xmlChar *) "dataset_id"

#define HIO_MANIFEST_KEY_BACKING_FILE (const xmlChar *) "hio_backing_file"
#define HIO_MANIFEST_KEY_DATASET_MODE (const xmlChar *) "hio_dataset_mode"
#define HIO_MANIFEST_KEY_FILE_MODE    (const xmlChar *) "hio_file_mode"
#define HIO_MANIFEST_KEY_MTIME        (const xmlChar *) "hio_mtime"
#define HIO_MANIFEST_KEY_COMM_SIZE    (const xmlChar *) "hio_comm_size"
#define HIO_MANIFEST_KEY_STATUS       (const xmlChar *) "hio_status"
#define HIO_SEGMENT_KEY_FILE_OFFSET   (const xmlChar *) "file_offset"
#define HIO_SEGMENT_KEY_APP_OFFSET0   (const xmlChar *) "app_offset0"
#define HIO_SEGMENT_KEY_APP_OFFSET1   (const xmlChar *) "app_offset1"
#define HIO_SEGMENT_KEY_LENGTH        (const xmlChar *) "length"

static void hioi_manifest_set_number (xmlNodePtr node, const xmlChar *name, unsigned long value) {
  char tmp[32];

  sprintf (tmp, "%lu", value);
  xmlNewChild (node, NULL, name, (const xmlChar *) tmp);
}

static void hioi_manifest_set_signed_number (xmlNodePtr node, const xmlChar *name, long value) {
  char tmp[32];

  sprintf (tmp, "%ld", value);
  xmlNewChild (node, NULL, name, (const xmlChar *) tmp);
}

static void hioi_manifest_prop_set_number (xmlNodePtr node, const xmlChar *name, unsigned long value) {
  char tmp[32];

  sprintf (tmp, "%lu", value);
  xmlNewProp (node, name, (const xmlChar *) tmp);
}

static void hioi_manifest_set_string (xmlNodePtr node, const xmlChar *name, const char *value) {
  xmlNewChild (node, NULL, name, (const xmlChar *) value);
}

static xmlNodePtr hioi_manifest_find_node (xmlNodePtr parent, const xmlChar *name) {
  xmlNodePtr cur;

  cur = parent->xmlChildrenNode;

  while (NULL != cur) {
    if (!strcmp ((char *) cur->name, (const char *) name)) {
      break;
    }
    cur = cur->next;
  }

  return cur;
}

static int hioi_manifest_get_string (xmlDocPtr xml_doc, xmlNodePtr node, const xmlChar *name,
				     xmlChar **string) {
  xmlNodePtr value_node;

  value_node = hioi_manifest_find_node (node, name);
  if (NULL == value_node) {
    return HIO_ERR_NOT_FOUND;
  }

  *string = xmlNodeListGetString (xml_doc, value_node->xmlChildrenNode, 1);
  if (!*string) {
    return HIO_ERROR;
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_get_number (xmlDocPtr xml_doc, xmlNodePtr node, const xmlChar *name,
				     unsigned long *value) {
  xmlChar *string_value;
  int rc;

  rc = hioi_manifest_get_string (xml_doc, node, name, &string_value);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  *value = strtoul ((char *) string_value, NULL, 0);
  xmlFree (string_value);

  return HIO_SUCCESS;
}

static int hioi_manifest_get_signed_number (xmlDocPtr xml_doc, xmlNodePtr node, const xmlChar *name,
                                            long *value) {
  xmlChar *string_value;
  int rc;

  rc = hioi_manifest_get_string (xml_doc, node, name, &string_value);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  *value = strtol ((char *) string_value, NULL, 0);
  xmlFree (string_value);

  return HIO_SUCCESS;
}

static int hioi_manifest_prop_get_string (xmlNodePtr node, const xmlChar *name,
                                          xmlChar **string) {
  *string = xmlGetProp (node, name);
  if (!*string) {
    return HIO_ERROR;
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_prop_get_number (xmlNodePtr node, const xmlChar *name,
                                          unsigned long *value) {
  xmlChar *string_value;
  int rc;

  rc = hioi_manifest_prop_get_string (node, name, &string_value);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  *value = strtoul ((char *) string_value, NULL, 0);
  xmlFree (string_value);

  return HIO_SUCCESS;
}

static xmlDocPtr hio_manifest_generate_xml_1_0 (hio_dataset_t dataset) {
  xmlNodePtr elements_node, element_node, segment_node, segments_node, top;
  hio_context_t context = dataset->dataset_context;
  hio_manifest_segment_t *segment;
  hio_element_t element;
  xmlDocPtr xml_doc;

  xml_doc = xmlNewDoc ((const xmlChar *) "1.0");
  if (NULL == xml_doc) {
    return NULL;
  }

  top = xmlNewNode(NULL, (const xmlChar *) "manifest");
  if (NULL == top) {
    hio_err_push (HIO_ERROR, dataset->dataset_context, &dataset->dataset_object,
		  "Could not get document root element");
    return NULL;
  }
  xmlDocSetRootElement(xml_doc, top);

  xmlNewProp (top, HIO_MANIFEST_PROP_VERSION, (const xmlChar *) HIO_MANIFEST_VERSION);
  xmlNewProp (top, HIO_MANIFEST_PROP_COMPAT, (const xmlChar *) HIO_MANIFEST_COMPAT);
  xmlNewProp (top, HIO_MANIFEST_PROP_IDENTIFIER, (const xmlChar *) dataset->dataset_object.identifier);
  hioi_manifest_prop_set_number (top, HIO_MANIFEST_PROP_DATASET_ID, (unsigned long) dataset->dataset_id);

  if (dataset->dataset_backing_file) {
    xmlNewChild (top, NULL, HIO_MANIFEST_KEY_BACKING_FILE, (const xmlChar *) dataset->dataset_backing_file);
  }

  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_DATASET_MODE, (unsigned long) dataset->dataset_mode);
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_FILE_MODE, (unsigned long) dataset->dataset_file_mode);
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_COMM_SIZE, (unsigned long) context->context_size);
  hioi_manifest_set_signed_number (top, HIO_MANIFEST_KEY_STATUS, (long) dataset->dataset_status);
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_MTIME, (unsigned long) time (NULL));

  if (HIO_FILE_MODE_BASIC == dataset->dataset_file_mode) {
    /* NTH: for now do not write elements for basic mode. this may change in future versions */
    return xml_doc;
  }

  elements_node = xmlNewChild (top, NULL, (const xmlChar *) "elements", NULL);
  if (NULL == elements_node) {
    xmlFreeDoc (xml_doc);
    return NULL;
  }

  hioi_list_foreach(element, dataset->dataset_element_list, struct hio_element_t, element_list) {
    element_node = xmlNewChild (elements_node, NULL, (const xmlChar *) "element", NULL);

    xmlNewProp (element_node, HIO_MANIFEST_PROP_IDENTIFIER, (const xmlChar *) element->element_object.identifier);

    if (element->element_backing_file) {
      xmlNewProp (element_node, HIO_MANIFEST_KEY_BACKING_FILE, (const xmlChar *) element->element_backing_file);
    }

    if (!hioi_list_empty (&element->element_segment_list)) {
      segments_node = xmlNewChild (element_node, NULL, (const xmlChar *) "segments", NULL);

      hioi_list_foreach(segment, element->element_segment_list, hio_manifest_segment_t, segment_list) {
        segment_node = xmlNewChild (segments_node, NULL, (const xmlChar *) "segment", NULL);

        hioi_manifest_set_number (segment_node, HIO_SEGMENT_KEY_FILE_OFFSET,
                                  (unsigned long) segment->segment_file_offset);
        hioi_manifest_set_number (segment_node, HIO_SEGMENT_KEY_APP_OFFSET0,
                                  (unsigned long) segment->segment_app_offset0);
        hioi_manifest_set_number (segment_node, HIO_SEGMENT_KEY_APP_OFFSET1,
                                  (unsigned long) segment->segment_app_offset1);
        hioi_manifest_set_number (segment_node, HIO_SEGMENT_KEY_LENGTH,
                                  (unsigned long) segment->segment_length);
      }
    }
  }

  return xml_doc;
}

int hioi_manifest_serialize (hio_dataset_t dataset, unsigned char **data, size_t *data_size) {
  xmlDocPtr xml_doc;
  int size;

  xml_doc = hio_manifest_generate_xml_1_0 (dataset);
  if (NULL == xml_doc) {
    return HIO_ERROR;
  }

  xmlDocDumpMemory (xml_doc, data, &size);
  xmlFreeDoc (xml_doc);
  if (NULL == *data) {
    return HIO_ERROR;
  }

  *data_size = (size_t) size;

  return HIO_SUCCESS;
}

int hioi_manifest_save (hio_dataset_t dataset, const char *path) {
  xmlSaveCtxtPtr save_context;
  xmlDocPtr xml_doc;
  long rc;

  xml_doc = hio_manifest_generate_xml_1_0 (dataset);
  if (NULL == xml_doc) {
    hio_err_push (HIO_ERROR, dataset->dataset_context, &dataset->dataset_object, "Could not generate manifest xml");
    return HIO_ERROR;
  }

  save_context = xmlSaveToFilename (path, NULL, XML_SAVE_FORMAT);
  if (NULL == save_context) {
    hio_err_push (HIO_ERROR, dataset->dataset_context, &dataset->dataset_object, "Could not create xml save context for file %s", path);
    xmlFreeDoc (xml_doc);
    return HIO_ERROR;
  }

  rc = xmlSaveDoc (save_context, xml_doc);
  xmlSaveClose (save_context);
  xmlFreeDoc (xml_doc);

  if (0 > rc) {
    return HIO_ERROR;
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_segment_1_0 (hio_element_t element, xmlDocPtr xml_doc,
					    xmlNodePtr segment_node) {
  unsigned long file_offset, app_offset0, app_offset1, length;
  int rc;

  rc = hioi_manifest_get_number (xml_doc, segment_node, HIO_SEGMENT_KEY_FILE_OFFSET,
				 &file_offset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_get_number (xml_doc, segment_node, HIO_SEGMENT_KEY_APP_OFFSET0,
				 &app_offset0);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_get_number (xml_doc, segment_node, HIO_SEGMENT_KEY_APP_OFFSET1,
				 &app_offset1);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_get_number (xml_doc, segment_node, HIO_SEGMENT_KEY_LENGTH,
				 &length);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  return hioi_element_add_segment (element, file_offset, app_offset0, app_offset1, length);
}

static int hioi_manifest_parse_segments_1_0 (hio_element_t element, xmlDocPtr xml_doc,
					     xmlNodePtr segments_node) {
  xmlNodePtr segment_node;
  int rc;

  segment_node = segments_node->xmlChildrenNode;

  while (segment_node) {
    if (!strcmp ((const char *) segment_node->name, "segment")) {
      rc = hioi_manifest_parse_segment_1_0 (element, xml_doc, segment_node);
      if (HIO_SUCCESS != rc) {
        return rc;
      }
    }

    segment_node = segment_node->next;
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_element_1_0 (hio_dataset_t dataset, xmlDocPtr xml_doc,
					    xmlNodePtr element_node) {
  hio_element_t element;
  xmlNodePtr segments_node;
  xmlChar *tmp_string;
  int rc;

  tmp_string = xmlGetProp (element_node, HIO_MANIFEST_PROP_IDENTIFIER);
  if (NULL == tmp_string) {
    hio_err_push (HIO_ERROR, dataset->dataset_context, &dataset->dataset_object,
                  "Manifest element missing identifier property. name: %s", element_node->name);
    return HIO_ERROR;
  }

  hioi_log (dataset->dataset_context, HIO_VERBOSE_DEBUG_LOW, "Manifest element: %s",
            tmp_string);

  element = hioi_element_alloc (dataset, (const char *) tmp_string);
  xmlFree (tmp_string);
  if (NULL == element) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  tmp_string = xmlGetProp (element_node, HIO_MANIFEST_KEY_BACKING_FILE);
  if (NULL != tmp_string) {
    element->element_backing_file = strdup ((char *) tmp_string);
    if (NULL == element->element_backing_file) {
      return  HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  segments_node = hioi_manifest_find_node (element_node, (const xmlChar *) "segments");

  if (NULL != segments_node) {
    rc = hioi_manifest_parse_segments_1_0 (element, xml_doc, segments_node);
    if (HIO_SUCCESS != rc) {
      hioi_element_release (element);
      return rc;
    }
  }

  hioi_dataset_add_element (dataset, element);

  hioi_log (dataset->dataset_context, HIO_VERBOSE_DEBUG_LOW, "Found element with identifier %s in manifest",
	    element->element_object.identifier);

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_elements_1_0 (hio_dataset_t dataset, xmlDocPtr xml_doc,
					     xmlNodePtr elements_node) {
  xmlNodePtr element_node;
  int rc;

  element_node = elements_node->xmlChildrenNode;

  while (element_node) {
    if (!strcmp ((const char *) element_node->name, "element")) {
      rc = hioi_manifest_parse_element_1_0 (dataset, xml_doc, element_node);
      if (HIO_SUCCESS != rc) {
        return rc;
      }
    }

    element_node = element_node->next;
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_1_0 (hio_dataset_t dataset, xmlDocPtr xml_doc) {
  xmlNodePtr top, elements_node;
  xmlChar *tmp_string;
  unsigned long mode = 0;
  long status;
  int rc;

  top = xmlDocGetRootElement (xml_doc);
  if (NULL == top) {
    hio_err_push (HIO_ERROR, dataset->dataset_context, &dataset->dataset_object,
		  "Could not retrieve xml root element");
    return HIO_ERROR;
  }

  /* check for compatibility with this manifest version */
  rc = hioi_manifest_prop_get_string (top, HIO_MANIFEST_PROP_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  hioi_log (dataset->dataset_context, HIO_VERBOSE_DEBUG_LOW, "Compatibilty version of manifest: %s",
	    (char *) tmp_string);

  if (strcmp ((char *) tmp_string, "1.0")) {
    xmlFree (tmp_string);
    /* incompatible version */
    return HIO_ERROR;
  }

  xmlFree (tmp_string);

  hioi_manifest_get_number (xml_doc, top, HIO_MANIFEST_KEY_DATASET_MODE, &mode);
  if (mode != dataset->dataset_mode) {
    hio_err_push (HIO_ERR_BAD_PARAM, dataset->dataset_context, &dataset->dataset_object,
		  "Mismatch in dataset mode. Requested mode: %d, File mode: %d", mode,
		  dataset->dataset_mode);
    return HIO_ERR_BAD_PARAM;
  }

  rc = hioi_manifest_get_number (xml_doc, top, HIO_MANIFEST_KEY_FILE_MODE, &mode);
  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  dataset->dataset_file_mode = mode;

  rc = hioi_manifest_get_signed_number (xml_doc, top, HIO_MANIFEST_KEY_STATUS, &status);
  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  dataset->dataset_status = status;

  /* get the file name of the backing file for this manifest */
  rc = hioi_manifest_get_string (xml_doc, top, HIO_MANIFEST_KEY_BACKING_FILE, &tmp_string);
  if (HIO_SUCCESS == rc) {
    dataset->dataset_backing_file = strdup ((const char *) tmp_string);
    xmlFree (tmp_string);
  }

  /* find and parse all elements covered by this manifest */
  elements_node = hioi_manifest_find_node (top, (const xmlChar *) "elements");
  if (NULL == elements_node) {
    /* no elements in this file. odd but still valid */
    return HIO_SUCCESS;
  }

  rc = hioi_manifest_parse_elements_1_0 (dataset, xml_doc, elements_node);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_header_1_0 (hio_context_t context, hio_dataset_header_t *header, xmlDocPtr xml_doc) {
  xmlNodePtr top, elements_node;
  xmlChar *tmp_string;
  unsigned long value;
  long svalue;
  int rc;

  top = xmlDocGetRootElement (xml_doc);
  if (NULL == top) {
    hio_err_push (HIO_ERROR, context, NULL, "Could not retrieve xml root element");
    return HIO_ERROR;
  }

  /* check for compatibility with this manifest version */
  rc = hioi_manifest_prop_get_string (top, HIO_MANIFEST_PROP_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Compatibilty version of manifest: %s", (char *) tmp_string);

  if (strcmp ((char *) tmp_string, "1.0")) {
    xmlFree (tmp_string);
    /* incompatible version */
    return HIO_ERROR;
  }

  xmlFree (tmp_string);

  /* fill in header */
  rc = hioi_manifest_get_number (xml_doc, top, HIO_MANIFEST_KEY_DATASET_MODE, &value);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->dataset_mode = value;

  rc = hioi_manifest_get_number (xml_doc, top, HIO_MANIFEST_KEY_FILE_MODE, &value);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->dataset_file_mode = value;

  rc = hioi_manifest_get_signed_number (xml_doc, top, HIO_MANIFEST_KEY_STATUS, &svalue);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->dataset_status = svalue;

  rc = hioi_manifest_prop_get_number (top, HIO_MANIFEST_PROP_DATASET_ID, &value);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->dataset_id = value;

  return HIO_SUCCESS;
}

int hioi_manifest_deserialize (hio_dataset_t dataset, unsigned char *data, size_t data_size) {
  xmlDocPtr xml_doc;
  int rc;

  xml_doc = xmlParseMemory ((const char *) data, data_size);
  if (NULL == xml_doc) {
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_1_0 (dataset, xml_doc);
  xmlFreeDoc (xml_doc);

  return rc;
}

int hioi_manifest_load (hio_dataset_t dataset, const char *path) {
  xmlDocPtr xml_doc;
  int rc;

  hioi_log (dataset->dataset_context, HIO_VERBOSE_DEBUG_LOW, "Loading dataset manifest for %s:%lu from %s",
	    dataset->dataset_object.identifier, dataset->dataset_id, path);

  if (access (path, F_OK)) {
    return HIO_ERR_NOT_FOUND;
  }

  if (access (path, R_OK)) {
    return HIO_ERR_PERM;
  }

  xml_doc = xmlParseFile (path);
  if (NULL == xml_doc) {
    hio_err_push (HIO_ERROR, dataset->dataset_context, &dataset->dataset_object,
		  "Could not parse manifest %s", path);
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_1_0 (dataset, xml_doc);
  xmlFreeDoc (xml_doc);

  return rc;
}

int hioi_manifest_read_header (hio_context_t context, hio_dataset_header_t *header, const char *path) {
  xmlDocPtr xml_doc;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Loading dataset manifest header from %s", path);

  if (access (path, F_OK)) {
    return HIO_ERR_NOT_FOUND;
  }

  if (access (path, R_OK)) {
    return HIO_ERR_PERM;
  }

  xml_doc = xmlParseFile (path);
  if (NULL == xml_doc) {
    hio_err_push (HIO_ERROR, context, NULL, "Could not parse manifest %s", path);
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_header_1_0 (context, header, xml_doc);
  xmlFreeDoc (xml_doc);

  return rc;
}

int hioi_element_find_offset (hio_element_t element, uint64_t app_offset0, uint64_t app_offset1,
                              off_t *offset, size_t *length) {
  hio_manifest_segment_t *segment;

  hioi_list_foreach(segment, element->element_segment_list, hio_manifest_segment_t, segment_list) {
    uint64_t base, bound, remaining;

    base = (uint64_t) segment->segment_app_offset0;
    bound = base + segment->segment_length;

    if (app_offset0 >= base && app_offset0 <= bound) {
      *offset = segment->segment_file_offset + (app_offset0 - base);

      remaining = segment->segment_length - (app_offset0 - base);

      if (remaining < *length) {
        *length = remaining;
      }

      return HIO_SUCCESS;
    }
  }

  return HIO_ERR_NOT_FOUND;
}
