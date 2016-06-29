# -*- Makefile.am -*-
#
# Copyright (c) 2014-2016 Los Alamos National Security, LLC. All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

all: libhio.la

AUTOMAKE_OPTIONS = subdir-objects
CLEANFILES = libconfig_parser_a-config_parser.c

lib_LTLIBRARIES = libhio.la

include_HEADERS = include/hio.h
noinst_HEADERS = include/config_parser.h include/hio_internal.h \
	include/hio_types.h include/hio_var.h include/hio_component.h \
	builtin-posix_component.h
EXTRA_DIST = config_parser.l

libconfig_parser_a-config_parser.c: config_parser.l
	$(LEX) --prefix=hioi_config_parser -o libconfig_parser_a-config_parser.c config_parser.l

libhio_la_CPPFLAGS = -I$(srcdir)/include
libhio_la_CFLAGS = $(AM_CFLAGS) $(XML_CFLAGS)
libhio_la_LDFLAGS = $(LTLDFLAGS) $(XML_LIBS)
libhio_la_SOURCES = hio_context.c hio_component.c hio_var.c hio_crc.c \
	hio_dataset.c hio_dataset_shared.c hio_element.c hio_internal.c hio_request.c \
	builtin-posix_component.c hio_manifest.c hio_fs.c hio_map.c api/dataset_open.c \
	api/dataset_close.c api/element_open.c api/element_close.c api/element_write.c \
	api/element_read.c api/dataset_unlink.c api/dataset_get_id.c \
	api/element_size.c api/dataset_alloc.c api/object_name.c \
	libconfig_parser_a-config_parser.c
if INTERNAL_JSON_C
# just link in the json objects directly
json_objects = $(top_builddir)/extra/json/build/arraylist.lo $(top_builddir)/extra/json/build/debug.lo \
	$(top_builddir)/extra/json/build/json_c_version.lo $(top_builddir)/extra/json/build/json_object.lo \
	$(top_builddir)/extra/json/build/json_object_iterator.lo $(top_builddir)/extra/json/build/json_tokener.lo \
	$(top_builddir)/extra/json/build/json_util.lo $(top_builddir)/extra/json/build/linkhash.lo \
	$(top_builddir)/extra/json/build/printbuf.lo $(top_builddir)/extra/json/build/random_seed.lo
# include both the archive (for the dynamic library) and the object files (for the static library)
libhio_la_LDFLAGS += --whole-archive $(top_builddir)/extra/json/build/.libs/libjson-c.la --no-whole-archive \
	-version-info 1:0:0
libhio_la_LIBADD = $(json_objects)
libhio_la_CPPFLAGS += -I$(top_builddir)/extra/json/json-c-0.12 -I$(top_builddir)/extra/json/build
$(top_builddir)/extra/json/build/arraylist.lo: json
$(top_builddir)/extra/json/build/debug.lo: json
$(top_builddir)/extra/json/build/json_c_version.lo: json
$(top_builddir)/extra/json/build/json_object.lo: json
$(top_builddir)/extra/json/build/json_object_iterator.lo: json
$(top_builddir)/extra/json/build/json_tokener.lo: json
$(top_builddir)/extra/json/build/json_util.lo: json
$(top_builddir)/extra/json/build/linkhash.lo: json
$(top_builddir)/extra/json/build/printbuf.lo: json
$(top_builddir)/extra/json/build/random_seed.lo: json
$(top_builddir)/extra/json/build/.libs/libjson-c.la: json
.PHONY: json
json:
	make -C $(top_builddir)/extra/json/build
endif

if DATAWARP_AVAILABLE

libhio_la_SOURCES += builtin-datawarp.c

endif
