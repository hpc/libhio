all: libhio.la

AUTOMAKE_OPTIONS = subdir-objects

lib_LTLIBRARIES = libhio.la
noinst_LIBRARIES = libconfig_parser.a

include_HEADERS = include/hio.h
noinst_HEADERS = include/config_parser.h include/hio_internal.h \
	include/hio_types.h include/hio_var.h include/hio_component.h \
	builtin-posix_component.h

libhio_la_CFLAGS = $(AM_CFLAGS) $(XML_CFLAGS)
libhio_la_LDFLAGS = $(LTLDFLAGS) $(XML_LIBS)
libhio_la_LIBADD = libconfig_parser.a
libhio_la_SOURCES = hio_context.c hio_component.c hio_var.c \
	hio_dataset.c hio_internal.c hio_request.c builtin-posix_component.c \
	hio_manifest.c api/dataset_open.c api/dataset_close.c \
	api/element_open.c api/element_close.c api/element_write.c \
	api/element_read.c api/dataset_unlink.c api/dataset_get_id.c

if DATAWARP_AVAILABLE

libhio_la_SOURCES += builtin-datawarp.c

endif

libconfig_parser_a_CFLAGS = $(AM_CFLAGS) -fPIC
libconfig_parser_a_LFLAGS = --prefix=hioi_config_parser -o libconfig_parser_a-config_parser.c
libconfig_parser_a_SOURCES = config_parser.l include/config_parser.h
