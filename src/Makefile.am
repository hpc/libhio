subdirs=include
all: libhio.la

AUTOMAKE_OPTIONS = subdir-objects

lib_LTLIBRARIES = libhio.la
noinst_LIBRARIES = libconfig_parser.a

libhio_la_CFLAGS = $(AM_CFLAGS) $(XML_CFLAGS)
libhio_la_LDFLAGS = $(LTLDFLAGS) $(XML_LIBS)
libhio_la_LIBADD = libconfig_parser.a
libhio_la_SOURCES = include/context.h include/hio.h context.c \
	include/hio_component.h hio_component.c hio_config.c \
	hio_dataset.c hio_internal.c hio_request.c builtin-posix_component.c \
	hio_manifest.c api/dataset_open.c api/dataset_close.c \
	api/open.c api/close.c api/write.c api/read.c

libconfig_parser_a_CFLAGS = $(AM_CFLAGS)
libconfig_parser_a_LFLAGS = --prefix=hioi_config_parser -o libconfig_parser_a-config_parser.c
libconfig_parser_a_SOURCES = config_parser.l include/config_parser.h
