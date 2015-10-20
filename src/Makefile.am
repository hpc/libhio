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
	hio_dataset.c hio_internal.c hio_request.c builtin-posix_component.c \
	hio_manifest.c hio_fs.c api/dataset_open.c api/dataset_close.c \
	api/element_open.c api/element_close.c api/element_write.c \
	api/element_read.c api/dataset_unlink.c api/dataset_get_id.c \
	api/element_size.c libconfig_parser_a-config_parser.c

if DATAWARP_AVAILABLE

libhio_la_SOURCES += builtin-datawarp.c

endif
