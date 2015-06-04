LDADD = ../src/libhio.la
AM_CPPFLAGS = -I../src/include
EXTRA_DIST = run_setup run01 run02 run03 run04 run05 run06 run07 run08 run09

clean-local:
	-rm -rf .test_root1

noinst_PROGRAMS = test01.x xexec2.x error_test.x
check_PROGRAMS = ${noinst_PROGRAMS}
TESTS = run01 run02 run03 run05 run05 run06 error_test.x

test01_x_SOURCES = test01.c
xexec2_x_SOURCES = xexec2.c cw_misc.c cw_misc.h

xexec2_x_CFLAGS = -Wall -Werror -DMPI -DHIO -DDLFCN
