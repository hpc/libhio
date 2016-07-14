include ../Makefile.cfg

all:	H5FDhio.o lib hdf5_hio_example

H5FDhio.o: H5FDhio.c
	$(CC) -c $< $(CFLAGS) -I$(HDF5_SRC)

hdf5_hio_example: hdf5_hio_example.c
	$(MPICC) $< -o $@ $(CFLAGS) $(LDFLAGS) -I$(MPI_ROOT)/include -L$(MPI_ROOT)/lib  -lmpi -lhdf5 -lhio -L. -lh5fdhio

lib:
	$(CC) -shared -o libh5fdhio.so H5FDhio.o $(LDFLAGS) -L$(MPI_ROOT)/lib -lhio -lmpi

install:
	install -m 644 libh5fdhio.so $(INSTALL_PREFIX)/lib/
	install -m 644 H5FDhio.h $(INSTALL_PREFIX)/include/

clean:
	rm -rf *.o hdf5_hio_example libh5fdhio.so *~
