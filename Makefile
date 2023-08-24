include ../../../utilib/Makefile

OPTIMIZATION=-g -I/usr/local/mpich/include -DMWSTATS

OBJS=MWprintf.o MWGroup.o MWRMComm.o MWMpiComm.o MWIndRC.o MWExec.o


cleanup:
	$(RM) *.o

all: $(OBJS)

test:
	$(RM) test.o
	make test.o

nqueens:	all nqueens.o
	$(LINK.cc) -o nqueens nqueens.o $(OBJS)  $(LDLIBS)
