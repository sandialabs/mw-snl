//
// MWExecMpi.h
//
// MWExec template instantiated using MWMPIComm objects
//

#ifndef __MWExecMpi_h
#define __MWExecMpi_h

#include "mpi.h"
#include "MWMpiComm.h"
#include "MWExec.h"

class MWMpiComm;


template <class MType, class WType>
class MWExec<MType, WType, MWMpiComm> : public 
		_MWExec_base<MType, WType, MWMpiComm>
{
public:

  /// Constructor.
  MWExec(bool eval_on_master_=false, MPI_Comm comm_=MPI_COMM_WORLD)
		: _MWExec_base<MType, WType, MWMpiComm>(false),
		  comm(comm_) {}

protected:

  /// Initialize the communication layer
  void comm_setup(int& argc, char**& argv, MWMpiComm& )
	{
	int running;
	MPI_Initialized(&running);
	if (!running) {
	   MWprintf( 10, "MPI starting up in MWExec: argc=%d\n",argc);
   	   MPI_Init(&argc,&argv);
	   MWprintf( 10, "MPI started in MWExec: argc=%d\n",argc);
	   comm_initialized_locally = true;
	   }
	else {
	   MWprintf( 10, "MPI is already initialized in MWExec: argc=%d\n",argc);
	   comm_initialized_locally = false;
	   }
	int rank=-1;
	MPI_Comm_rank(comm,&rank);
	set_MWprintf_id(rank);
	if (rank == 0)
	   as_master=true;
	int size=-1;
	MPI_Comm_size(comm,&size);
	if (size == 1)
	   eval_on_master=true;
	}

  /// Cleanup the communication layer
  void comm_cleanup(MWMpiComm& )
	{
	MWprintf( 90, "MWExecMPI::comm_cleanup: local=%d\n",comm_initialized_locally);
	if (comm_initialized_locally)
   	   MPI_Finalize();
	}

  ///
  int bufsize;

  /// The communicator group used by MPI.
  MPI_Comm comm;

  /// If true, then this object initialized MPI
  bool comm_initialized_locally;
};


#endif
