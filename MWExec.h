//
// MWExec.h
//
// TODO: MW batch communication
//

#ifndef __MWExec_h
#define __MWExec_h

#include <csignal>

extern "C" void MWsignal_handler(int sig);


template <class MType, class WType, class CommType>
class _MWExec_base
{
public:

  /// Constructor.
  _MWExec_base(bool as_master_=false) 
		: as_master(as_master_), eval_on_master(false),
		  worker(0), master(0)  {}

  /// Destructor
  virtual ~_MWExec_base()
		{
		if (master) delete master;
		if (worker)  delete worker;
		}

  /// Launch the master-worker process with command-line arguments.
  virtual void go(int argc, char* argv[]);

  /// Launch the master-worker process with no command-line arguments.
  virtual void go()
		{go(0,0);}

protected:

  /**
   * If true, then the driver executes the master, otherwise the worker
   * is executed.
   */
  bool as_master;

  /// If true, the a worker class is executed on the master processor
  bool eval_on_master;

  /// Pointer to a worker class
  WType* worker;

  /// Pointer to a master class
  MType* master;

  /// Initialize the communication layer
  virtual void comm_setup(int&, char**&, CommType& )
	{ MWprintf(10,"No comm setup needed.\n");}

  /// Cleanup the communication layer
  virtual void comm_cleanup(CommType& ) 
	{ MWprintf(10,"No comm cleanup needed.\n");}
};



template <class MType, class WType, class CommType>
void _MWExec_base<MType,WType, CommType>::go(int argc, char* argv[])
{
//
// Setup signal handler
//
signal(SIGSEGV,MWsignal_handler);
signal(SIGHUP,MWsignal_handler);
signal(SIGINT,MWsignal_handler);
signal(SIGQUIT,MWsignal_handler);
signal(SIGILL,MWsignal_handler);
signal(SIGABRT,MWsignal_handler);
signal(SIGBUS,MWsignal_handler);
signal(SIGUSR1,MWsignal_handler);
signal(SIGUSR1,MWsignal_handler);
signal(SIGTERM,MWsignal_handler);
signal(SIGALRM,MWsignal_handler);

//
// Initialize the communication layer (if needed)
//
CommType comm;
comm_setup(argc, argv, comm);

//
// Launch the Master/Worker logic
//
if (as_master) {
   if (!master)
      master = new MType;
   master->setRMC(&comm);
   if (eval_on_master) {
      MWprintf(99,"Creating local worker on master.\n");
      if (!worker)
         worker = new WType(&comm);
      master->set_local_worker(worker);
      }
   master->go(argc,argv);
   }
else {
   if (!worker)
      worker = new WType(&comm);
   worker->go(argc,argv);
   }

//
// Cleanup the communication layer (if needed)
//
comm_cleanup(comm);
}


template <class MType, class WType, class CommType>
class MWExec : public _MWExec_base<MType, WType, CommType>
{
public:

  /// Constructor.
  MWExec(bool as_master_=false) 
		: _MWExec_base<MType, WType, CommType>(as_master_) {}
};

#endif
