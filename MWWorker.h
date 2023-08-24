//
// MWWorker.h
//

#ifndef __MWWorker_h
#define __MWWorker_h

#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/resource.h>  /* for getrusage */
#include <sys/time.h>      /* for gettimeofday */

#include "MWprintf.h"
#include "MWTask.h"

template <class CommType>
class MWAbstractDriver;

template <class CommType>
class MWDriver;


/**  This is the worker class that performs tasks for the 
	MWDriver.  This class simply executes the tasks given 
	to it and reports the results back to the master.
	To create an application that derives from this 
	class, the following two methods need to be 
	implemented:

	\begin{itemize}
	\item unpack_init_data()
	\item execute_task()
	\end{itemize}

	@see MWDriver
	@see MWWorker
	@author Mike Yoder, modified by Jeff Linderoth and Jean-Pierre Goux
	*/

template <class CommType>
class MWWorker : public MWBase
{

  friend class MWAbstractDriver<CommType>;
  friend class MWDriver<CommType>;

public:
    
  /// Default constructor
  MWWorker(CommType* RMC);  

  /// Default Destructor
  virtual ~MWWorker()
		{if (task) delete task;}

  /** Start up the worker.
    * Setup the RMC, then call worker_setup, ind_benchmark
    * and worker_mainloop.
    */
  void go( int argc, char *argv[] );

  /** This is run before the worker_mainloop().  It prints a message 
      that the worker has been spawned, sends the master an INIT
      message.
  */
  MWReturn worker_setup(); 

  /// Awaits a reply from the master.
  MWReturn ind_benchmark ( );

  /** This executes that main loop of the worker.  If \c loopflag is 
      \c true this sits in a loop in which it asks for work from the master
      and does the work.  Otherwise, it executes a single iteration
      of the worker's main loop.
  */
  void worker_mainloop (bool loopflag = true);

  /// Our RM / Comm class.  Used here only for communication.
  CommType* RMC;

protected:

  /// The task ID of the master - used for sending messages.
  int master;

  /// The task ID of this worker - used to recognize local workers.
  int myid;

  /// The name of the machine the worker is running on.
  char mach_name[64];

  /// The name of the master machine.
  char master_mach_name[64];

  /** Here we might in the future pack some useful information about
      the specific machine on which we're running.  Right now,
      all workers are equal, and we pass only the hostname.

      There must be a "matching" unpack_worker_initinfo() in
      the MWDriver class.
  */
  virtual void pack_worker_initinfo() {};

  /** This unpacks the initial data that is sent to the worker
      once the master knows that he has started.

      There must be a "matching" pack_worker_init_data() in
      the MWDriver class derived for your application.
  */
  virtual MWReturn unpack_init_data() = 0;

  /**
    If you have some driver data that you would like to use on the
    worker in order to execute the task, you should unpack it here.
  */
  virtual void unpack_driver_task_data( void ) {};
  
  /// Terminate the worker process.
  void terminate();

  /// The task instance that a worker will use for packing/unpacking 
  /// information from the master
  MWTask<CommType> *task;

  /// Run a benchmark, given an TaskType
  virtual double benchmark() { return 0.0; }

  /// Execute the task
  virtual void execute_task(MWTask<CommType>* ) = 0;

  /// Create a control task
  virtual MWTask<CommType>* getControlTask(typename MWTask<CommType>::TaskType tasktype) {return 0;}

  /// Execute a control task
  void execute_control(MWTask<CommType>*) {}
};


template <class CommType>
MWWorker<CommType>::MWWorker(CommType* RMC_) 
  : RMC(RMC_),
    master(0),
    task(0)
{ }


template <class CommType>
void MWWorker<CommType>::go( int argc, char *argv[] ) 
{
MWprintf ( 10, "Worker about to call comm setup\n");
RMC->setup( argc, argv, &myid, &master );
MWprintf ( 10, "Worker %x started.\n", myid );
worker_setup();
ind_benchmark();
worker_mainloop();
}


template <class CommType>
MWBase::MWReturn MWWorker<CommType>::worker_setup()
{
gethostname ( mach_name, 64 );
MWprintf ( 10, "Worker started on machine %s.\n", mach_name );
	
/* Pack and send to the master all these information
	   concerning the host and the worker specificities  */
RMC->initsend();
RMC->pack( mach_name );
pack_worker_initinfo();
	
int status = RMC->send( master, INIT );
MWprintf ( 10, "Worker sent the master %x an INIT message.\n", master );
	
if ( status < 0 ) {
   MWprintf ( 10, "Had a problem sending my name to master.  Exiting.\n");
   RMC->exit(1);
   }

return OK;
}


template <class CommType>
MWBase::MWReturn MWWorker<CommType>::ind_benchmark ( )
{
// wait for the setup info from the master 

int buf_id;
bool status = RMC->recv( master, -1, buf_id );   
if (!status)
   return OK;

if ( buf_id < 0 ) {
   MWprintf ( 10, "Had a problem receiving INIT_REPLY.  Exiting.\n" );
   RMC->exit( INIT_REPLY_FAILURE );
   }

int len, tag, tid;
int tmp = RMC->bufinfo ( buf_id, &len, &tag, &tid );
MWprintf ( 10, "Got Something from the master in reply to INIT %d (status=%d)\n", tag,tmp);

MWReturn ustat = OK;
// unpack initial data to set up the worker state

switch ( tag ) {
  case INIT_REPLY:
	{
	if ( RMC->unpack ( master_mach_name ) != 0 ) {
		int err = -1;
		MWprintf ( 10, "Error unpacking master hostname. \n");
		RMC->initsend ( );
		RMC->pack ( &err, 1 );
		RMC->send ( master, BENCH_RESULTS );
		return ustat;
		}

	if ( (ustat = unpack_init_data()) != OK ) {
		int err = -1;
		MWprintf ( 10, "Error unpacking initial data.\n" );
		RMC->initsend();
		RMC->pack( &err, 1 );
		RMC->send( master, BENCH_RESULTS );
		return ustat;
	}

	int bench_tf = 0;
	RMC->unpack( &bench_tf, 1 );

	if ( bench_tf ) {
		MWprintf ( 10, "Recvd INIT_REPLY, now benchmarking.\n" );
		task->unpack_work();
		double bench_result = benchmark( );
		MWprintf ( 40, "Benchmark completed....%f\n", bench_result );
		int zero = 0;
		RMC->initsend();
		RMC->pack( &zero, 1 );  // zero means that unpack_init_data is OK.
		RMC->pack( &bench_result, 1 );
	} else {
		MWprintf ( 10, "Recvd INIT_REPLY, no benchmark.\n" );
		double z = 0.0;
		int zero = 0;
		RMC->initsend();
		RMC->pack( &zero, 1 );  // zero means that unpack_init_data is OK.
		RMC->pack( &z, 1 );
	}
	MWprintf ( 10, "Worker Sending BENCH_RESULTS\n");
	RMC->send( master, BENCH_RESULTS );

	return ustat;
	break;
	}

  case CHECKSUM_ERROR:
	{
	MWprintf ( 10, "Got a checksum error\n");
	RMC->exit( CHECKSUM_ERROR_EXIT );
	}
  }

return OK;
}


template <class CommType>
void MWWorker<CommType>::worker_mainloop(bool loopflag)
{
bool flag = true;
while (flag == true) {
	//
	// Reset the flag for the next iteration
	//
	flag &= loopflag;

	// wait here for any message from master
	int buf_id;
	bool msg_received = RMC->recv ( master, -1, buf_id );
	if (!msg_received)
           continue;

	if( buf_id < 0 ) {
	  MWprintf( 10, "Could not receive message from master.  Exiting\n" );
	  RMC->exit( buf_id );
	  }

	int len = -2; 
	int tag = -2; 
	int tid = -2;		
	int status = RMC->bufinfo ( buf_id, &len, &tag, &tid );

	switch ( tag ) {
			
	  /* The master has gone down and come back 
	     up, and we have to re-initialize ourself. */
	  case RE_INIT:
		{
		worker_setup();
		ind_benchmark();
		break;
		}

	  case REFRESH:
		{
		unpack_init_data ( );
		break;
		}

	  case DO_THIS_WORK:
		{
		typename MWTask<CommType>::TaskType thisTaskType;
		double wall_time = 0.0;
		double cpu_time = 0.0;
		struct rusage r;
		struct timeval t;
		int tstat;
		MWTask<CommType> *curTask = task;
			
		//
		// Unpack a task request
		//
		int mytemp;
		tstat = RMC->unpack ( &mytemp, 1, 1);
		if ( tstat != 0 ) {
		   MWprintf ( 10, "Error: The receive buffer not unpacked on %d\n", mach_name );
		   RMC->exit ( UNPACK_FAILURE );
		   }

		//
		// Settup the task type
		//
		thisTaskType = (typename MWTask<CommType>::TaskType)mytemp;
		if (thisTaskType != MWTask<CommType>::NORMAL)
		   curTask = getControlTask(thisTaskType);
		curTask->taskType = thisTaskType;

		//
		// Get task number
		//
		int num = -1;
		tstat = RMC->unpack( &num, 1, 1 );
		if( tstat != 0 ) {
		  MWprintf( 10, "Error.  The receive buffer not unpacked on %s\n", mach_name );
		  RMC->exit( UNPACK_FAILURE );
		  }
		curTask->number = num;
		MWprintf( 60, " Worker %s got task number %d\n", mach_name, num );
			
		//
		// Unpack task data
		//
		unpack_driver_task_data();
		curTask->unpack_work();
			
		//
		// Set our stopwatch.
		//
		MWprintf( 60, " Worker %s getting time of day.\n", mach_name);
		gettimeofday ( &t, NULL );
		wall_time -= timeval_to_double( t );
		getrusage ( RUSAGE_SELF, &r );
		cpu_time -= timeval_to_double ( r.ru_utime );
		cpu_time -= timeval_to_double ( r.ru_stime );
		MWprintf( 90, " Worker %s stopwatch=%d\n", mach_name, cpu_time);

		//
		// Execute the task
		//
		if (thisTaskType == MWTask<CommType>::NORMAL) {
		   MWprintf( 60, " Worker %s executing normal task.\n", mach_name);
		   execute_task(curTask);
 		   }
		else {
		   MWprintf( 60, " Worker %s executing nonstandard task.\n", mach_name);
		   curTask->printself ( 10 );
		   execute_control(curTask);
		   curTask->printself ( 10 );
		   }

		//
		// Record the execution time
		//
		MWprintf( 60, " Worker %s computing cpu_time.\n", mach_name);
		gettimeofday ( &t, NULL );
		wall_time += timeval_to_double ( t );
		getrusage ( RUSAGE_SELF, &r );
		cpu_time += timeval_to_double ( r.ru_utime );
		cpu_time += timeval_to_double ( r.ru_stime );
		MWprintf( 90, " Worker %s stopwatch=%d\n", mach_name, cpu_time);

		//
		// Send the results to the master
		//
		MWprintf( 40, " Worker %s sending results to master: num=%d wall=%f cpu=%f\n", mach_name,num,wall_time,cpu_time);
		RMC->initsend();
		RMC->pack( &num );
		RMC->pack( &wall_time );
		RMC->pack( &cpu_time );
		curTask->pack_results();
		status = RMC->send(master, RESULTS);
		if ( status < 0 ){
		   MWprintf( 10, "ERROR: Could not send results of task %d\n", num ); 
		   MWprintf( 10, "Exiting worker!" ); 
		   RMC->exit( FAIL_MASTER_SEND );
		   }
		MWprintf ( 40, "%s sent results of job %d.\n", 
					   mach_name, curTask->number );

		//
		// Cleanup
		//
		if (thisTaskType != MWTask<CommType>::NORMAL)
		   delete curTask;
		}
		break;
		
	  case KILL_YOURSELF:
		terminate();
		flag=false;
		break;

	  case CHECKSUM_ERROR:
		{
		MWprintf ( 10, "Got a checksum error\n");
		RMC->exit( CHECKSUM_ERROR_EXIT );
		break;
		}

	  default:
		{
		MWprintf ( 10, "Worker received strange command %d.\n", tag );
		RMC->exit( UNKNOWN_COMMAND );
		break;
		}
	  }
	}

MWprintf ( 90, "%s ending worker_mainloop.\n", mach_name );
}


template <class CommType>
void MWWorker<CommType>::terminate() 
{   
MWprintf ( 10, "%s is terminating.\n", mach_name );
RMC->exit(0);
}
#endif
