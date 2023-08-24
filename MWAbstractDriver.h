//
// MWAbstractDriver.h
//
// An abstract definition of a Master-Worker class.  This class is needed
// to avoid circular dependencies between the MWTask, MWWorker and MWDriver
// classes.  The possible circular dependency was created when we moved 
// some of the 'global' data out of the old MWDriver.C file.
//
// Although we could _also_ create work a work-around whereby the MWTask and
// MWWorker objects reference each other through the MWDriver, that is
// rather awkward (and probably not very efficient).  Further, this provides a
// nice place to define the API.
//
// This class _does_ contain some data, but it does not include code that 
// requires methods/data from MWWorkerID or MWTask objects.
//

#ifndef __MWAbstractDriver_h
#define __MWAbstractDriver_h

#include <vector>
#include <iostream>
#include <unistd.h>
#include <assert.h>
using namespace std;
#include "MW.h"

//
// Forward declaration
//
template <class CommType>
class MWTask;

template <class CommType>
class MWWorkerID;

template <class CommType>
class MWWorker;

/// This is if you wish to have XML output.
//#define XML_OUTPUT


/** This provides an API for a master-worke class that is robust in an
	opportunistic environment.  The goal is to be completely fault -
	tolerant, dealing with all possiblities of host (worker) problems.
	To do this, the MWDriver class manages a set of tasks and a
	set of workers.  It monitors messages about hosts coming up and
	going down, and assigns tasks appropriately.

	This class is built upon some sort of resource management and
	message passing lower layer.  Previously, it was built directly
	on top of Condor - PVM, but the interface to that has been
	abstracted away so that it can use any facility that provides
	for resource management and message passing.  See the abstract
	MWRMComm class for details of this lower layer.  When interfacing
	with this level, you'll have use the RMC object that's a static
	member of the MWDriver, MWTask, and MWWorker class.

	To implement an application, a user must derive a class from
	this base class and implement the following methods:

	\begin{itemize}
	\item get_userinfo()
	\item setup_initial_tasks()
	\item setup_additional_tasks()
	\item pack_worker_init_data()
	\item act_on_completed_task()
	\end{itemize}

	For a higher level of control regarding the distribution of tasks to
	workers, the following methods have to be implemented:
	\begin{itemize}
	\item set_workClasses()			// TODO - check
	\item setWorkGroup()
	\end{itemize}

	Similar application dependent methods must be implemented
	for the "Task" of work to be done and the "Worker" who performs
	the tasks.
    
	@see MWTask
	@see MWWorker
	@see MWRMComm
	@author Mike Yoder, Jeff Linderoth, Jean-Pierre Goux, Sanjeev Kulkarni
*/

template <class CommType>
class MWAbstractDriver : public MWBase {

  friend class MWTask<CommType>;
  friend class MWWorkerID<CommType>;

public:

  /** @name Type definitions.  */
  //@{
  /** The ways in which tasks may be added to the list.
    */
  enum MWTaskAdditionMode {
    /// Tasks will be added at the end of the list
    ADD_AT_END,
    /// Tasks will be added at the beginning
    ADD_AT_BEGIN,
#ifdef MW_SORTED_WORKERS
    /// Tasks will be added based on their key (low keys before high keys)
    ADD_BY_KEY
#endif
    };

  /** The suspension policy to use - What do we do when it happens?
	*/
  enum MWSuspensionPolicy {
    /// Normally do nothing unless there are idle workers.
    DEFAULT,
    /// Always reassign the task; move it to the front of the todo list
    REASSIGN 
    };

  /** Tasks are always assigned to he first "IDLE" machine.  By ordering
      the machine list, we can implement a number of scheduling policies.
      Insert your own favorite policy here.
    */
  enum MWMachineOrderingPolicy {
    /// The machines are ordered simply as they become available
    NO_ORDER,
    /** Machines are ordered by the "benchmark" result.  Larger benchmark 
	results go first, so if using time to complete a task as the benchmark,
	you should return 1/time as the benchmark value.
      */
    BY_USER_BENCHMARK,
    /// Machines are ordered by reported KFLOPS.
    BY_KFLOPS,
    /** User-defined policy.
	If the user sets this policy, then the WorkerIDLess class must be
	initialized with a function pointer that defines the key. */
    USER_ORDERING_POLICY,
    };
  //@}


  /** @name Main Routines */
  //@{
  /// Default constructor
  MWAbstractDriver();
	
  /** Destructor - walks through lists of tasks & workers and
	deletes them. */	
  virtual ~MWAbstractDriver();

  /** This method runs the entire application.  What it *really* does
	is call setup_master(), then master(), then printresults(), 
	and then ends.  See the other functions	for details. 
	*/
  virtual void go( int argc, char *argv[] );
	
  /** This version of go simply calls go(0, NULL).*/
  virtual void go()
		{ go ( 0, NULL ); };
  
  /** Prints the results. Applications may re-implement this
	to print their application specific results.
	*/
  virtual void printResults();

  ///
  CommType* getRMC()
		{return RMC;}

  ///
  void setRMC(CommType* RMC_)
		{ RMC = RMC_; }
  //@}


  /** @name Worker Management */
  //@{
  /// Returns the number of workers
  virtual int numWorkers() const = 0;
	
  /// Returns the number of active workers
  virtual int numActiveWorkers() const = 0;

  /// Counts the number of workers in the given arch class
  virtual int numWorkers( int arch ) const = 0;

  /// Counts the number of workers in the given state
  virtual int numWorkersInState( MWworker_states ThisState ) const = 0;

  /// Prints the available workers
  virtual void printWorkers() const = 0;

  /**
   *  Set a worker that runs on the local process.
   *  The master communicates with this worker only _after_ all of the
   *  other workers' messages have been handled.
   */
  virtual void set_local_worker(MWWorker<CommType>* worker)
		{local_worker=worker;}
  //@}


  /** @name Task Management */
  //@{
  /** If true, then continue looking for additional tasks even after all
    * current tasks are finished.
    */
  bool server_mode;

  /// Returns the number of tasks on the todo list.
  virtual int numTasksTodo() = 0;

  /// Returns the number of running tasks.
  virtual int numTasksRunning() = 0;

  /// Sets the function that MWAbstractDriver users to get the "key" for a task
  void set_task_key_function( MWKey (*keyFunc)( MWTask<CommType> * ) )
		{TaskLess.keyFunc = keyFunc;}

  /** This deletes all tasks in the todo list with a key greater than 
      the one specified */
  virtual int delete_tasks_greater_than( MWKey ) = 0;

  /// (Mostly for debugging) -- Prints the task keys in the todo list
  virtual int print_task_keys( void ) = 0;

  /** Register the task that will be sent to each worker upon startup.
	This way, the user knows which machines are fastest, and MW can perform
	automatic "normalization" of the equivalent CPU time.
	If no task is specified, then a default benchmark task is 
	created by allocate_benchmark_task. This task will be deleted by
	MWDriver.
	*/
  void register_benchmark_task( MWTask<CommType> *t=0 )
		{
		if (t)
		   bench_task = t;
		else
		   bench_task = allocate_benchmark_task();
		}

  /// Returns the value (only) of the best key in the Todo list 
  virtual MWKey min_todo_keyval() = 0;

  /// Returns the best value (only) of the best key in the Running list.
  virtual MWKey min_running_keyval( )   = 0;

  /// Sets the insertion mode for tasks
  void set_addmode(MWTaskAdditionMode amode)
		{addmode = amode;}
  //@}


  /** @name Worker Policy Management. (PUBLIC) */
  //@{
  /**
   * Set the policy to use when suspending.
   * The default behavior is to ignore tasks on suspended workers until
   * the todo list is empty.
   */
  void set_suspension_policy( MWSuspensionPolicy policy )
		{suspensionPolicy = policy;}

  /**
   * Sets the machine ordering policy.
   * The default behavior is to select machines in an arbitrary order.
   */
  void set_machine_ordering_policy(
			MWMachineOrderingPolicy mode,
  			MWKey (*wkey)( const MWWorkerID<CommType> * ) = 0 );

  /** Sets the timeout_limit and turn worker_timeout to 1 */
  void set_worker_timeout_limit(double timeout_limit, int timeout_frequency);
  //@}


  /** @name Work Group Management
    *
    * These functions manage the definition of the work classes that
    * workers can solve.  Each worker belongs to one or more class, and
    * each task can be solved by one or more classes of workers.
    */
  //@{
  /// Set up workGroups
  virtual void setupWorkGroups( int num );

  /// Get the number of work groups
  int numWorkGroups() const
		{return workGroupTaskNum.size();}

  /// Returns the number of workers in the given group.
  int numWorkersInGroup(int groupNum) const
		{return workGroupWorkerNum[groupNum];}

  /// Returns the number of tasks that can be solved by workers in a group.
  int numTasksInGroup( int groupNum ) const
		{return workGroupTaskNum[groupNum];}
  //@}

	
  /** @name Debugging flags

      These flags are useful for debugging the code (e.g. 
      simulating slow or broken processors.

  */
  /// Slow down the master's main loop by this many seconds
  static int master_slowdown;

  /** @name Checkpoint Management
			
      These are logical checkpoint handling functions.  They are
      virtual, and are *entirely* application-specific.  In them, the
      user must save the "state" of the application to permanent
      storage (disk).  To do this, you need to:
			
      \begin{itemize}
      \item Implement the methods write_master_state() and
      read_master_state() in your derived MWDriver app.
      \item Implement the methods write_ckpt_info() and 
      read_ckpt_info() in your derived MWTask class.
      \end{itemize}
			
      Then MWDriver does the rest for you.  When checkpoint() is
      called (see below) it opens up a known filename for writing.
      It passes the file pointer of that file to write_master_state(), 
      which dumps the "state" of the master to that fp.  Here 
      "sate" includes all the variables, info, etc of YOUR
      CLASS THAT WAS DERIVED FROM MWDRIVER.  All state in
      MWDriver.C is taken care of (there's not much).  Next, 
      checkpoint will walk down the running queue and the todo
      queue and call each member's write_ckpt_info().  
      
      Upon restart, MWDriver will detect the presence of the 
      checkpoint file and restart from it.  It calls 
      read_master_state(), which is the inverse of 
      write_master_state().  Then, for each task in the 
      checkpoint file, it creates a new MWTask, calls 
      read_ckpt_info() on it, and adds it to the todo queue.
			
      We start from there and proceed as normal.
			
      One can set the "frequency" that checkpoint files will be 
      written (using set_checkpoint_frequency()).  The default
      frequency is zero - no checkpointing.  When the frequency is
      set to n, every nth time that act_on_completed_task gets 
      called, we checkpoint immediately afterwards.  If your
      application involves "work steps", you probably will want to 
      leave the frequency at zero and call checkpoint yourself
      at the end of a work step.
  */
  //@{
  /** This function writes the current state of the job to disk.  
      See the section header to see how it does this.
      @see MWTask
  */
  virtual void checkpoint() {}
	
  /** This function checks to see whether checkpointing information is 
      available. By default, this returns false, and checkpoint-base
      restarts are not attempted. */
  virtual bool checkpoint_data_exists() {return false;}

  /** This function does the inverse of checkpoint.  
      It opens the checkpoint file, calls read_master_state(), 
      then, for each task class in the file, creates a MWTask, 
      calls read_ckpt_info on it, and adds that class to the
      todo list. */
  virtual void restart_from_ckpt() {}
	
  /// Cleanup the checkpointing files after a successful termination.
  virtual void checkpoint_end() {}

  /** This function sets the frequency with with checkpoints are
      done.  It returns the former frequency value.  The default
      frequency is zero (no checkpoints).  If the frequency is n, 
      then a checkpoint will occur after the nth call to 
      act_on_completed_task().  A good place to set this is in
      get_userinfo().
      @param freq The frequency to set checkpoints to.
      @return The former frequency value.
  */
  int set_checkpoint_frequency( int freq );
	
  /** Set a time-based frequency for checkpoints.  The time units
      are in seconds.  A value of 0 "turns off" time-based 
      checkpointing.  Time-based checkpointing cannot be "turned 
      on" unless the checkpoint_frequency is set to 0.  A good
      place to do this is in get_userinfo().
      @param secs Checkpoint every "secs" seconds
      @return The former time frequency value.
  */
  int set_checkpoint_time( int secs );
	
  /** Here you write out all 'state' of the driver to an output
      stream. */
  virtual void write_master_state(ostream& os) {}

  /** Here, you read in the 'state' of the driver from an input
      stream. */
  virtual void read_master_state(istream& is ) {}
  //@}


#ifdef XML_OUTPUT
  /** @name XML I/O Management */
  //@{
  ///
  void  write_XML_status() = 0;

  /**
     If you want to display information about status of some
     results variables of your solver, you have to dump a string in
     ASCII, HTML or XML format out of the following method. 
     The iMW interface will be in charge of displaying this information
     on the user's browser.
   */
  virtual char* get_XML_results_status(void );
  //@}
#endif


protected:
	

  /** @name Pure Virtual Methods
	These are the methods from the MWDriver class that a user 
	{\bf must} reimplement to create an application. 
   */
  //@{
  /** This function is called to read in all information
	specific to a user's application and do any initialization on
	this information.
	*/
  virtual MWReturn get_userinfo( int argc, char *argv[] )=0;

  /** This function must return a number n > 0 of pointers
	to Tasks to "jump start" the application.
	The MWTasks pointed to should be of the task type derived
	for your application
    */
  virtual MWReturn setup_initial_tasks( vector<MWTask<CommType>*>& task ) = 0;
	
  /** This function returns n >= 0 additional tasks that are inserted into
	the task list.  This is a handy function to derive when using MW
	to control a server.
	The MWTasks pointed to should be of the task type derived
	for your application
    */
  virtual MWReturn setup_additional_tasks( vector<MWTask<CommType>*>& task )
		{return OK;}
	
  /** This function performs actions that happen
	once the Driver receives notification of a completed task.  
	*/
  virtual MWReturn act_on_completed_task(MWTask<CommType>*) = 0;

  /** This function should be implemented by the application
	to assign the workGroup number to the worker if it is doing
	intelligent work scheduling.  The default behavior is to set
	everyone to the same work group.
	*/
  virtual void setWorkGroup( MWWorkerID<CommType> *w )
		{w->addGroup(0);}

  /** A common theme of Master-Worker applications is that there is 
	a base amount of "initial" data defining the problem, and then 
	just incremental data defining "Tasks" to be done by the Workers.

	This one packs all the user's initial data.  It is unpacked 
	in the worker class, in unpack_init_data().
	*/
  virtual MWReturn pack_worker_init_data( void ) = 0;
	
  /** This one unpacks the "initial" information sent to the driver
	once the worker initializes. 
     
	Potential "initial" information that might be useful is...
	\begin{itemize}
	\item Information on the worker characteristics  etc...
	\item Information on the bandwith between MWDriver and worker
	\end{itemize}
		   
	These sorts of things could be useful in building some 
	scheduling intelligence into the driver.
	*/
  virtual void unpack_worker_initinfo( MWWorkerID<CommType> *w ) {};
	
  /** OK, This one is not pure virtual either, but if you have some 
	"driver" data that is conceptually part of the task and you wish
	not to replicate the data in each task, you can pack it in a
	message buffer by implementing this function.  If you do this, 
	you must implement a matching unpack_worker_task_data()
	function.
	*/
  virtual void pack_driver_task_data( void ) {};

  /** Create a MWWorkerID class.
	This defaults to constructing a MWWorkerID class, but we add this
	method since derived applications may wish to allocate derived
	classes of MWWorkerID.  This design seems simpler than adding a 
	template argument for MWDriver that defines the worker ID class.
	*/
  virtual MWWorkerID<CommType>* allocate_workerID()
		{return new MWWorkerID<CommType>(this);}

  /// Create a MWTask task object.
  virtual MWTask<CommType>* allocate_task() = 0;

  /// Allocate the 'default' benchmark task.
  virtual MWTask<CommType>* allocate_benchmark_task()
		{ return NULL; }
  //@}
	
	
  /** @name Protected Main Routines */
  //@{
  /** This method is called before the main loop in go() is executed.
      It does some setup, including calling the get_userinfo() and
      create_initial_tasks() methods.  It then figures out how
      many machines it has and starts worker processes on them.
      @param argc The argc from the command line
      @param argv The argv from the command line
      @return This is the from the user's get_userinfo() routine.
      If get_userinfo() returns OK, then the return value is from
      the user's setup_initial_tasks() function.
  */
  virtual MWReturn master_setup( int argc, char *argv[] );

  /// Collect statistics about the execution
  virtual void collect_statistics() {}

  /** Grab the next task off the todo list, make and send a work
      message, and send it to a worker.  That worker is marked as 
      "working" and has its runningtask pointer set to that task.  The
      worker pointer in the task is set to that worker.  The task
      is then placed on the running queue. This returns true if
      a task was assigned to this worker.
  */
  virtual bool send_task_to_worker( MWWorkerID<CommType> *w ) = 0;
	
  /**  After each result message is processed, we try to match up
       tasks with workers.  (New tasks might have been added to the list
       during processing of a message).  Don't send a task to
       "nosend", since he just reported in.
  */
  virtual void rematch_tasks_to_workers( MWWorkerID<CommType> *nosend ) = 0;
	
  /** A wrapper around the lower level's hostaddlogic.  Handles
      things like counting machines and deleting surplus */
  virtual void call_hostaddlogic() = 0;

  /** Our Resource Management / Communication class.  It's a member of this 
	class because that way derived classes can use it easily. */
  CommType* RMC;
  //@}


  /** @name Protected MainLoop Routines

     In the case that the user wants to take specific actions
     when notified of processors going away, these methods
     may be reimplemented.  Care must be taken when
     reimplementing these, or else things may get messed up.
     
     Probably a better solution in the long run is to provide 
     users hooks into these functions or something. 
		   
     Basic default functionality that updates the known
     status of our virtual machine is provided. 
  */
  //@{
  /** This is the main controlling routine of the master.  It provides a
      big switch statement that calls routines to deal with a single 
      message.  The flag \c busy returns true if this routine did 
      real work when it was called.
    */
  virtual MWReturn master_handle_msg(bool& busy);

  /** Act on a "completed task" message from a worker.
      Calls pure virtual function {\tt act_on_completed_task()}.
      @return Is from the return value of {\tt act_on_completed_task()}.
  */
  virtual MWReturn handle_worker_results( MWWorkerID<CommType> *w ) = 0;
	
  /** Here, we get back the benchmarking
      results, which tell us something about the worker we've got.
      Also, we could get some sort of error back from the worker
      at this stage, in which case we remove it. */
  virtual MWReturn handle_benchmark( MWWorkerID<CommType> *w ) = 0;

  /** This is what gets called when a host goes away.  We figure out
      who died, remove that worker from our records, remove its task
      from the running queue (if it was running one) and put that
      task back on the todo list. */
  virtual void handle_hostdel() = 0;
	
  /** Implements a suspension policy.  Currently either DEFAULT or
      REASSIGN, depending on how suspensionPolicy is set. */
  virtual void handle_hostsuspend() = 0;
	
  /** Here's where you go when a host gets resumed.  Usually, 
      you do nothing...but it's nice to know...*/
  virtual void handle_hostresume() = 0;
  
  /** We do basically the same thing as handle_hostdel().  One might 
      {\em think} that we could restart something on that host; 
      in practice, however -- especially with the Condor-PVM RMComm
      implementation -- it means that the host has gone down, too.
      We put that host's task back on the todo list.
  */
  virtual void handle_taskexit() = 0;

  /** Routine to handle when the communication layer says that a
      checksum error happened. If the underlying Communitor
      gives a reliably reliable communication then this messge
      need not be generated. But for some Communicators like
      MW-File we may need some thing like this.
  */
  virtual void handle_checksum () = 0;
  //@}


  /** @name Protected Task Management

      MW provides a mechanism for performing tasks on workers that are 
      potentially "lost".  If the RMComm fails to notify MW of a worker
      going away in a timely fashion, the state of the computing platform
      and MW's vision of its state may become out of synch.  In order to
      make sure that all tasks are done in a timely fashion, the user may set
      a time limit after which a task running on a "lost" worker 
      may be rescheduled.
   */
  //@{
  /// Add a list of tasks
  virtual void addTasks(vector<MWTask<CommType>*>& task_list) = 0;

  /// Go through the list of timed out WORKING workers and reschedule tasks
  virtual void reassign_tasks_timedout_workers(bool& busy) = 0;

  /** If false : workers never timeout and can potentially work forever on a task
      If true : workers time out after worker_timeout_limit seconds */	
  bool worker_timeout;

  /** Limit of seconds after which workers are considered time out and 
      tasks are re-assigned */
  double worker_timeout_limit;

  /** Frequency at which we check if there are timed out workers */
  int worker_timeout_check_frequency;

  /** Based on the time out frequency, next timeout check time*/
  int next_worker_timeout_check;

  /** A function class that compares two MWTask pointers based upon a 
      function pointer that defines the 'key' for comparison.
   */
  MWKeyFuncLess<MWTask<CommType>* > TaskLess;


  /// Defines where tasks should be added in the task list
  MWTaskAdditionMode addmode;
  //@}
  

  /** @name Protected Worker Management
		   
     These methods act on the list of workers (or specifically) ID's of 
     workers, that the driver knows about.
  */
  //@{
  /// Adds a worker to the list of avaiable workers
  virtual void worker_add ( MWWorkerID<CommType> *w ) = 0;

  /** Unpacks the initial worker information, and sends the
      application startup information (by calling pure virtual
      {\tt pack_worker_init_data()}

      The return value is taken as the return value from the user's
      {\tt pack_worker_init_data()} function.
  */
  virtual MWReturn worker_init( MWWorkerID<CommType> *w ) = 0;

  /// Terminate all the workers
  virtual void terminate_workers() = 0;
	
  /// Looks up information about a worker given its task ID
  virtual MWWorkerID<CommType> * worker_find( int tid, bool erase_flag=false ) = 0;
	
  /// This function removes worker from the list and deletes it.
  virtual void worker_terminate( MWWorkerID<CommType> *w ) = 0;

  /// Returns true if a worker is marked fror removal
  virtual bool worker_marked_for_removal( MWWorkerID<CommType>* w) = 0;

  /** A function class that compares two WorkerID pointers based upon a 
      function pointer that defines the 'key' for comparison.
   */
  MWKeyFuncLess<MWWorkerID<CommType>* > WorkerIDLess;

  /// A key function for benchmark data
  static MWKey compareBenchmark(MWWorkerID<CommType>* );

  /// A key function for kflops data
  static MWKey compareKFLOPS(MWWorkerID<CommType>* );

  /// Specifies how machines/workers are ordered
  MWMachineOrderingPolicy machine_ordering_policy;

  /// Defines what should happen on a suspension
  MWSuspensionPolicy suspensionPolicy;

  /// The pointer to a local worker
  MWWorker<CommType>* local_worker;
  //@}
	

  /** @name Protected Checkpointing Management */
  //@{
  /// If true, then checkpointing is performed.  Default value is false.
  bool perform_checkpoint;

  /** How often to checkpoint?  Task frequency based. */
  int checkpoint_frequency;
	
  /** How often to checkpoint?  Time based. */
  int checkpoint_time_freq;

  /** Time to do next checkpoint...valid when using time-based 
      checkpointing. */
  long next_ckpt_time;
	
  /** The number of tasks acted upon up to now.  Used with 
      checkpoint_frequency */
  int num_completed_tasks;
	
  /** The benchmark task. */
  MWTask<CommType> *bench_task;
  //@}


  /** @name Protected Work Group Management */
  //@{
  /// Counts the number of tasks that can be solved by each class of workers.
  vector< int > workGroupTaskNum;

  /// Counts the number of workers in each class.
  vector< int > workGroupWorkerNum;
  //@}

};



////--------------------------------------------------------
////
//// Main Routines
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
MWAbstractDriver<CommType>::MWAbstractDriver()
 : 
   server_mode(false),
   worker_timeout(false),
   worker_timeout_limit(0.0),
   worker_timeout_check_frequency(0),
   next_worker_timeout_check(0),

   machine_ordering_policy(NO_ORDER),
   suspensionPolicy(DEFAULT),
   local_worker(0),

   perform_checkpoint(false),
   checkpoint_frequency(0),		// No checkpointing
   checkpoint_time_freq(0),
   next_ckpt_time(0),
   bench_task(0)
{
workGroupWorkerNum.resize(1);
workGroupTaskNum.resize(1);
}


//============================================================================
//
//
template <class CommType>
MWAbstractDriver<CommType>::~MWAbstractDriver()
{
if ( bench_task )
   delete bench_task;
}


//============================================================================
//
//
template <class CommType>
void MWAbstractDriver<CommType>::go(int argc, char* argv[])
{
//
// Setup the master
//
if (master_setup(argc,argv) != OK)
   return;

if (!server_mode && (numWorkers() == 0) && !local_worker) {
   MWprintf(1,"No workers and no local worker\nMaster terminating\n");
   RMC->exit(0);
   }

//
// Startup the main processing loop:
//
MWReturn ustat = OK;
vector<MWTask<CommType>*> new_tasks;
#ifdef USING_MPI
int num_remote_workers= numWorkers() - (local_worker ? 1 : 0);
#endif
while ( (server_mode + numTasksTodo() + numTasksRunning() > 0) && 
        (ustat == OK) ) {
  bool busy=false;
#ifdef USING_MPI
  if (num_remote_workers > 0)
#endif
     ustat = master_handle_msg (busy);
  if (!busy && local_worker)
     local_worker->worker_mainloop(false);
  //
  // A useful debugging aid ... slow down the master
  //
  if (master_slowdown > 0) {
     MWprintf(10,"Sleeping for %d seconds\n",master_slowdown);
     sleep(master_slowdown);
     MWprintf(10,"Waking up\n");
     }
  //
  // Look for additional tasks
  //
  if (server_mode) {
     if (numTasksTodo() + numTasksRunning() == 0)
        sleep(1);
     new_tasks.resize(0);
     MWReturn retval = setup_additional_tasks( new_tasks );
     if ( retval == OK )
        if (new_tasks.size() > 0)
           addTasks( new_tasks );
     else {
        MWprintf( 10, "setup_additional_tasks() returned %d\n", retval );
        RMC->exit(1);
        }
     }
  }
if ( ustat != OK )
   MWprintf( 10, "The user signaled %d to stop execution.\n", ustat );

//
// If we're done, then print the results.
//
if ( ustat == OK || ustat == QUIT ) {
   collect_statistics();
   printResults();
   if (perform_checkpoint)
      checkpoint_end();
   }

//
// Remove the workers
//
terminate_workers();

// Does not return.
MWprintf ( 10, "The MWAbstractDriver is done.\n" );
RMC->exit(0);
}


//============================================================================
//
//
template <class CommType>
MWBase::MWReturn MWAbstractDriver<CommType>::master_setup(int argc, char* argv[])
{
//
// Some diagnostics
//
MWprintf ( 70, "MWDriver is pid %ld.\n", getpid() );
char wd[_POSIX_PATH_MAX];
if ( getcwd( wd, 100 ) == NULL )
   MWprintf ( 10, "getcwd failed!  errno %d.\n", errno );
MWprintf ( 70, "Working directory is %s.\n", wd );

//
// Setup the RMC
//
int myid, master_id;
RMC->setup( argc, argv, &myid, &master_id );

//
// Either restart from checkpoint data or startup from scratch
//
int nworkers;
if (perform_checkpoint && checkpoint_data_exists()) {
   //
   // Restart from a checkpoint file
   //
   MWprintf ( 50, "Starting from a checkpoint file.\n" );
   restart_from_ckpt();
   nworkers = RMC->restart_workers();
   }
else {
   //
   // Startup from scratch
   //
   MWprintf ( 50, "Starting from the beginning.\n" );
   MWprintf ( 50, "argc=%d, argv[0]=%s\n", argc, argv[0] );

   //
   // Setup user data
   //
   MWReturn ustat = OK;
   ustat = get_userinfo( argc, argv );
   if ( ustat != OK ) {
      RMC->exit(1);
      return ustat;
      }
   if ( RMC->get_num_exec_classes() == 0 )
      RMC->set_num_exec_classes(1);
   if ( RMC->get_num_arch_classes() == 0 ) {
      RMC->set_num_arch_classes(1);
      RMC->set_num_executables(1);
      }

   //
   // Setup the initial tasks
   //
   vector<MWTask<CommType>*> orig_tasks;
   ustat = setup_initial_tasks( orig_tasks );
   if ( ustat == OK )
      addTasks( orig_tasks );
   else {
      MWprintf( 10, "setup_initial_tasks() returned %d\n", ustat );
      RMC->exit(1);
      return ustat;
      }

   //
   // Initialize the workers
   //
   nworkers = RMC->init_workers();
   if (local_worker) {
      local_worker->worker_setup();
      local_worker->ind_benchmark();
      }
   }
//
// Allocate memory for workers
//
MWprintf( 20, "Initializing %d workers\n",nworkers);
for (int i=0; i<nworkers; i++) {
  MWWorkerID<CommType>* w = allocate_workerID();
  RMC->config_worker(w);
  worker_add(w);
  }
if (local_worker) {
   MWprintf( 20, "Initializing local worker\n");
   MWWorkerID<CommType>* w = allocate_workerID();
   RMC->config_worker(w);
   local_worker->master = master_id;
   local_worker->myid = w->get_id1();
   worker_add(w);
   }

//
// Try to get as many hosts as we have workers setup
//
call_hostaddlogic();

return OK;
}


//============================================================================
//
//
template <class CommType>
void MWAbstractDriver<CommType>::printResults ( )
{
int bytes_packed;
int bytes_unpacked;
RMC->get_packinfo(bytes_packed, bytes_unpacked);

MWprintf ( 10, "In MWAbstractDriver packed %d and unpacked %d\n", 
				bytes_packed, bytes_unpacked );
}



////--------------------------------------------------------
////
//// Main Routines
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
MWBase::MWReturn MWAbstractDriver<CommType>::master_handle_msg(bool& busy) 
{
busy = false;

MWWorkerID<CommType> *w = NULL;
MWReturn ustat = OK;
	
if ( numTasksTodo() > 0)
   MWprintf( 99, " CONTINUE -- todo list has %d unfinished tasks.\n",numTasksTodo() ); 

else if ( numActiveWorkers() > 0)
   MWprintf( 60, " CONTINUE -- running list has %d active workers.\n", numActiveWorkers() ); 

//
// Get any message from anybody
//
int buf_id;
bool status = RMC->recv( -1, -1, buf_id );
MWprintf( 90, " RMC->recv results: status=%d buf_id=%d\n",status,buf_id);
if (status) {
   busy = true;
   if ( buf_id < 0 ) {
      MWprintf ( 10, "ERROR, Problem with RMC->recv!\n" );
      return QUIT;
      }
   int info, len, tag, sending_host;
   info = RMC->bufinfo ( buf_id, &len, &tag, &sending_host );


   //
   // Process message
   //
   switch ( tag ) {
			
     //
     // A request from a worker to initialize itself
     //
     case INIT: 
	if ( (w = worker_find ( sending_host )) == NULL ) {
	   MWprintf ( 10, "We got an INIT message from a worker (%08x) "
			   "who we don't have records for.\n", sending_host);
	   RMC->initsend ( );
	   RMC->send ( sending_host, KILL_YOURSELF );
	   break;
	   }
	ustat = worker_init( w );
	break;

     case BENCH_RESULTS:
	if ( (w = worker_find ( sending_host )) == NULL ) {
	   MWprintf ( 10, "We got an BENCH_RESULTS message from (%08x), "
			   "who we don't have records for.\n", sending_host);
	   RMC->initsend ( );
	   RMC->send ( sending_host, KILL_YOURSELF );
	   break;
	   }
	ustat = handle_benchmark( w );

	if ( ustat == OK ) {
	   printWorkers();
	   send_task_to_worker( w );
	   } 
	else 
	   //
	   // Ignore benchmarking errors ... we don't want to shutdown
	   //
	   ustat = OK;
	break;

     case RESULTS:
	MWprintf ( 10, "We got a RESULTS message from a worker (%08x).\n", 
			sending_host);
	if ( (w = worker_find ( sending_host )) == NULL ) {
	   MWprintf ( 10, "We got a RESULTS message from a worker (%08x) "
			   "who we don't have records for.\n", sending_host);
	   RMC->initsend ( );
	   RMC->send ( sending_host, KILL_YOURSELF );
	   break;
	   }
	ustat = handle_worker_results ( w );

	if ( worker_marked_for_removal(w) )
	   worker_terminate ( w );
	else {
	   if ( ustat == OK ) 
	      send_task_to_worker( w );
	   else {
	      //
	      // Worker returned an error, so terminate the worker
	      //
	      RMC->initsend ( );
	      RMC->send ( sending_host, KILL_YOURSELF );
	      worker_terminate ( w );
	      call_hostaddlogic ( );
	      }
	   }
	break;

     case HOSTADD:
	{
	MWWorkerID<CommType> *w = allocate_workerID();

	int r=0;
	r = RMC->start_worker( w );
	if ( r >= 0 )
	   worker_add( w );
	else
	   delete w;
			
	call_hostaddlogic();
	break;
	}

     case HOSTDELETE:
	handle_hostdel();
	break;

     case TASKEXIT:
	handle_taskexit();
	break;

     case CHECKSUM_ERROR:
	handle_checksum();
	break;

     case HOSTSUSPEND:
	handle_hostsuspend();
	break;

     case HOSTRESUME:
	handle_hostresume();
	break;

     default:
	MWprintf ( 10, "MWAbstractDriver<CommType>::master_handle_msg -- Unknown message %d.  What the heck?\n", tag );
	abort();

     };
  //
  // Do some assigning of tasks.  If many tasks are added all in a batch, 
  // then many "SEND_WORK" commands are ignored, and the workers sit
  // idle.  Checking again here whether or not there are workers
  // waiting (or suspended) is a good idea.
  //
  // Note: this is only done if we received a message, since otherwise
  // the state of the system is not changed.
  //
  rematch_tasks_to_workers( w );
  }
		
#if defined( SIMULATE_FAILURE )
//
// Fail with probability p
//
const double p = 0.0025;
	
if( drand48() < p ) {
   MWprintf ( 10, "Simulating FAILURE!\n" );
   terminate_workers();
   RMC->exit(1);
   }
#endif	


//
// If there is a worker_timeout_limit check all workers if there are
// timed-out workers in working state
//
if (worker_timeout) {
   time_t now = time(0);

   if (next_worker_timeout_check  <= now) {
      next_worker_timeout_check = now + worker_timeout_check_frequency; 
      MWprintf ( 80, "About to check timedout workers"); 	       					 
      bool timedout_flag;
      reassign_tasks_timedout_workers(timedout_flag);
      if (timedout_flag) busy=true;
      }
   }

#ifdef NWSENABLED
//
// TODO - remove this?
//
		time_t now = time(0);
		if ( next_nws_measurement_check < now )
		{
			MWprintf ( 10, "About to do nws measurements\n");
			do_nws_measurements ( );
		}
#endif
		
return ustat;
}


////--------------------------------------------------------
////
//// Worker Policy Management
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
void MWAbstractDriver<CommType>::set_machine_ordering_policy(
			MWMachineOrderingPolicy mode,
  			MWKey (*wkey)( const MWWorkerID<CommType> * ) )
{
switch(mode) {
  case NO_ORDER:
	machine_ordering_policy = mode;
	// Ignore wkey
        WorkerIDLess.keyFunc = 0;
	break;

  case BY_USER_BENCHMARK:
	machine_ordering_policy = mode;
	// Ignore wkey
        WorkerIDLess.keyFunc = &MWAbstractDriver<CommType>::compareBenchmark;
	break;

  case BY_KFLOPS:
	machine_ordering_policy = mode;
	// Ignore wkey
        WorkerIDLess.keyFunc = &MWAbstractDriver<CommType>::compareKFLOPS;
	break;

  case USER_ORDERING_POLICY:
	if (!wkey) {
           MWprintf ( 10, "Cannot set user-defined machine ordering policy without a comparison function.");
	   abort();
	   }
	machine_ordering_policy = mode;
        WorkerIDLess.keyFunc = wkey;
	break;

  default:
	break;
  };
}


template <class CommType>
MWBase::MWKey MWAbstractDriver<CommType>::compareKFLOPS( MWWorkerID<CommType> *w )
{ return (MWKey) w->KFlops; }

template <class CommType>
MWBase::MWKey MWAbstractDriver<CommType>::compareBenchmark( MWWorkerID<CommType> *w )
{ return (MWKey) w->get_bench_result(); }


template <class CommType>
int MWAbstractDriver<CommType>::master_slowdown=-1;


//============================================================================
//
//
template <class CommType>
void MWAbstractDriver<CommType>::set_worker_timeout_limit( 
					double timeout_limit, int timeout_freq ) 
{
if (timeout_limit > 0) {
   worker_timeout = true;
   worker_timeout_limit = timeout_limit;

   worker_timeout_check_frequency = timeout_freq ;
   next_worker_timeout_check = time(0) + timeout_freq ;

   MWprintf( 30, "Set worker timeout limit to %lf\n", timeout_limit);
   MWprintf( 30, "Set worker timeout frequency to %d\n", timeout_freq);
   }
else
   MWprintf(10, "ERROR: Timeout limit has to be > 0!\n");
}



////--------------------------------------------------------
////
//// Work Group Management
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
void MWAbstractDriver<CommType>::setupWorkGroups( int num )
{
if (num == (int)workGroupTaskNum.size())
   return;

if ( num < 1 ) {
   MWprintf ( 10, "Use of setupWorkGroups with < 1 classes not allowed\n");
   assert ( 0 );
   }

if (numTasksTodo() + numTasksRunning() > 0) {
   MWprintf ( 10, "Calling setupWorkGroups after tasks have been created!\n");
   assert ( 0 );
   }

workGroupWorkerNum.resize(num);
workGroupTaskNum.resize(num);
}



////--------------------------------------------------------
////
//// Checkpoint Management
////
////--------------------------------------------------------

//============================================================================
//
//
template <class CommType>
int MWAbstractDriver<CommType>::set_checkpoint_frequency( int freq )
{
if ( checkpoint_time_freq != 0 ) {
   MWprintf( 10, "Cannot set_checkpoint_frequency while time-based "
                                   "frequency is not zero!\n" );
   return checkpoint_frequency;
   }
int old = checkpoint_frequency;
checkpoint_frequency = freq;
return old;
}


//============================================================================
//
//
template <class CommType>
int MWAbstractDriver<CommType>::set_checkpoint_time ( int secs )
{
if ( checkpoint_frequency != 0 ) {
   MWprintf ( 10, "Cannot set_checkpoint_time while task-based "
                                   "frequency is not zero!\n" );
   return checkpoint_time_freq;
   }
int old = checkpoint_time_freq;
checkpoint_time_freq = secs;
next_ckpt_time = time(0) + secs;
return old;
}

#endif

