//
// MWDriver.h
//
// TODO: replace print styles with stream operators
//
// This provides an implementation of the MW API defined by
// MWAbstractDriver.  This design assumes:
//	1. Checkpointing is only done on the master
//	2. 
//

#ifndef __MWDriver
#define __MWDriver

#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <map>
#include <list>
#include <cstdlib>
#include <ctime>
#include <cassert>
#include <cstdarg>
#include <cerrno>
#include <cstdio>

#include <netdb.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "MW.h"
#include "MWWorkerID.h"
#include "MWWorker.h"
#include "MWTask.h"
#include "MWGroup.h"
#ifdef MWSTATS
#include "MWStats.h"
#endif



template <class CommType>
class MWDriver : public MWAbstractDriver<CommType> {

  friend class MWTask<CommType>;

public:

  /**@name Main Routines */
  //@{
  ///
  MWDriver();
	
  ///
  virtual ~MWDriver();
  //@}


  /**@name Worker Management */
  //@{
  ///
  int numWorkers() const
		{return workers.size();}

  ///
  int numActiveWorkers() const
		{return active_workers.size();}

  ///
  int numWorkers( int arch ) const;

  ///
  int numWorkersInState( typename MWBase::MWworker_states ThisState ) const;
	
  ///
  void printWorkers() const;
  //@}


  /**@name Task Management */
  //@{
  ///
  int numTasksTodo()
		{return todo.size();}

  ///
  int numTasksRunning()
		{return running_tasks.size();}

  ///
  int delete_tasks_greater_than( typename MWBase::MWKey );

  ///
  int print_task_keys( void );

  /// Returns the value (only) of the best key in the Todo list 
  typename MWBase::MWKey min_todo_keyval() {return 0.0;}
		//{return min_element(todo.begin(), todo.last());}

  /// Returns the best value (only) of the best key in the Running list.
  typename MWBase::MWKey min_running_keyval( )  {return 0.0;}
		//{return min_element(runningfirst(), running.last());}
  //@}


  /** @name Checkpoint Management */
  //@{
  ///
  void checkpoint();
	
  ///
  bool checkpoint_data_exists();

  ///
  void restart_from_ckpt();

  ///
  void checkpoint_end();
  //@}


#ifdef XML_OUTPUT
  /** @name XML I/O Management */
  //@{
  ///
  void  write_XML_status();

  ///
  virtual char* get_XML_results_status(void );
  //@}
#endif


protected:
	
  /** @name Protected Main Routines */
  //@{
  /** A function to support the execution of MWDriver from a GUI
      control panel. */
  void ControlPanel ( );	

  /// 
  bool send_task_to_worker( MWWorkerID<CommType> *w );
	
  ///
  void rematch_tasks_to_workers( MWWorkerID<CommType> *nosend );
	
  ///
  void call_hostaddlogic();

  ///
  void terminate_workers();
	
  /** This is called in both handle_hostdelete and handle_taskexit.
      It removes the host from our records and cleans up 
      relevent pointers with the task it's running. */
  void hostPostmortem ( MWWorkerID<CommType> *w );

#ifdef MWSTATS
  ///
  void collect_statistics();

  /** The instance of the stats class that takes workers and later
      prints out relevant stats...
    */
  MWStatistics<MWWorkerID<CommType>,CommType>* stats;
#endif

  /// This returns the hostname
  const char *getHostName ();
  //@}


  /**@name Protected MainLoop Management */
  //@{
  ///
  typename MWDriver<CommType>::MWReturn handle_worker_results( MWWorkerID<CommType>* w );
	
  ///
  typename MWDriver<CommType>::MWReturn handle_benchmark( MWWorkerID<CommType> *w );

  ///
  void handle_hostdel();
	
  ///
  void handle_hostsuspend();
	
  ///
  void handle_hostresume();
  
  ///
  void handle_taskexit();

  ///
  void handle_checksum ();
  //@}
	

  /** @name Protected Task Management
   */
  //@{
  /// Add a task to the list.  The MWDriver assumes ownership of the task.
  void addTask( MWTask<CommType>* task );
	
  /** Add a bunch of tasks to the list.  You do this by making
      an array of pointers to MWTasks and giving that array to 
      this function.  The MWDriver assumes ownership of the tasks. */
  void addTasks( vector<MWTask<CommType>*>& tasklist)
		{
		for (size_t i=0; i<tasklist.size(); i++)
		  addTask(tasklist[i]);
		}

  /** Go through the list of timed out WORKING workers and reschedule tasks */
  void reassign_tasks_timedout_workers(bool& busy);

  /// This puts a (generally failed) task at the beginning of the list
  void pushTask( MWTask<CommType> * );

  /// Get the next task from the todo list.
  virtual MWTask<CommType>* getNextTask( MWGroup& grp );
	
  /// Removes a task from the set of running tasks
  MWTask<CommType>* remove_running_task( int tasknum )
		{
		typename map<int,MWTask<CommType>*>::iterator iter = running_tasks.find(tasknum);
		if (iter == running_tasks.end())
		   return 0;
		MWTask<CommType>* tmp = iter->second;
		running_tasks.erase(iter);
		return tmp;
		}
	
  /// Print the tasks in the list of tasks to do
  void print_running_tasks();
	
  /** Add one task to the todo list; do NOT set the 'number' of
      the task - useful in restarting from a checkpoint */
  void ckpt_addTask(MWTask<CommType>* task)
		{pushTask(task);}
	
  /// returns the worker this task is assigned to, NULL if none.
  MWWorkerID<CommType>* task_assigned( MWTask<CommType> *t )
		{return t->worker;}
	
  /// Assign a task to a worker and send a message to the worker.
  void assign_task_to_worker ( MWWorkerID<CommType>* w, MWTask<CommType>* t );

  /// Release a worker
  void release_worker (MWWorkerID<CommType>* w, MWTask<CommType>* t)
		{
		t->worker=NULL;
		w->runningtask=NULL;
		}

  /** MWDriver keeps a unique identifier for each task -- 
      here's the counter */
  int task_counter;
	
  /** A map of tasknum to task object for running tasks.
    * The todo list may be ordered in some arbitrary order.  However, we
    * frequently need to find a running task given its number.  It might be 
    * more efficient to do this with a hash table, but they aren't part of the 
    * STL standard just yet.  */
  map<int,MWTask<CommType>*> running_tasks;

  /** The list of tasks to do.
    * This list may be reordered, so iterators pointing into it may become 
    * invalid.
    */
  list<MWTask<CommType>*> todo;
  //@}


  /** @name Protected Worker Management
   */
  //@{
  ///
  bool worker_marked_for_removal(MWWorkerID<CommType>* w)
		{return w->is_doomed();}

  ///
  void worker_add ( MWWorkerID<CommType> *w );
  
  /// Looks up information about a worker given its task ID
  MWWorkerID<CommType> * worker_find( int tid, bool erase_flag=false )
		{
		typename map<int,MWWorkerID<CommType>*>::iterator iter = workers.find(tid);
		if (iter == workers.end())
		   return 0;
		MWWorkerID<CommType>* tmp = iter->second;
		if (erase_flag)
		   workers.erase(iter);
		return tmp;
		}
	
  ///
  typename MWDriver<CommType>::MWReturn worker_init( MWWorkerID<CommType> *w );
	
  /// This function removes worker from the list, removes it and deletes
  /// the structure.
  void worker_terminate( MWWorkerID<CommType> *w );

  /// Based on the worker ordering policy, reorder the workers list appropriately
  void reorder_workers();

  /** A map of host_id to worker object.
    * The workers list may be ordered in some arbitrary order.  However, we
    * frequently need to find a worker given its id.  It might be more efficient
    * to do this with a hash table, but they aren't part of the STL standard
    * just yet.  */
  map<int,MWWorkerID<CommType>*> workers;

  /** The list of idle workers.
    * This list may be reordered to prioritize the workers, so iterators
    * into this list may become invalid. */
  list<MWWorkerID<CommType>*> idle_workers;

  /** The list of active workers.
    * These will usually be running.  If a running worker stops working, then
    * the master will remove it.  The running tasks are associated 
    * with these running workers, so we don't keep a seperate structure for them.
    */
  list<MWWorkerID<CommType>*> active_workers;

  /** Move this workere into the appropriate list (idle/active) based on
    * its current state.
    */
  void update_worker_state(MWWorkerID<CommType>* w);

  /** A class that is used to record the state of a worker in the 
    * MWDriver.
    */
  template <class CommType_>
  class MWWorkerState {
    public:
    bool sflag;
    typename list<MWWorkerID<CommType_>*>::iterator iter;
    };

  /** Maps worker id1's to a MWWorkerState object, which has info
    * about whether a worker is recorded as idle or active.
    */
  map<int,MWWorkerState<CommType>*> worker_state;
  //@}
	

  /** @name Protected Checkpointing Management */
  //@{
  /** The name of the checkpoint file */
  string ckpt_filename;
  //@}


#if defined( XML_OUTPUT )
  
  /**@name XML and Status Methods.

     This function is called by the CORBA layer to get the
     XML status of the MWDriver.
  */
  //@{
  /// Returns the sum of the bench results for the currently working machines
  double get_instant_pool_perf();

  ///
  char* get_XML_status();

  ///
  char* get_XML_job_information();

  ///
  char* get_XML_problem_description();

  ///
  char* get_XML_interface_remote_files();
  
  ///
  char* get_XML_resources_status();

  ///
  const char *xml_filename;

  ///
  const char *xml_menus_filename;

  ///
  const char *xml_jobinfo_filename;

  ///
  const char *xml_pbdescrib_filename;

  /// Set the current machine information
  void get_machine_info();

  /// Returns a pointer to the machine's Arch
  char *get_Arch(){ return Arch; }
  
  /// Returns a pointer to the machine's OpSys
  char *get_OpSys(){ return OpSys; }

  /// Returns a pointer to the machine's IPAddress
  char *get_IPAddress(){ return IPAddress; }

  ///
  double get_CondorLoadAvg(){ return CondorLoadAvg; }

  ///
  double get_LoadAvg(){ return LoadAvg; }

  ///
  int get_Memory(){return Memory;}

  ///
  int get_Cpus(){return Cpus;}

  ///
  int get_VirtualMemory(){return VirtualMemory;}

  ///
  int get_Disk(){return Disk;}

  ///
  int get_KFlops(){return KFlops;}

  ///
  int get_Mips(){return Mips;}

  ///
  char Arch[64];

  ///
  char OpSys[64];

  ///
  char IPAddress[64];

  ///
  double CondorLoadAvg;

  ///
  double LoadAvg;

  ///
  int Memory;

  ///
  int Cpus;

  ///
  int VirtualMemory;

  ///
  int Disk;

  ///
  int KFlops;

  ///
  int Mips;

  /// Utility functions used by get_machine info
  int check_for_int_val(char* name, char* key, char* value);
  double check_for_float_val(char* name, char* key, char* value);
  int check_for_string_val(char* name, char* key, char* value);
  //@}
#endif

#ifdef NWSENABLED
  
  /// The name of the machine the worker is running on.
  char mach_name[64];

  /// for measuring network connectivity
  double defaultTimeInterval;

  ///
  int defaultIterations;

  ///
  int defaultPortNo;

  time_t nws_measurement_period;
  time_t next_nws_measurement_check;
  int nws_nameserver_port;
  int nws_memory_port;
  int nws_sensor_port;
  void do_nws_measurements ( );
  void lookupWorkerAndsetConnectivity ( char *host, double latency );
  void start_nws_daemons ( );
#endif
	
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
MWDriver<CommType>::MWDriver() 
 : task_counter(0),
   ckpt_filename("checkpoint")
{
#ifdef MWSTATS
stats = new MWStatistics<MWWorkerID<CommType>,CommType>();
#endif
    
#if defined( XML_OUTPUT )
xml_menus_filename = "menus";
xml_jobinfo_filename = "job_info";
xml_pbdescrib_filename = "pbdescrib";
xml_filename = "/u/m/e/metaneos/public/html/iMW/status.xml";
CondorLoadAvg = NO_VAL;
LoadAvg = NO_VAL;
Memory = NO_VAL;
Cpus = NO_VAL;
VirtualMemory = NO_VAL;
Disk = NO_VAL;
KFlops = NO_VAL;
Mips = NO_VAL;
memset(OpSys, 0, sizeof(OpSys)); 
memset(Arch, 0 , sizeof(Arch)); 
memset(mach_name, 0 , sizeof(mach_name)); 
get_machine_info();
#endif    

#ifdef NWSENABLED
defaultTimeInterval = 10;
defaultIterations = 5;
defaultPortNo = 7;
nws_measurement_period = 60;
next_nws_measurement_check = time(0) + nws_measurement_period;
nws_nameserver_port = 8095;
nws_memory_port = 8195;
nws_sensor_port = 8295;
#endif
}


//============================================================================
//
//
template <class CommType>
MWDriver<CommType>::~MWDriver() 
{
//
// Delete the workers
//
typename map<int,MWWorkerID<CommType>*>::iterator currWorker = workers.begin();
typename map<int,MWWorkerID<CommType>*>::iterator lastWorker = workers.end();
while (currWorker != lastWorker) {
  delete currWorker->second;
  ++currWorker;
  }

//
// Delete the tasks
//
typename map<int,MWTask<CommType>*>::iterator currTask = running_tasks.begin();
typename map<int,MWTask<CommType>*>::iterator lastTask = running_tasks.end();
while (currTask != lastTask) {
  delete currTask->second;
  ++currTask;
  }

#ifdef MWSTATS
if ( stats )
   delete stats;
#endif
}


////--------------------------------------------------------
////
//// Worker Management
////
////--------------------------------------------------------


//============================================================================
//
// Note: this could be made more efficient by recording the 
// exec class of a worker when adding/deleting.  However, for now
// we assume that host add/deletion is relatively infrequent.
//
template <class CommType>
int MWDriver<CommType>::numWorkers( int ex_cl ) const
{
int count = 0;

typename map<int,MWWorkerID<CommType>*>::const_iterator curr = workers.begin();
typename map<int,MWWorkerID<CommType>*>::const_iterator last = workers.end();
while (curr != last) {
  if (curr->second->get_exec_class() == ex_cl)
     count++;
  ++curr;
  }

return count;
}


//============================================================================
//
//
template <class CommType>
int MWDriver<CommType>::numWorkersInState( typename MWBase::MWworker_states ThisState ) const
{
int count = 0;   
  
typename map<int,MWWorkerID<CommType>*>::const_iterator curr = workers.begin();
typename map<int,MWWorkerID<CommType>*>::const_iterator last = workers.end();
while (curr != last) {
  // We handle the case WORKING separately such that
  // we don't count the workers with no runningtask
  if (ThisState == WORKING) {
     if ( (curr->second->currentState() == ThisState) && 
	  (curr->second->runningtask != NULL) ) 
	count++;
     }
  else {
     if ( curr->second->currentState() == ThisState ) 
	count++;
     }

  curr++;
  }

return count;
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::printWorkers() const
{
MWprintf ( 1, "---- List of Idle Workers follows: ----------------\n" );

typename list<MWWorkerID<CommType>*>::const_iterator curr = idle_workers.begin();
typename list<MWWorkerID<CommType>*>::const_iterator last = idle_workers.end();
while (curr != last) {
  (*curr)->printself(91);
  curr++;
  }

MWprintf ( 1, "---- List of Active Workers follows: ----------------\n" );

curr = active_workers.begin();
last = active_workers.end();
while (curr != last) {
  (*curr)->printself(91);
  curr++;
  }

MWprintf ( 1, "---- End worker list --- %d workers ------------\n", 
			workers.size() );
}


////--------------------------------------------------------
////
//// Task Management
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
int MWDriver<CommType>::delete_tasks_greater_than( typename MWBase::MWKey key ) 
{
if ( ! TaskLess ) {
   MWprintf( 10, " delete_task_worse_than:  no task key "
				  "function defined!\n");
   return -1;
   }
	
typename list<MWTask<CommType>*>::iterator currTask = todo.begin();
typename list<MWTask<CommType>*>::iterator lastTask = todo.end();
while (currTask != lastTask) {
  if ( (*TaskLess.keyFunc)( *currTask ) > key) {
     todo.erase(currTask);
     delete *currTask;
     }

  ++currTask;
  }

return 0;
}


//============================================================================
//
//
template <class CommType>
int MWDriver<CommType>::print_task_keys() 
{
if ( ! TaskLess ) {
   MWprintf( 10, " No key function assigned to tasks -- Can't print\n");
   return -1;
   }

MWprintf( 10, "Task Keys:\n");
typename list<MWTask<CommType>*>::iterator currTask = todo.begin();
typename list<MWTask<CommType>*>::iterator lastTask = todo.end();
while (currTask != lastTask) {
  MWprintf( 10, "   %f\t(%d)\n", (*TaskLess.keyFunc)(*currTask), (*currTask)->number );

  ++currTask;
  }

return 0;
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
void MWDriver<CommType>::checkpoint() 
{
    
int total;
ofstream ofstr;
/* We're going to write the checkpoint to a temporary file, 
	   then move the file to "ckpt_filename" */

/* We're not going to use tempnam(), because it sometimes 
	   gives us a filename in /var/tmp, which is bad for rename() */

const char *tempName = "mw_tmp_ckpt_file";

ofstr.open(tempName);
if ( !ofstr ) {
   MWprintf ( 10, "Failed to open %s for writing! errno %d\n", 
				   tempName, errno );
   return;
   }


//
// Write header
//
MWprintf ( 50, "Beginning checkpoint()\n" );
ofstr << "MWDriver Checkpoint File." << time(0) << endl;
    
//
// Write internal MWDriver state:
//
ofstr << task_counter << " "
	<< checkpoint_frequency << " "
	<< checkpoint_time_freq << " "
	<< num_completed_tasks << endl;
ofstr << (int)suspensionPolicy << " "
	<< (int)machine_ordering_policy << endl;
if ( bench_task ) {
   ofstr << 1 << " ";
   bench_task->write_ckpt_info ( ofstr );
   }
else
   ofstr << 0 << endl;
MWprintf ( 80, "Wrote internal MWDriver state.\n" );


//
// Write RMC state
//
RMC->write_checkpoint( ofstr );
MWprintf ( 80, "Wrote RMC state.\n" );

#ifdef MWSTATS
//
// Write worker stats
//
stats->write_checkpoint( ofstr, workers );
MWprintf ( 80, "Wrote the Worker stats.\n" );
#endif

//
// Write the user master's state:
//
write_master_state( ofstr );
MWprintf ( 80, "Wrote the user master's state.\n" );

//
// Write the number of work groups
//
ofstr << workGroupWorkerNum.size() << endl;


//
// Write active task info
//
int tempnum = 0;
typename list<MWWorkerID<CommType>*>::iterator w_curr = active_workers.begin();
typename list<MWWorkerID<CommType>*>::iterator w_last = active_workers.end();
while (w_curr != w_last) {
  if ((*w_curr)->runningtask->taskType == MWTask<CommType>::NORMAL)
     tempnum++;

  w_curr++;
  }
int num_tasks = tempnum + todo.size();
    
ofstr << "Tasks: " << num_tasks << endl;

total = 0;
w_curr = active_workers.begin();
w_last = active_workers.end();
while (w_curr != w_last) {
  if ((*w_curr)->runningtask->taskType == MWTask<CommType>::NORMAL) {
     ofstr << (*w_curr)->runningtask->number << " ";
     (*w_curr)->runningtask->write_ckpt_info(ofstr);
     (*w_curr)->runningtask->write_group_info(ofstr);
     }
  w_curr++;
  }

//
// Write todo task info
//
typename list<MWTask<CommType>*>::iterator t_curr = todo.begin();
typename list<MWTask<CommType>*>::iterator t_last = todo.end();
while (t_curr != t_last) {
  ofstr << (*t_curr)->number << " ";
  (*t_curr)->write_ckpt_info(ofstr);
  (*t_curr)->write_group_info(ofstr);
  }
MWprintf ( 80, "Wrote task list.\n" );

ofstr.close();

/* Now we rename our temp file to the real thing. Atomicity! */
if ( rename( tempName, ckpt_filename.data() ) == -1 ) {
   MWprintf ( 10, "rename( %s, %s ) failed! errno %d.\n", 
			   tempName, ckpt_filename.data(), errno );
   }

MWprintf ( 50, "Done checkpointing.\n" );
}


//============================================================================
//
//
template <class CommType>
bool MWDriver<CommType>::checkpoint_data_exists()
{
struct stat statbuf;
return (stat( ckpt_filename.data(), &statbuf ) == -1);
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::restart_from_ckpt() 
{
ifstream ifstr(ckpt_filename.data());

if ( !ifstr ) {
   MWprintf ( 10, "Failed to open %s for reading! errno %d\n", 
			   ckpt_filename.data(), errno );
   return;
   }

char buf[128];
time_t then;
ifstr >> buf >> buf >> buf >> then;
MWprintf ( 10, "This checkpoint made on %s\n", ctime( &then ) );

ifstr >> task_counter >> checkpoint_frequency >> checkpoint_time_freq
		>> num_completed_tasks;

int susp_policy, mach_policy;
ifstr >> susp_policy >> mach_policy;
suspensionPolicy = (typename MWDriver<CommType>::MWSuspensionPolicy) susp_policy;
machine_ordering_policy = (typename MWDriver<CommType>::MWMachineOrderingPolicy) mach_policy;

int hasbench = 0;
ifstr >> hasbench;
if ( hasbench ) {
   if (!bench_task)
      bench_task = allocate_benchmark_task();
   assert(bench_task != 0);
   bench_task->read_ckpt_info( ifstr );
   MWprintf ( 10, "Got benchmark task:\n" );
   bench_task->printself( 10 );
   } 
MWprintf ( 10, "Read internal MW info.\n" );

RMC->read_checkpoint( ifstr );
MWprintf ( 10, "Read RMC state.\n" );

#ifdef MWSTATS
// read old stats 
stats->read_checkpoint( ifstr );
#endif
MWprintf ( 10, "Read defunct workers' stats.\n" );

read_master_state( ifstr );
MWprintf ( 10, "Read the user master's state.\n" );

int MWworkGroups;
ifstr >> MWworkGroups;
setupWorkGroups(MWworkGroups);

int num_tasks;
ifstr >> buf >> num_tasks;
if ( strcmp( buf, "Tasks:" ) ) {
   MWprintf ( 10, "Problem in reading Tasks separator. buf = %s\n", buf );
   ifstr.close();
   return;
   }

MWTask<CommType> *t;
for (int i=0 ; i<num_tasks ; i++ ) {
    t = allocate_task();
    ifstr >> t->number;
    t->read_ckpt_info( ifstr );
    t->read_group_info ( ifstr );
    t->printself( 80 );
    ckpt_addTask( t );
    }
MWprintf ( 10, "Read the task list.\n" );

ifstr.close();
MWprintf ( 10, "We are done restarting.\n" );
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::checkpoint_end()
{
//
// Remove checkpoint file.
//
struct stat statbuf;
if ( stat( ckpt_filename.data(), &statbuf ) == 0 ) {
   if ( unlink( ckpt_filename.data() ) != 0 )
      MWprintf ( 10, "unlink %s failed! errno %d\n",
                                         ckpt_filename.data(), errno );
   }
}


////--------------------------------------------------------
////
//// Protected Main Routines
////
////--------------------------------------------------------


//
//============================================================================
//
template <class CommType>
void MWDriver<CommType>::ControlPanel ( )
{
typename MWBase::MWReturn ustat = OK;
RMC->hostadd ();
master_handle_msg();
MWWorkerID<CommType> *worker = allocate_workerID();
worker->go( 1, NULL );
master_handle_msg();
worker->ind_benchmark ( );
ustat = master_handle_msg();
    
while ( ( (!todo.empty()) || (active_workers.size() > 0) ) && ( ustat == OK ) ) {
  ustat = worker->worker_mainloop_ind ( );
  ustat = master_handle_msg();
  }
    
MWprintf(71, "\n\n\n ***** Almost done ***** \n\n\n\n");
delete worker;
}


//============================================================================
//
//
template <class CommType>
bool MWDriver<CommType>::send_task_to_worker( MWWorkerID<CommType> *w ) 
{
//
// It can be the case that a worker who asks for work might
// already have some work that has been sent (e.g. after we execute
// rematch_tasks_to_workers), in which case we don't want to give
// him another task. 
//
if ( w->currentState() != IDLE )
   return false;

MWTask<CommType> *t = NULL;

#ifdef NWSENABLED
double lastMeasured;
w->getNetworkConnectivity ( lastMeasured );
struct timeval timeVal;
gettimeofday ( &timeVal, NULL );
double nowTime = timeval_to_double ( timeVal );

if ( nowTime > lastMeasured + 60 * 60 ) {
   MWprintf ( 10, "Creating NWSTask: nowTime = %f, lastMeasured = %f\n", 
							nowTime, lastMeasured);
		
   MWprintf ( 10, "Sending MWNWSTask %f %d\n", defaultTimeInterval, 
							defaultIterations);
   t = new MWNWSTask ( task_counter++, defaultTimeInterval, defaultIterations, 
						defaultPortNo, getHostName() );
   t->printself ( 10 );
   }
else
#endif

t = getNextTask( w->getGroup() );

//
// We have a task to do ... so assign the worker to it.
//
if ( t != NULL ) {
   assign_task_to_worker ( w, t );
   MWprintf ( 10, "Sent task %d to %s  ( id: %d ).\n", 
				t->number, w->machine_name(), w->get_id1() );
   return true;
   }
//
// This added to make things work better at program's end:
// The end result will be that a suspended worker will have its
// task adopted.  We first look for a task with a suspended worker.
// We call it t.  The suspended worker's 'runningtask' pointer 
// will still point to t, but t->worker will point to this
// worker (w).  Now there will be two workers working on the 
// same task.  The first one to finish reports the results, and
// the other worker's results get discarded when (if) they come in.
//
if ( active_workers.size() > 0) {
   // here there are no tasks on the todo list, but there are 
   // tasks in the run queue.  See if any of those tasks are
   // being held by a suspended worker.  If so, adopt that task.
   typename list<MWWorkerID<CommType>*>::iterator curr = active_workers.begin();
   typename list<MWWorkerID<CommType>*>::iterator last = active_workers.end();
   while (curr != last) {
     if (t->worker->isSusp() && t->getGroup().doesOverlap(&((*curr)->getGroup()))) {
          MWprintf ( 60, "Re-assigning task %d to %s.\n", 
		t->number, (*curr)->machine_name() );
          assign_task_to_worker ( *curr, t );
	  return true;
          }
       curr++;
       }
   return false;
   }

return false;
//
// The task is null and no tasks can be adopted ... so the worker
// stays on the idle list.
//
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::rematch_tasks_to_workers( MWWorkerID<CommType> *nosend )
{
//
// For each worker that is idle, execute send_task_to_worker.
// now do a send_task_to_worker.  Don't send it to the worker "nosend",
// who just reported in.  
//  
int ns = nosend ? nosend->get_id1() : -1;
    
bool flag=true;
typename list<MWWorkerID<CommType>*>::iterator curr = idle_workers.begin();
typename list<MWWorkerID<CommType>*>::iterator last = idle_workers.end();
while (flag && (curr != last)) {
  if ( ( (*curr)->currentState() == IDLE ) && 
	( ( (*curr)->get_id1() != ns ) || ((*curr)->get_id2() != ns) ) )  {
     MWprintf ( 90, "In rematch_tasks_to_workers(), trying send...\n" );
     flag = send_task_to_worker( *curr );
     }
  curr++;
  }
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::call_hostaddlogic() 
{
//
// This is a wrapper around the lower level's hostaddlogic(), 
// which will return 0 as a basic OK value, and a positive number
// of hosts to delete if it thinks we should delete hosts.  This
// can happen if the user changes the target number of workers 
// to a lower value.
//

vector<int> num_workers(RMC->get_num_exec_classes());
for (int j=0 ; j < RMC->get_num_arch_classes() ; j++ )
  num_workers[j] = numWorkers( j );

int numtodelete = RMC->hostaddlogic( num_workers );
if ( numtodelete == 0 )
   return;

//
// Make sure we don't count already doomed machines:
//
typename map<int,MWWorkerID<CommType>*>::iterator curr = workers.begin();
typename map<int,MWWorkerID<CommType>*>::iterator last = workers.end();
while (curr != last) {
  if ( curr->second->is_doomed() )
     numtodelete--;
  curr++;
  }

MWprintf ( 40, "Deleting %d hosts.\n", numtodelete );
MWprintf ( 40, "Ignore the HOSTDELETE or TASKEXIT messages.\n" );

//
// Walk thru list and kill idle workers.
//
curr = workers.begin();
last = workers.end();
while (curr != last) {
  if (numtodelete <= 0) break;
  if (curr->second->isIdle()) {
     MWprintf ( 80, "Idle worker:\n" );
     worker_terminate ( curr->second );
     numtodelete--;
     }
  curr++;
  }
//
// Now walk thru workers and remove suspended machines
//
// Note: can we combine this with the previous loop?
//
if ( numtodelete <= 0 ) return;
curr = workers.begin();
last = workers.end();
while (curr != last) {
  if (numtodelete <= 0) break;
  if (curr->second->isSusp()) {
     MWprintf ( 80, "Suspended worker:\n" );
     curr->second->printself ( 80 );
     hostPostmortem( curr->second ); // this really does do what we want!
     RMC->removeWorker( curr->second );
     numtodelete--;
     } 
  }

//
// Finally, walk thru list and mark working workers for removal
//
// TODO: verify when this would happen!
//
if ( numtodelete <= 0 ) return;
curr = workers.begin();
last = workers.end();
while (curr != last) {
  if (numtodelete <= 0) break;
  if (!(curr->second->is_doomed())) {
     curr->second->mark_for_removal();
     MWprintf ( 40, "Host %s marked for removal.\n", 
				  curr->second->machine_name() );
     numtodelete--;
     }
   curr++;
   }
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::terminate_workers() 
{
MWprintf( 40, "Terminating workers:\n");

typename map<int,MWWorkerID<CommType>*>::iterator curr = workers.begin();
typename map<int,MWWorkerID<CommType>*>::iterator last = workers.end();
while (curr != last) {
  if (!local_worker || (curr->second->get_id1() != local_worker->myid)) {
     RMC->initsend ( );
     MWprintf( 140, "Sending Termination Message: %d %d\n", curr->second->get_id1(), KILL_YOURSELF);
     RMC->send ( curr->second->get_id1(), KILL_YOURSELF );
     }
  curr->second->ended();
  update_worker_state( curr->second );
#ifdef MWSTATS
  stats->gather(curr->second);
#endif
  RMC->exec_class_num_workers(curr->second->get_exec_class())--;
  for ( int i = 0; i < numWorkGroups(); i++ ) {
    if ( curr->second->doesBelong ( i ) )
       curr->second->deleteGroup ( i );
    }
  RMC->removeWorker( curr->second );

  curr++;
  }
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::hostPostmortem (MWWorkerID<CommType> *w) 
{
w->ended();
RMC->exec_class_num_workers(w->get_exec_class())--;
for ( int i = 0; i < numWorkGroups(); i++ ) {
  if ( w->doesBelong ( i ) )
     w->deleteGroup ( i );
  }

MWTask<CommType>* t = w->runningtask;
if ( t == NULL )
   MWprintf ( 60, " %s (%d) was not running a task.\n", 
				 w->machine_name(), w->get_id1() );

else if ( t->taskType == MWTask<CommType>::NORMAL ) {
   MWprintf ( 60, " %s (%d) was running task %d.\n", 
				 w->machine_name(), w->get_id1(), t->number );
   //
   // remove that task from the running queue...iff that
   // task hasn't ALREADY been reassigned.  
   //
   w->runningtask = NULL;
   MWWorkerID<CommType> *o;

   if ( !(o = task_assigned( t )) ) {
       MWTask<CommType> *check = remove_running_task( t->number );
       if ( t == check ) {
	  t->worker = NULL;
	  pushTask( t );
	  }
       }		
   else {
      t->worker = o;
      MWprintf ( 60, " Task %d already has been reassigned\n", t->number );
      }
   }

else {
   // It is a control task
   // XXX Maybe also need to delete the task from RunQ, Insure complained about this.
   MWTask<CommType>* check = remove_running_task( t->number );
   if ( t == check )
      MWprintf(21, "The control task IS in the RunQ, and should haven't been deleted!");
   //
   // TODO - need to update the group info here??
   //
   delete t;
   }

#ifdef MWSTATS
stats->gather( w );
#endif
}
		

//============================================================================
//
//
template <class CommType>
const char* MWDriver<CommType>::getHostName ()
{
struct utsname nm;
struct hostent *hostptr;
struct in_addr *ptr;

uname ( &nm );
hostptr = gethostbyname ( nm.nodename );
ptr = (struct in_addr *)hostptr->h_addr_list[0];

return inet_ntoa ( *ptr );
}


////--------------------------------------------------------
////
//// Protected MainLoop Routines
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
typename MWDriver<CommType>::MWReturn MWDriver<CommType>::handle_worker_results( MWWorkerID<CommType>* w )
{
typename MWBase::MWReturn ustat = OK;

int tasknum;
double wall_time = 0;
double cpu_time = 0;
RMC->unpack( &tasknum );
RMC->unpack( &wall_time );
RMC->unpack( &cpu_time );
MWprintf( 90, "handle_worker_results: num=%d wall=%f cpu=%f\n", tasknum, wall_time, cpu_time);

//
// A simple check to verify that cpu_time is not NAN.
//
if ( cpu_time != cpu_time )
   MWprintf( 10, "Error.  cpu_time of %lf, assigning to wall_time of %lf\n",
							cpu_time, wall_time );
	
//
// Sanity checks on cpu_time
//
if ( cpu_time > 1.1 * wall_time || cpu_time < 0.01 * wall_time ) {
   MWprintf( 10, "Warning.  Cpu time =%lf,  wall time = %lf seems weird.\n", 
						cpu_time, wall_time );
   MWprintf( 10, "Assigning cpu time to be %lf\n", wall_time );
						cpu_time = wall_time;
   }

//
// Summarize worker performance
//
MWprintf ( 60, "Unpacked task %d result from %s ( id %d ).\n", 
				tasknum, w->machine_name(), w->get_id1() );
MWprintf ( 60, "It took %5.2f secs wall clock and %5.2f secs cpu time\n", 
				wall_time, cpu_time );

//
// Sanity checks on task info
//
if (!w->runningtask) {
   MWprintf( 10, "MWDriver::handle_worker_results -- ERROR: %s (id %d) does not have a running task.\n", w->machine_name(), w->get_id1());
   return QUIT;
   }
if ( tasknum != w->runningtask->number) {
   MWprintf( 10, "MWDriver::handle_worker_results -- ERROR: MW thinks that %s (id %d) was running %d, not %d\n", w->machine_name(), w->get_id1(), w->runningtask->number, tasknum );
   return QUIT;
   }

//
// Remove this task from the running map
//
MWTask<CommType> *t = remove_running_task(tasknum);

if ( w->isSusp() ) {
   MWprintf ( 60, "Got results from a suspended worker!\n" );
   //
   // In an asynchronous world, the worker send its results and then get
   // suspended, which could cause the task to get REASSIGNed if the results
   // message takes a while to get to the master.
   //
   // We set the status flag for this task, so we know to delete it
   // when it gets removed from the todo list.
   //
   if ( suspensionPolicy == REASSIGN )
      t->status=true;
   }

if ( t == NULL ) {
   MWprintf ( 60, "Task not found in running list; assumed done.\n" );
   w->completedtask( wall_time, cpu_time );
   update_worker_state( w );
   if ( w->runningtask )
      w->runningtask = NULL;
   return OK;
   }

//
// Unpack the results
//
t->unpack_results();
t->working_time = wall_time;
t->cpu_time = cpu_time;

if ( t->taskType == MWTask<CommType>::NORMAL ) {
   ustat = act_on_completed_task( t );
   num_completed_tasks++;

   //
   // Perform checkpointing if required
   //
   if (perform_checkpoint) {
      if ( (checkpoint_frequency > 0) && 
           (num_completed_tasks % checkpoint_frequency) == 0 )
         checkpoint();
      else {
         if ( checkpoint_time_freq > 0 ) {
	    time_t now = time(0);
	    MWprintf ( 80, "Trying ckpt-time, %d %d\n", 
				   			next_ckpt_time, now );
	    if ( next_ckpt_time <= now ) {
	       next_ckpt_time = now + checkpoint_time_freq;
	       MWprintf ( 80, "About to...next_ckpt_time = %d\n", 
							   next_ckpt_time );
	       checkpoint();
	       }
	    }
         }
      }
   }

#ifdef NWSENABLED
      else
		{
			if ( t->taskType == MWNWS )
			{
				if ( w ) 
					w->setNetworkConnectivity ( ((MWNWSTask *)t)->median );
				MWprintf ( 10, "Got results of NWSTask\n");
			}
		}
#endif

if ( t->worker == NULL ) 
   MWprintf ( 40, "Task had no worker...huh?\n" );

if ( w != t->worker )
   MWprintf ( 90, "This task not done by 'primary' worker, but that's ok.\n" );
else
   MWprintf ( 90, "Task from 'primary' worker %s.\n", w->machine_name() );
		
w->completedtask( wall_time, cpu_time );
update_worker_state( w );
release_worker(w,t);

//
// Note: this appears to be the only place that we need to cleanup the group
// data for a task.
//
if ( ! task_assigned( t ) ) {
   MWprintf( 80, "Deleting task %d\n", t->number );
   for ( int tempi = 0; tempi < numWorkGroups(); tempi++ ) {
     if ( t->doesBelong ( tempi ) )
	t->deleteGroup ( tempi );
     }
   delete t;
   }

return ustat;
}


//============================================================================
//
//
template <class CommType>
typename MWDriver<CommType>::MWReturn MWDriver<CommType>::handle_benchmark ( MWWorkerID<CommType> *w ) 
{
//
// Unpack the status flag for benchmarking
//
int okay = 1;
RMC->unpack( &okay, 1 );
if ( okay == -1 ) {
   MWprintf ( 10, "A worker failed to initialize!\n" );
   w->printself(10);
   worker_terminate ( w );
   return QUIT;
   } 

//
// Unpack the benchmark result
//
double bres = 0.0;
RMC->unpack( &bres, 1 );
MWprintf( 10, "Received bench result %lf from %s ( id: %d )\n", 
		  bres, w->machine_name(), w->get_id1() );

//
// Update various data structures
//
w->set_bench_result( bres );
#ifdef MWSTATS
stats->update_best_bench_results( bres );
#endif
w->benchmarkOver();
update_worker_state( w );
reorder_workers();

return OK;
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::handle_hostdel () 
{
//
// Here we find out a host has gone down.
// this tells us who died:
//
int who;
RMC->who ( &who );

MWWorkerID<CommType> *w = worker_find( who, true );
if ( w == NULL ) 
   MWprintf ( 40, "Got HOSTDELETE of worker %d, but it isn't in "
				   "the worker list.\n", who );
else {
   MWprintf ( 40, "Got HOSTDELETE!  \" %s \" (%d) went down.\n", 
				   w->machine_name(), who );   
   hostPostmortem ( w );
   delete w;
   // 
   // See if we need more hosts
   //
   call_hostaddlogic();
   }
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::handle_hostsuspend () 
{
//
// Find the worker
//
int who;
   RMC->who ( &who );
MWWorkerID<CommType> *w = worker_find ( who );
if ( w == NULL ) {
   MWprintf ( 40, "Got HOSTSUSPEND of worker %d, but it isn't in "
						"the worker list.\n", who );
   return;
   }

w->suspended();
update_worker_state( w );
MWprintf ( 40, "Got HOSTSUSPEND!  \"%s\" is suspended.\n", w->machine_name() );   

if ( !w->runningtask) {
   MWprintf ( 60, " --> It was not running a task.\n" );
   return;
   }

else if ( w->runningtask->taskType != MWTask<CommType>::NORMAL ) {
   MWprintf ( 60, " --> It was running a control task.\n");
   return;
   }

else {  //  w->runningtask->taskType == MWTask<CommType>::NORMAL
   int task = w->runningtask->number;
   MWprintf ( 60, " --> It was running task %d.\n", task );

   switch ( suspensionPolicy ) {

	case REASSIGN: 
		{
		// remove task from running list; push on todo.

		// First see if it's *on* the running list.  If it's 
		//	not, then it's either back on the todo list or
		//	its already done.  If this is the case, ignore. */
		MWTask<CommType> *rmTask = remove_running_task( task );
		if ( !rmTask ) {
		   MWprintf ( 60, "Not Re-Assigning task %d.\n", task );
		   break;
		   }

		MWTask<CommType> *rt = w->runningtask;
		if ( rt != rmTask )
			MWprintf ( 10, "REASSIGN: something big-time screwy.\n");

		// Now we put that task on the todo list and rematch
		// the tasks to workers, in case any are waiting around.
		pushTask( rt );
		// just in case...
		rematch_tasks_to_workers ( w );
		break;
		}

	case DEFAULT: 
		{
		//
		// We only take action if the todo list is empty.
		//
		if ( todo.empty() ) {
			// here there are no more tasks left to do, we want
			// to re-allocate this job.  First, find idle worker:
        	   typename list<MWWorkerID<CommType>*>::iterator curr = idle_workers.begin();
        	   typename list<MWWorkerID<CommType>*>::iterator last = idle_workers.end();
        	   while (curr != last) {
          	   if ( (*curr)->isIdle() && 
			(*curr)->getGroup().doesOverlap( &(w->runningtask->getGroup()) ) )
             	      MWprintf ( 60, "Reassigning task %d to %s.\n", 
					w->runningtask->number, (*curr)->machine_name() );
             	      assign_task_to_worker ( *curr, w->runningtask );
             	      break;
                      }
          	      curr++;
          	   }
		}
	};
    }
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::handle_hostresume () 
{
int who;
RMC->who ( &who );

MWWorkerID<CommType> *w = worker_find ( who );
if ( w != NULL ) {
   w->resumed();
   update_worker_state( w );
   MWprintf ( 40, "Got HOSTRESUME.  \"%s\" has resumed.\n", 
  							 w->machine_name() );
   if ( w->runningtask != NULL ) 
      MWprintf ( 60, " --> It's still running task %d.\n", 
							w->runningtask->number );
   else 
      MWprintf ( 60, " --> It's not running a task.\n" );
   }
else 
   MWprintf ( 40, "Got HOSTRESUME of worker %08x, but it isn't in "
					 "the worker list.\n", who );
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::handle_taskexit () 
{
//
// We usually get this message just before a HOSTDELETE, 
// so don't do anything like remove that host...
//

int who;
RMC->who ( &who );
MWWorkerID<CommType> *w = worker_find ( who, true );
if ( w == NULL ) {
   MWprintf ( 40, "Got TASKEXIT of worker %d, but it isn't in "
   					"the worker list.\n", who );
   return;
   }

MWprintf ( 40, "Got TASKEXIT from machine %s (%d).\n", 
			   w->machine_name(), w->get_id1());

hostPostmortem( w );
RMC->removeWorker ( w );
//
// We may need more hosts at this point
//
call_hostaddlogic();
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::handle_checksum ( )
{
//
// Some worker returned us a checksum error message
// Actually there may be some bootstrapping problem here
//
int who;
RMC->who ( &who );
MWWorkerID<CommType> *w = worker_find ( who );
MWprintf( 10, "Worker \" %s \" ( %d ) returned a checksum error!\n", 
						w->machine_name(), who );

if ( w == NULL ) {
   MWprintf ( 10, "Got CHECKSUM_ERROR from worker %08d, but it isn't in "
					"the worker list.\n", who );
   return;
   }

else {
   switch ( w->currentState ( ) ) {
	case BENCHMARKING:
		{
		int stat = RMC->initsend();
		if ( stat < 0 ) 
		   MWprintf( 10, "Error in initializing send in pack_worker_init_data\n");
		if ( pack_worker_init_data() != OK ) {
		   MWprintf ( 10, "pack_worker_init_data returned !OK.\n" );
		   w->printself(10);
		   w->started();
		   w->ended();
		   update_worker_state( w );
		   RMC->exec_class_num_workers(w->get_exec_class())--;
		   worker_find ( w->get_id1(), true );
		   RMC->removeWorker ( w );
		   return;
		   }

		int benchmarking = bench_task ? 1 : 0;

		RMC->pack( &benchmarking, 1 );
		if ( bench_task )
		   bench_task->pack_work();

		RMC->send( w->get_id1(), INIT_REPLY );
		// Note the fact that the worker is doing a benchmark.
		w->benchmark();
		update_worker_state( w );
		break;
		}

	case WORKING:
		{
		if ( w->runningtask ) {
		   //
		   // It was running a task
		   // See if this task is there in the done list
		   //
		   if ( w->runningtask->taskType == MWTask<CommType>::NORMAL )
		      pushTask ( w->runningtask );

		   send_task_to_worker ( w );
		   }
		else
		   // How is this possible that a worker is not working on something
		   MWprintf ( 10, "ERROR: Worker was not working on any task but still gave a CHECKSUM_ERROR\n");
		break;
		}
			
	default:
		MWprintf(30, "MWDriver<CommType>::handle_checksum() - other worker state.\n");
	}
   }
}


////--------------------------------------------------------
////
//// Protected Task Management
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::addTask(MWTask<CommType>* task)
{
task->number = task_counter++;
if ( numWorkGroups() <= 1 ) {
   task->initGroups ( 1 );
   task->addGroup ( 0 );
   }
pushTask ( task );
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::reassign_tasks_timedout_workers(bool& busy)
{
busy = false;
MWprintf ( 10, "Enter : reassign_tasks_timedout_workers()\n");

if (worker_timeout_limit <= 0)
   return;

struct timeval t;
gettimeofday ( &t, NULL );
double now = timeval_to_double ( t );
MWprintf( 60, "Now: %lf\n", now );

//
// Iterate through active workers looking for timedout workers
//
typename list<MWWorkerID<CommType>*>::iterator curr = active_workers.begin();
typename list<MWWorkerID<CommType>*>::iterator last = active_workers.end();
while (curr != last) {
  if ((*curr)->currentState() == WORKING)
     MWprintf( 50, "Last event from %s ( id: %d ) : %lf\n", 
				(*curr)->machine_name(), 
		  		(*curr)->get_id1(), (*curr)->get_last_event() );

  if ( (now - (*curr)->get_last_event() ) > worker_timeout_limit ) {
     //
     // this worker is timed out
     //
     if ((*curr)->runningtask) {
        busy=true;
	int task = (*curr)->runningtask->number;
	MWTask<CommType> *rmTask = remove_running_task( (*curr)->runningtask->number );
	if ( rmTask ) {
	   MWprintf ( 10, "Worker %d is timed-out\n",(*curr)->get_id1());
	   MWprintf ( 10, "Task %d rescheduled \n",rmTask->number);
	   }
	else
	   MWprintf ( 60, "Task not found in running list; assumed done or in todo list.\n" );
	if ( !rmTask || rmTask->taskType != MWTask<CommType>::NORMAL ) 
	   MWprintf ( 60, "Not Re-Assigning task %d.\n", task );
	else {
	   MWTask<CommType> *rt = (*curr)->runningtask;
	   if ( rt != rmTask ) 
	      MWprintf ( 10, "REASSIGN: something big-time screwy.\n");

	   /* Now we put that task on the todo list and rematch
		the tasks to workers, in case any are waiting around.*/
	   pushTask( rt );
	   // just in case...
	   rematch_tasks_to_workers ( *curr );
	   }
	}
     }
  curr++;
  }

//
// TODO: verify that the freed up tasks get put back in the todo list
//
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::pushTask( MWTask<CommType> *push_task ) 
{
//
// Put a task into the todo list in order
//
// This is just like addTask, except we don't want to 
// increment the number
//
    
if ( todo.empty() ) {
   todo.push_back(push_task);
   return;
   }

switch ( addmode ) {
  case ADD_AT_BEGIN: 
	todo.push_front(push_task);
	break;

  case ADD_AT_END: 
	todo.push_back(push_task);
	break;

#ifdef MW_SORTED_WORKERS

  case ADD_BY_KEY: 
	// This will insert at the first key WORSE than the 
	// given one
	if ( ! TaskLess ) {
	   MWprintf ( 10, "ERROR!  Adding by key, but no key "
	  						"function defined!\n" );
	   assert(0);
	   }
	todo.push_back(push_task);
        todo.sort(TaskLess);
        break;

#endif

  default: 
	MWprintf ( 10, "What the heck kinda addmode is %d?\n", addmode );
	assert( 0 );
  };
}




//============================================================================
//
//
template <class CommType>
MWTask<CommType>* MWDriver<CommType>::getNextTask( MWGroup& grp )  
{
typename list<MWTask<CommType>*>::iterator curr = todo.begin();
typename list<MWTask<CommType>*>::iterator last = todo.end();
while (curr != last) {
  if ((*curr)->status) {
     //
     // This task has been marked as finished
     // TODO: cleanup a task  ... and delete it?
     //
     delete *curr;
     }
  else {
     if ( grp.doesOverlap( &((*curr)->getGroup()) ) ) {
        MWTask<CommType>* t = *curr;
        todo.erase(curr);
	return t;
        }
     }
  curr++;
  }
return 0;
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::print_running_tasks() 
{
MWprintf ( 60, "PrintRunQ start:\n" );

typename map<int,MWTask<CommType>*>::const_iterator curr = running_tasks.begin();
typename map<int,MWTask<CommType>*>::const_iterator last = running_tasks.end();
while (curr != end) {
  if (*curr)
     (*curr)->printself();
  ++curr;
  }

MWprintf ( 60, "PrintRunQ end.\n\n" );
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::assign_task_to_worker ( MWWorkerID<CommType> *w, MWTask<CommType> *t )
{
t->worker = w;
w->gottask(t->number);
update_worker_state( w );
w->runningtask = t;
running_tasks[t->number] = t;

RMC->initsend();
int tmp = (int)(t->taskType);
RMC->pack ( &tmp, 1, 1 );
RMC->pack( &(t->number), 1, 1 );
pack_driver_task_data();
t->pack_work();
RMC->send( w->get_id1(), DO_THIS_WORK );
}


////--------------------------------------------------------
////
//// Protected Worker Management
////
////--------------------------------------------------------


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::worker_add( MWWorkerID<CommType> *w ) 
{
//
// Setup the worker's work-group(s)
//
w->initGroups ( numWorkGroups() );
setWorkGroup( w );

//
// Update the number of workers executing a given class of executables.
//
RMC->exec_class_num_workers(w->get_exec_class())++;

//
// Put a worker on the workers list.  At this point, no benchmark 
// information or machine information is known, so we don't reorder the list.
// 
idle_workers.push_back(w);
workers[w->get_id1()] = w;
}


//============================================================================
//
//
template <class CommType>
typename MWDriver<CommType>::MWReturn MWDriver<CommType>::worker_init( MWWorkerID<CommType> *w ) 
{
char workerhostname[64];
MWBase::MWReturn ustat = OK;

//
// We pack the hostname on the worker side...
//
RMC->unpack( workerhostname );
w->set_machine_name( workerhostname );
    
MWprintf ( 40, "The MWDriver recognizes the existence of worker \"%s\".\n",
					workerhostname );
w->started();
update_worker_state( w );
unpack_worker_initinfo( w );

w->get_machine_info();

int stat = RMC->initsend();
if ( stat < 0 ) {
   MWprintf( 10, "Error in initializing send in pack_worker_init_data\n");
   worker_terminate ( w );
   return OK;
   }

if ( RMC->pack ( getHostName() ) != 0 ) {
   MWprintf ( 10, "packing of hostName returned !OK.\n");
   worker_terminate ( w );
   return ustat;
   }

#ifdef NWSENABLED
RMC->pack ( &nws_nameserver_port, 1, 1 );
RMC->pack ( &nws_memory_port, 1, 1 );
RMC->pack ( &nws_sensor_port, 1, 1 );
#endif
if ( (ustat = pack_worker_init_data()) != OK ) {
   MWprintf ( 10, "pack_worker_init_data returned !OK.\n" );
   w->printself(10);
   worker_terminate ( w );
   return ustat;
   }

//
// Don't benchmark if this is the local worker.
//
if (!(local_worker && (w->get_id1() == local_worker->myid))) {
   int benchmarking = bench_task ? true : false;
   RMC->pack( &benchmarking, 1 );
   if ( bench_task ) 
      bench_task->pack_work();

   MWprintf ( 10, "Master sending an INIT_REPLY\n");
   RMC->send( w->get_id1(), INIT_REPLY );
   //
   // Note the fact that the worker is doing a benchmark. 
   //
   w->benchmark();
   update_worker_state( w );
   }
return ustat;
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::worker_terminate( MWWorkerID<CommType> *w )
{
w->ended();
update_worker_state( w );
#ifdef MWSTATS
stats->gather(w);
#endif
RMC->removeWorker ( w );
RMC->exec_class_num_workers(w->get_exec_class())--;

for ( int i = 0; i < numWorkGroups(); i++ ) {
  if ( w->doesBelong ( i ) )
     w->deleteGroup ( i );
  }

if (!w->runningtask) {
   typename list<MWWorkerID<CommType>*>::iterator curr = idle_workers.begin();
   typename list<MWWorkerID<CommType>*>::iterator last = idle_workers.end();
   while (curr != last) {
     if ((*curr)->get_id1() == w->get_id1()) {
        idle_workers.erase(curr);
	break;
	}
     ++curr;
     }
   if (curr == last) {
      MWprintf ( 10, "worker_terminate -- ERROR: idle worker not in list of idle processors!\n");
      abort();
      }
   }

//
// Erase the worker from the worker_map object
//
worker_find( w->get_id1(), true );
//
// Delete the WorkerID object itself
//
delete w;
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::reorder_workers()
{
if (machine_ordering_policy == NO_ORDER)
   return;

#ifdef MW_SORTED_WORKERS
if (! WorkerIDLess ) {
   MWprintf( 10, "MWDriver::reorder_workers - WARNING: cannot reorder because the WorkerIDLess class is empty.\n");
   return;
   }

idle_workers.sort(WorkerIDLess);
#endif
}


//============================================================================
//
//
template <class CommType>
void MWDriver<CommType>::update_worker_state(MWWorkerID<CommType>* w)
{
typename map<int,MWWorkerState<CommType>*>::iterator iter;
iter = worker_state.find(w->get_id1());

bool init_flag=false;
if (iter == worker_state.end()) {
   worker_state[w->get_id1()] = new MWWorkerState<CommType>;
   iter = worker_state.find(w->get_id1());
   iter->second->sflag=true;
   init_flag=true;
   }
   
if ((w->currentState() == EXITED) || (w->currentState() == IDLE)){
   if (iter->second->sflag) {
      if (!init_flag) {
         active_workers.erase(iter->second->iter);
         MWprintf(50,"Worker %d is being removed from the active list.\n",
						w->get_id1());
         }
      iter->second->sflag=false;
      iter->second->iter = idle_workers.insert(idle_workers.end(),w);
      MWprintf(50,"Worker %d is being added to the idle list.\n",
						w->get_id1());
     }
   }

else if (!iter->second->sflag) {
   if (!init_flag) {
      idle_workers.erase(iter->second->iter);
      MWprintf(50,"Worker %d is being removed from the idel list.\n",
						w->get_id1());
      }
   iter->second->sflag=true;
   iter->second->iter = active_workers.insert(active_workers.end(),w);
   MWprintf(50,"Worker %d is being added to the active list.\n",
						w->get_id1());
   }
}



#ifdef OLD_CODE
int
MWDriver<CommType>::refreshWorkers ( int i, MWREFRESH_TYPE type )
{
	// send only to idle workers
	MWWorkerID<CommType> *so = MWcurrentWorker;
	int j;

	if ( type == MW_NONE )
		return 0;
	if ( type == MW_THIS )
	{
		for ( j = 0; j < numWorkGroups(); j++ )
		{
			if ( MWcurrentWorker->doesBelong ( j ) )
				MWcurrentWorker->deleteGroup ( j );
		}
		RMC->initsend ( );
		pack_worker_init_data ( );
		RMC->send ( MWcurrentWorker->get_id1(), REFRESH );
		return 0;
	}

	int num = 0;
	int max = workers->number();
	MWWorkerID<CommType> *w;

	while ( num < max )
	{
		w = (MWWorkerID<CommType> *)workers->Remove();
		if ( w != MWcurrentWorker && w->currentState() != IDLE )
			workers->Append ( w );
		else if ( !w->doesBelong ( i ) )
			workers->Append ( w );
		else
		{
			// You have to remove them from the list.
			for ( j = 0; j < numWorkGroups(); j++ )
			{
				if ( w->doesBelong ( j ) )
					w->deleteGroup ( j );
			}
			RMC->initsend();
			MWcurrentWorker = w;
			pack_worker_init_data ( );
			RMC->send ( w->get_id1(), REFRESH );
			workers->Append ( w );
		}
		num++;
	}
	MWcurrentWorker = so;
	return 0;
}


/* 8/28/00

   Qun added Jeff's idea of adding a SORTED list of tasks.
   This can greatly improve the efficiency in many applications.
   
   XXX I haven't debugged this, and probably there needs to be more
   checking that the list is actually sorted.  
 */

/* added by Qun: for insert a list of sorted tasks */

void 
MWDriver<CommType>::addSortedTasks( int n, MWTask **add_tasks )
{
	if ( n <= 0 ) {
		MWprintf ( 10, "Please add a positive number of tasks!\n" );
		RMC->exit(1);
	}

	for ( int i = 0; i < n; i++ )
	{
		add_tasks[i]->number = task_counter++;
		if ( numWorkGroups() <= 1 )
		{
			add_tasks[i]->initGroups ( numWorkGroups() );
			add_tasks[i]->addGroup ( 0 );
		}
		pushTask ( add_tasks[i] );
	}
}
#endif



#if defined( XML_OUTPUT )

double 
MWDriver<CommType>::get_instant_pool_perf( ) 
{
	double total_perf = 0;
	int total = 0;
	MWWorkerID<CommType> *w;

	while ( total < workers->number() ) 
	{
		total++;
		w = (MWWorkerID<CommType> *)workers->Remove();
		if ( (w->currentState() == WORKING) && (w->runningtask != NULL) ) 
		{
			total_perf += w->get_bench_result();
		}
		workers->Append ( w );
	}
	return total_perf;
}


void MWDriver<CommType>::write_XML_status()
{
  ofstream xmlfile("/u/m/e/metaneos/public/html/iMW/status.xml",
		   ios::trunc|ios::out);


  if( ! xmlfile ) {
    cerr << "Cannot open 'status.xml data file' file!\n";
  }

  //system("/bin/rm -f ~/public/html/iMW/status.xml");
  //system("/bin/mv -f ./status.xml ~/public/html/iMW/");   

  // system("/bin/more status.xml");

  char *temp_ptr ;
  temp_ptr = get_XML_status();

  xmlfile << temp_ptr;
  delete temp_ptr;

  //  xmlfile << ends;

  xmlfile.close();

  //system("/bin/rm -f ~/public/html/iMW/status.xml");
  //system("/bin/cp -f status.xml /u/m/e/metaneos/public/html/iMW/");
  // system("/bin/cp -f status.xml ~metaneos/public/html/iMW/");
}

#include <strstream.h>

char* MWDriver<CommType>::get_XML_status(){

  ifstream menus(xml_menus_filename);

  ostrstream xmlstr;

  xmlstr << "<?xml version=\"1.0\"?>" << endl;
  xmlstr << "<?xml:stylesheet type=\"text/xsl\" href=\"menus-QAP.xsl\"?>" << endl;
  xmlstr << "<INTERFACE TYPE=\"iMW-QAP\">" << endl;

  // xmlstr << menus;

  char ch;
  while (menus.get(ch)) xmlstr.put(ch);

  // menus >> xmlstr ;

  xmlstr << "<MWOutput TYPE=\"QAP\">" << endl;

  char *temp_ptr ;

  temp_ptr = get_XML_job_information();

  xmlstr << temp_ptr;
  delete temp_ptr;

  temp_ptr = get_XML_problem_description();

  xmlstr << temp_ptr;
  delete temp_ptr;

  temp_ptr = get_XML_interface_remote_files();

  xmlstr << temp_ptr;
  delete temp_ptr;

  temp_ptr = get_XML_resources_status();

  xmlstr << temp_ptr;
  delete temp_ptr;

  temp_ptr = get_XML_results_status();

  xmlstr << temp_ptr;
  delete temp_ptr;

  xmlstr << "</MWOutput>" << endl;
  xmlstr << "</INTERFACE>" << endl;

 xmlstr << ends;



  menus.close();


  return xmlstr.str();

}

char* MWDriver<CommType>::get_XML_job_information()
{

  ifstream jobinfo(xml_jobinfo_filename);

  ostrstream xmlstr;

  char ch;
  while (jobinfo.get(ch)) xmlstr.put(ch);

  xmlstr << ends;

  jobinfo.close();
 

  return xmlstr.str();

};

char* MWDriver<CommType>::get_XML_problem_description(){

  ifstream pbdescrib(xml_pbdescrib_filename);

  ostrstream xmlstr;

  char ch;
  while (pbdescrib.get(ch)) xmlstr.put(ch);

  xmlstr << ends;

  pbdescrib.close();


  return xmlstr.str();

};

char* MWDriver<CommType>::get_XML_interface_remote_files(){

  ostrstream xmlstr;

  xmlstr << "<InterfaceRemoteFiles>" << endl;

  // dump here content of file

  xmlstr << "</InterfaceRemoteFiles>" << endl;

  xmlstr << ends;

  return xmlstr.str();

}


char* MWDriver<CommType>::get_XML_resources_status()
{

  int total;
  int i;
  ostrstream xmlstr;

  // Begin XML string

  xmlstr << "<ResourcesStatus>" << endl;

  double average_bench;
  double equivalent_bench;
  double min_bench;
  double max_bench;
  double av_present_workers;
  double av_nonsusp_workers;
  double av_active_workers;
  double equi_pool_performance;
  double equi_run_time;
  double parallel_performance;
  double wall_time;

  stats->get_stats(&average_bench,
		  &equivalent_bench,
		  &min_bench,
		  &max_bench,
		  &av_present_workers,
		  &av_nonsusp_workers,
		  &av_active_workers,
		  &equi_pool_performance,
		  &equi_run_time,
		  &parallel_performance,
		  &wall_time,
		  workers
		  );

  // MWStats Information

  xmlstr << "<MWStats>" << endl;

  xmlstr << "<WallClockTime>" << wall_time << "</WallClockTime>" << endl;
  xmlstr << "<NormalizedTotalCPU>" << equi_run_time << "</NormalizedTotalCPU>" << endl;
  xmlstr << "<ParallelEff>" <<  parallel_performance << "</ParallelEff>" << endl;

  xmlstr << "<BenchInfo>" << endl;
     xmlstr << "<InstantPoolPerf>" << get_instant_pool_perf() << "</InstantPoolPerf>" << endl;
     xmlstr << "<EquivalentPoolPerf>" << equi_pool_performance << "</EquivalentPoolPerf>" << endl;
     xmlstr << "<AverageBench>" << average_bench << "</AverageBench>" << endl;
     xmlstr << "<EquivalentBench>" << equivalent_bench << "</EquivalentBench>" << endl;
     xmlstr << "<MinBench>" << min_bench << "</MinBench>" << endl;
     xmlstr << "<MaxBench>" << max_bench << "</MaxBench>" << endl;
  xmlstr << "</BenchInfo>" << endl;

  xmlstr << "<AverageWorkersStats>" << endl;
     xmlstr << "<AveragePresentWorkers>" << av_present_workers << "</AveragePresentWorkers>" << endl;
     xmlstr << "<AverageNonSuspWorkers>" << av_nonsusp_workers << "</AverageNonSuspWorkers>" << endl;
     xmlstr << "<AverageActiveWorkers>"  << av_nonsusp_workers << "</AverageActiveWorkers>" << endl;
  xmlstr << "</AverageWorkersStats>" << endl;


  xmlstr << "</MWStats>" << endl;

     // Master Information

     xmlstr << "<Master>" << endl;

     xmlstr << "<MasterPhysicalProperties>" << endl;
     xmlstr << "<Name>" << mach_name << "</Name>" << endl;
     //     xmlstr << "<IPAddress>" << get_IPAddress() << "</IPAddress>" << endl;
     xmlstr << "<OpSys>" << get_OpSys() << "</OpSys>" << endl;
     xmlstr << "<Arch>" << get_Arch() <<"</Arch>" << endl;
     xmlstr << "<Memory>" << get_Memory() << "</Memory>" << endl;
     xmlstr << "<VirtualMemory>" << get_VirtualMemory() << "</VirtualMemory>" <<  endl; 
     xmlstr << "<DiskSpace>" << get_Disk() << "</DiskSpace>" << endl;
     xmlstr << "<KFlops>" << get_KFlops() << "</KFlops>" << endl;
     xmlstr << "<Mips>" << get_Mips() << "</Mips>" << endl;
     xmlstr << "<CPUs>" << get_Cpus() << "</CPUs>" << endl;
     xmlstr << "<NWSinfos/>" << endl;
     xmlstr << "</MasterPhysicalProperties>" << endl;
                       


     xmlstr << "<MasterUsageProperties>" << endl;
     xmlstr << "<StartTime>July 1st 1999, 18:21:12s GMT</StartTime>"  << endl;
     xmlstr << "</MasterUsageProperties>" << endl;

     xmlstr << "</Master>" << endl;

     // Worker Information

     xmlstr << "<Workers>" << endl;

     int numworkers = numWorkersInState( INITIALIZING ) +  numWorkersInState( BENCHMARKING ) + numWorkersInState( IDLE ) + numWorkersInState( WORKING ) ;

     xmlstr << "<WorkersNumber>" << numworkers << "</WorkersNumber>" << endl;

     xmlstr << "<WorkersStats>" << endl;

     xmlstr << "<WorkersInitializing>" << numWorkersInState( INITIALIZING ) << "</WorkersInitializing>" << endl;
     xmlstr << "<WorkersBenchMarking>" << numWorkersInState( BENCHMARKING ) << "</WorkersBenchMarking>" << endl;
     xmlstr << "<WorkersWaiting>" << numWorkersInState( IDLE ) << "</WorkersWaiting>" << endl;
     xmlstr << "<WorkersWorking>" << numWorkersInState( WORKING ) << "</WorkersWorking>" << endl;
     xmlstr << "<WorkersSuspended>" << numWorkersInState( SUSPENDED ) << "</WorkersSuspended>" << endl;
     xmlstr << "<WorkersDone>" << numWorkersInState( EXITED ) << "</WorkersDone>" << endl;

     xmlstr << "</WorkersStats>" << endl;

     xmlstr << "<WorkersList>" << endl;

	 total = 0;
     MWWorkerID<CommType> *w;

     while ( total < workers->number() ) 
	 {
		 total++;
		 w = (MWWorkerID<CommType> *)workers->Remove();

       if ((w->currentState() != SUSPENDED) || (w->currentState() != EXITED))
	   {
		         
       xmlstr << "<Worker>" << endl;

       xmlstr << "<WorkerPhysicalProperties>" << endl;

          xmlstr << "<Name>" << w->machine_name() << "</Name>" << endl;
	  //          xmlstr << "<IPAddress>" << w->get_IPAddress() << "</IPAddress>" << endl; 
          xmlstr << "<Status>" << MWworker_statenames[w->currentState()] << "</Status>" << endl; 
	  xmlstr << "<OpSys>" << w->OpSys << "</OpSys>" << endl; 
	  xmlstr << "<Arch>" <<  w->Arch << "</Arch>" << endl; 
	  xmlstr << "<Bandwidth>" << "N/A" << "</Bandwidth>" << endl; 
	  xmlstr << "<Latency>" << "N/A" << "</Latency>" << endl; 
	  xmlstr << "<Memory>" << w->Memory << "</Memory>" <<  endl; 
	  xmlstr << "<VirtualMemory>" << w->VirtualMemory << "</VirtualMemory>" <<  endl; 
	  xmlstr << "<DiskSpace>" << w->Disk << "</DiskSpace>" << endl; 
	  xmlstr << "<BenchResult>" << w->get_bench_result() <<"</BenchResult>" << endl;
	  xmlstr << "<KFlops>" << w->KFlops <<"</KFlops>" << endl;
	  xmlstr << "<Mips>" << w->Mips <<"</Mips>" << endl;   
	  xmlstr << "<CPUs>" << w->Cpus <<"</CPUs>" << endl;
	  xmlstr << "<NWSinfos>" << "N/A" << "</NWSinfos>" << endl; 

       xmlstr << "</WorkerPhysicalProperties>" << endl;

       xmlstr << "<WorkerUsageProperties>" << endl;

          xmlstr << "<TotalTime>" << "N/A" << "</TotalTime>" << endl;
          xmlstr << "<TotalWorking>" << w->get_total_working() << "</TotalWorking>" << endl;
          xmlstr << "<TotalSuspended>" << w->get_total_suspended() << "</TotalSuspended>" << endl;

       xmlstr << "</WorkerUsageProperties>" << endl;

       xmlstr << "</Worker>" << endl;
       }
       workers->Append ( w );
     }

     xmlstr << "</WorkersList>" << endl;

     xmlstr << "</Workers>" << endl;

     // Global Worker Statistics

      xmlstr << "<GlobalStats>" << endl;

      

      xmlstr << "</GlobalStats>" << endl;

     // Task Pool Information


     xmlstr << "<TaskPoolInfos>" << endl;

     int nrt = get_number_running_tasks();
     int tt = get_number_tasks();
     int tdt = tt - nrt;

     xmlstr << "<TotalTasks>" << tt << "</TotalTasks>" << endl;
     xmlstr << "<TodoTasks>" << tdt << "</TodoTasks>" << endl;
     xmlstr << "<RunningTasks>" << nrt << "</RunningTasks>" << endl;
     xmlstr << "<NumberCompletedTasks>" << num_completed_tasks << "</NumberCompletedTasks>" << endl;
     //xmlstr << "<MaxNumberTasks>" << max_number_tasks << "</MaxNumberTasks>" << endl;
     xmlstr << "</TaskPoolInfos>" << endl;

     // Memory Info

     // Secondary storage info


  // End  XML string

  xmlstr << "</ResourcesStatus>" << endl;
  xmlstr << ends ;

  return xmlstr.str();

}

char* MWDriver<CommType>::get_XML_results_status(){

  ostrstream xmlstr;

  xmlstr << "<ResultsStatus>" << endl;
  xmlstr << "</ResultsStatus>" << endl;

  xmlstr << ends;

  return xmlstr.str();

}

/* The following (until the end of the file) was written by one of 
   Jean-Pierre's students.  It isn't exactly efficient, dumping
   a condor_status output to a file and reading from that.  I understand
   that it does work, however.  -MEY (8-18-99) */

int MWDriver<CommType>::check_for_int_val(char* name, char* key, char* value) {
	if (strcmp(name, key) == 0)
		return atoi(value);
	else return NO_VAL;
}

double MWDriver<CommType>::check_for_float_val(char* name, char* key, char* value) {
	if (strcmp(name, key) == 0)
		return atof(value);
	else return NO_VAL;  
}

int  MWDriver<CommType>::check_for_string_val(char* name, char* key, char* value) {
	if (strcmp(name, key) == 0)
		return 0;
	else return NO_VAL;  
}


void MWDriver<CommType>::get_machine_info()
{
  FILE* inputfile;
  char filename[50];
  char key[200];
  char value[1300];
  char raw_line[1500];
  char* equal_pos;
  int found;
  char temp_str[256];

  char zero_string[2];

  memset(zero_string, 0 , sizeof(zero_string));

  memset(filename, '\0', sizeof(filename));
  memset(key, '\0', sizeof(key));
  memset(value, '\0', sizeof(value));

  strcpy(filename, "/tmp/metaneos_file2");

  memset(temp_str, '\0', sizeof(temp_str));
  sprintf(temp_str, "/unsup/condor/bin/condor_status -l %s > %s", mach_name, filename);

  if (system(temp_str) < 0)
    {

      MWprintf( 10, "Error occurred during attempt to get condor_status for %s.\n",  mach_name );
      return;
    }

  if ((inputfile = fopen(filename, "r")) == 0)
    {
      MWprintf( 10, "Failed to open condor_status file!\n");
      return;
    }
  else
    {
      MWprintf( 90, "Successfully opened condor_status file.\n");      
    }

  while (fgets(raw_line, 1500, inputfile) != 0)
    {
      found = 0;
      equal_pos = strchr(raw_line, '=');

      if (equal_pos != NULL)
	{
	  strncpy(key, raw_line, equal_pos - (raw_line+1));
	  strcpy(value, equal_pos+2);

	  if (CondorLoadAvg == NO_VAL && !found)
	    {
	      CondorLoadAvg = check_for_float_val("CondorLoadAvg", key, value);
	      if (CondorLoadAvg != NO_VAL)
		found = 1;
	    }

	  if (LoadAvg == NO_VAL && !found)
	    {
	      LoadAvg = check_for_float_val("LoadAvg", key, value);
	      if (LoadAvg != NO_VAL)
		found = 1;
	    }

	  if (Memory == NO_VAL && !found)
	    {
	      Memory = check_for_int_val("Memory", key, value);
	      if (Memory != NO_VAL)
		found = 1;
	    }

	  if (Cpus == NO_VAL && !found)
	    {
	      Cpus = check_for_int_val("Cpus", key, value);
	      if (Cpus != NO_VAL)
		found = 1;
	    }

	  if (VirtualMemory == NO_VAL && !found)
	    {
	      VirtualMemory = check_for_int_val("VirtualMemory", key, value);
	      if (VirtualMemory != NO_VAL)
		found = 1;
	    }

	  if (Disk == NO_VAL && !found)
	    {
	      Disk = check_for_int_val("Disk", key, value);
	      if (Disk != NO_VAL)
		found = 1;
	    }

	  if (KFlops == NO_VAL && !found)
	    {
	      KFlops = check_for_int_val("KFlops", key, value);
	      if (KFlops != NO_VAL)
		found = 1;
	    }

	  if (Mips == NO_VAL && !found)
	    {
	      Mips = check_for_int_val("Mips", key, value);
	      if (Mips != NO_VAL)
		found = 1;
	    }

	  
	  if ( (strncmp(Arch, zero_string, 1) == 0) && !found)
	    {	     
	      if (check_for_string_val("Arch", key, value) == 0){
	       strncpy( Arch, value, sizeof(Arch) );	       
	      }
	     
	     if (strncmp(Arch, zero_string, 1) != 0)
	       found = 1;
	    }

	  if ( (strncmp(OpSys, zero_string, 1) == 0) && !found)
	    {	     
	      if (check_for_string_val("OpSys", key, value) == 0){
	       strncpy( OpSys, value, sizeof(OpSys) );	       
	      }
	     
	     if (strncmp(OpSys, zero_string, 1) != 0)
	       found = 1;
	    }

	  if ( (strncmp(IPAddress, zero_string, 1) == 0) && !found)
	    {	     
	      if (check_for_string_val("StartdIpAddr", key, value) == 0){
	       strncpy(IPAddress , value, sizeof(IPAddress) );	       
	      }
	     
	     if (strncmp(IPAddress, zero_string, 1) != 0)
	       found = 1;
	    }


	  memset(key, '\0', sizeof(key));
	  memset(value, '\0', sizeof(value));

	}

    }

  MWprintf(90,"CURRENT MACHINE  : %s \n", mach_name);
  MWprintf(90,"Architecture : %s \n", Arch);
  MWprintf(90,"Operating System : %s \n", OpSys);
  MWprintf(90,"IP address : %s \n", IPAddress);

  MWprintf(90,"CondorLoadAvg : %f\n", CondorLoadAvg);
  MWprintf(90,"LoadAvg : %f\n", LoadAvg);
  MWprintf(90,"Memory : %d\n", Memory);
  MWprintf(90,"Cpus : %d\n", Cpus);
  MWprintf(90,"VirtualMemory : %d\n", VirtualMemory);
  MWprintf(90,"Disk : %d\n", Disk);
  MWprintf(90,"KFlops : %d\n", KFlops);
  MWprintf(90,"Mips : %d\n", Mips);

  fclose( inputfile );

  if (remove(filename) != 0)
    {
       MWprintf(10,"Condor status file NOT removed!\n");
     }
   else
     {
       MWprintf(90,"Condor status file removed.\n");
     }


}
#endif

#ifdef NWSENABLED
void MWDriver<CommType>::do_nws_measurements ( )
{
	FILE *fp = NULL;
	int n = 1;
	char host[_POSIX_PATH_MAX];
	int port;
	char temp1[_POSIX_PATH_MAX], temp2[_POSIX_PATH_MAX], temp3[_POSIX_PATH_MAX];
	char *tmp;
	double latency = 0;
	char *workerString = new char[ ( getNumWorkers() + 1 ) * _POSIX_PATH_MAX ];

	int total = 0;
    MWWorkerID<CommType> *w;

	strcpy ( workerString, getenv ("MW_NWS_PATH") );
	strcat ( workerString, "/nws_ping" );
	
    while ( total < workers->number() ) 
	{
		total++;
		w = (MWWorkerID<CommType> *)workers->Remove();
		if ( strcspn ( (const char *)w->machine_name(), " " ) > 0 )
		{
			strcat ( workerString, " " );
			strcat ( workerString, w->machine_name() );
			sprintf ( temp3, ":%d", nws_sensor_port );
			strcat ( workerString, temp3 );
		}
        workers->Append ( w );
	}
	MWprintf ( 10, "The string is %s\n", workerString );
	fp = popen ( workerString, "r" );
	if ( fp == NULL )
	{
		MWprintf (10, "Could not fork the nws_ping. \n");
	}
	else
	{
		n = 1;
		while ( n != EOF || n != 0 )
		{
			n = fscanf ( fp, "%s %s %s", temp1, temp2, temp3 );
			if ( n == EOF || n == 0 ) break;
			n = 0;
			tmp = strstr ( temp3, ":" );
			while ( &temp3[n] != &tmp[0] )
			{
				host[n] = temp3[n++];
			}
			host[n] = '\0';
			n = fscanf ( fp, "%s", temp1 );
			if ( n == EOF || n == 0 ) break;
			if ( strcmp ( temp1, "failure" ) == 0 ) continue;
			n = fscanf ( fp, "%lf %s %s", &latency, temp2, temp3 );
			if ( n == EOF || n == 0 ) break;
			n = fscanf ( fp, "%lf %s", &latency, temp1 );
			if ( n == EOF || n == 0 ) break;
			lookupWorkerAndsetConnectivity ( host, latency );
		}
		pclose(fp);
	}
	delete workerString;

	next_nws_measurement_check = time(0) + nws_measurement_period;
}

void MWDriver<CommType>::start_nws_daemons ( )
{
	int i;
	char temp1[8 * _POSIX_PATH_MAX];
	char temp2[8 * _POSIX_PATH_MAX];
	char temp[_POSIX_PATH_MAX];

	sprintf ( temp1, "%s/%s", getenv ( "MW_NWS_PATH" ), "nws_nameserver" );
	strcat ( temp1, " -p" );
	sprintf ( temp, " %d", nws_nameserver_port );
	strcat ( temp1, temp );
	strcat ( temp1, " -l /dev/null" );
	strcat ( temp1, " & " );
	if ( system ( temp1 ) < 0 )
	{
		MWprintf ( 10, "fork of nameserver failed %d\n", errno );
		return;
	}

	sprintf ( temp2, "%s/%s", getenv ( "MW_NWS_PATH" ), "nws_memory" );
	strcat ( temp2, " -N " );

	/* Changed into getHostName(char *) */
	char hostName[80]; getHostName(hostName);
	strcat ( temp2, hostName );
	sprintf ( temp, ":%d", nws_nameserver_port );
	strcat ( temp2, temp );
	strcat ( temp2, " -p " );
	sprintf ( temp, " %d", nws_memory_port );
	strcat ( temp2, temp );
	strcat ( temp2, " -l /dev/null" );
	strcat ( temp2, " & " );
	if ( system ( temp2 ) < 0 )
	{
		MWprintf ( 10, "fork of nws_memory failed %d\n", errno );
		return;
	}

	return;
}

void
MWDriver<CommType>::lookupWorkerAndsetConnectivity ( char *host, double latency )
{
	int total = 0;
	MWWorkerID<CommType> *w;
	MWprintf ( 10, "Setting the connectivity of %s to %lf\n", host, latency );

	while ( total < workers->number() )
	{
		total++;
		w = (MWWorkerID<CommType> *)workers->Remove();
		workers->Append ( w );
		if ( strcmp ( w->machine_name() == host ) )
			w->setNetworkConnectivity ( latency );
	}
}
#endif

#ifdef MWSTATS
template <class CommType>
void MWDriver<CommType>::collect_statistics()
{
//   Since terminate_workers can take a long time --we 
//   collect the statistics beforehand.  Therefore,
//   we gather all the stats from the currently working workers,
//   then call makestats here (before terminate_workers).
//
map<int,MWWorkerID<CommType>*>::iterator curr = workers.begin();
map<int,MWWorkerID<CommType>*>::iterator last = workers.end();
while (curr != last) {
  stats->gather(curr->second);
  ++curr;
  }

//
// Tell the stats class to do its magic
//
stats->makestats();
}
#endif

#endif
