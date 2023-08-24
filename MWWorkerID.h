//
// MWWorkerID.h
//

#ifndef __MWWorkerID_h
#define __MWWorkerID_h

#include <list>
#include <cstring>
#include <ctime>
#include <sys/time.h>
#include <sys/types.h>
//#include <errno.h>
//#include <stdio.h>
//#include <stdarg.h>
//#include <memory.h>

#include "MW.h"
#include "MWAbstractDriver.h"
#include "MWGroup.h"

//
// Forward declaration
//
template <class TaskType>
class MWTask;

template <class CommType, class Worker>
class MWStatistics;


/**
 * This class is used by MWDriver to keep tabs on a worker application.
 * It keeps statistical information that is useful to the MWstats class.  
 * This information is used at the end of a run.
 */
template <class CommType>
class MWWorkerID : public MWBase {

  /// Allow access to worker's data
  friend class MWStatistics<MWWorkerID<CommType>,CommType>;

public:

  /** This static array is used to hold strings that represent the
    MWworker_states.  They are handy for printing out a worker - now
    you'll see "WORKING" instead of "2" in printself().
    @see MWworker_states */
  static const char* MWworker_statenames[];

  /// Default constructor
  MWWorkerID(MWAbstractDriver<CommType>* master);
    
  /// Default destructor
  virtual ~MWWorkerID() {}
    
  /// Return primary id of this worker
  int get_id1() { return id1; };

  /// Return secondary id of this worker
  int get_id2() { return id2; };

  /** Return the virtual id.  This is an int starting at zero and
	increasing by one with each worker.  When a worker dies, this
	number can be reused by a worker that joins us. */
  int get_vid() { return virtual_id; };

  /// Set primary id
  void set_id1( int i ) { id1 = i; };

  /// Set secondary id
  void set_id2( int i ) { id2 = i; };

  /// Set the machine's name
  void set_machine_name( char* name)
    		{strncpy( mach_name, name, 64 );}

  /// get the current machine information
  virtual void get_machine_info()
		{}

  /// Returns a pointer to the machine's name
  const char *machine_name() const
		{return mach_name;}
    
  /// Set this worker's arch class
  void set_arch ( int a ) { arch = a; };

  /// Get this worker's arch class
  int get_arch() const { return arch; };

  /// Sets the exec_class
  void set_exec_class ( int num ) { exec_class = num; };

  /// Gets the exec_class
  int get_exec_class ( ) const { return exec_class; };

  /// Sets the executable
  void set_executable ( char *exec ) { executable_name = exec; };

  /// returns the executable name
  char* get_executable ( ) const { return executable_name; };

  /// Mark this worker so that when the task completes it will exit
  void mark_for_removal() { doomed = true; };

  /// returns true if mark_for_removal was called previously
  bool is_doomed() { return doomed; };

  /// 
  void set_bench_result( double bres );

  ///
  double get_bench_result() { return bench_result; };

  /// 
  double getNetworkConnectivity ( double &t );

  //
  void setNetworkConnectivity ( double );

  /// The task running on this worker.  NULL if there isn't one.
  MWTask<CommType> *runningtask;
    
  /// The state of the worker.
  enum MWworker_states currentState()
		{return state;}

  /// Set the state of the worker
  void setState ( MWworker_states st )
		{ state = st; };
    
  /** Print out a description of this worker.
	@param level The debug level to use */
  virtual void printself( int level = 40 ) const; 

  /**@name Statistics Collection Calls
       These methods are called when events happen to workers.
  */
  //@{
  /// Resets the statistics
  void reset_stats()
		{
		total_time = 0.0;
		total_working = 0.0;
		total_suspended = 0.0;
		cpu_while_working = 0.0;
		normalized_cpu_working_time = 0.0;
		sum_benchmark = 0.0;
		num_benchmark = 0;
		}

  /// Returns true if this worker is idle.
  bool isIdle() { return (state==IDLE); }

  /// Returns true if this worker is suspended.
  bool isSusp() { return (state==SUSPENDED); }

  /** This should be called when the master becomes aware of the 
      existence of this worker. */
  void started();

  /// Called when the worker is doing the benchmarking task
  void benchmark();

  /// Called when the worker has done the benchmarking task
  void benchmarkOver();

  /// Called when this worker gets a task.
  void gottask( int tasknum );

  /// Called when this worker just finished the task it was working on.
  void completedtask( double wall_time = 0.0, double cpu_time = 0.0 );

  /// Called when the worker becomes suspended
  void suspended();

  /// Called when the worker resumes.
  void resumed();

  /// Called when the worker dies.
  void ended();

  ///
  double get_total_time()
		{ return total_time;};
  ///
  double get_last_event()	
		{return  last_event;};
  ///
  double get_total_suspended()
		{return total_suspended;};
  ///
  double get_total_working()
		{return total_working;};
  //@}

  ///
  void initGroups ( int num )
		{group.init(num);}

  ///
  void addGroup ( int num );

  ///
  void deleteGroup ( int num );

  ///
  bool doesBelong ( int num )
		{return group.belong(num);}

  ///
  MWGroup& getGroup ( )
		{return group;}


  /**@name Checkpointing Call

	   Each instance of a MWWorkerID needs to be able to checkpoint
	   itself.  It has to store statistics information on itself.
	   It never has to read them in, though, because stats that we write
	   out here wind up being read in by the stats class when we restart.
  */
  //@{
  /// Return the relevant stats info for this worker.
  void ckpt_stats( double *up, 
			 double *working, 
			 double *susp, 
			 double *cpu, 
			 double *norm, 
			 double* s_bench, 
			 int* n_bench );
  //@}
	
protected:

  /** @name Protected Data */
  //@{
  /// The machine name of the worker
  char mach_name[64];

  /// A "primary" identifier of this machine.  (Was pvmd_tid)
  int id1;
    
  /// A "secondary" identifier of this machine.  (Was user_tid)
  int id2;
    
  /** A "virtual" number for this worker.  Assigned starting at 
	0 and working upwards by one.  Also, when a worker dies, 
	this number can get taken over by another worker */
  int virtual_id;

  /// This worker's arch class
  int arch;   // (should be just a 0, 1, 2, etc...)

  /// This is the exec_class
  int exec_class;

  /// This is the executable_name
  char *executable_name;

  /// true if marked for removal, false otherwise	
  bool doomed;  

  /// The results of the benchmarking process.
  double bench_result;

  /// The state of this worker.
  enum MWworker_states state;
  //@}


  /** @name Time information 
	    Note that all time information is really stored as a double.
	    We use gettimeofday() internally, and convert the result to 
	    a double.  That way, we don't have to mess around with the
	    struct timeval... */
  //@{
  /// The time that this worker started.
  double start_time;
	
  /// The total time that this worker ran for.
  double total_time;

  /// The time of the last 'event'.
  double last_event;
	
  /// The time spent in the suspended state
  double total_suspended;

  /// The time spent working
  double total_working;

  /// The cpu usage while working
  double cpu_while_working;

  /// The benchmarked (weighted) cpu working time
  double benchmarked_cpu_working_time;

  /// The benchmarked (normalized) cpu working time
  double normalized_cpu_working_time;

  /// The sum of the benchmark values for that vid
  double sum_benchmark;

  /// The number of the benchmark values for that vid
  int num_benchmark;
  //@}

  ///
  bool isBenchMark;
  ///
  bool isBenchMarkAvailable;

  /** @name Virtual Id Helpers */
  //@{
  /// Set virtual id
  void set_vid( int i )
		{ virtual_id = i; };

  /// Returns the next available virtual id
  int get_next_vid()
		{
		if (misc_vid_nums.empty())
		   return max_vid_num++;
		int tmp = *(misc_vid_nums.begin());
		misc_vid_nums.pop_back();
		return tmp;
		}

  /// Returns a virtual id to the pool
  void release_vid( int vid )
		{ misc_vid_nums.push_back(vid); }

  ///
  static int max_vid_num;

  ///
  static list<int> misc_vid_nums;
  //@}

  /// Variables regarding the nw bandwidth.
  double networkLatency;

  ///
  double networkLatency_lastMeasuredTime;

  /// The tasks that this worker can work on.
  MWGroup group;

  /// Pointer to an abstract MWDriver
  MWAbstractDriver<CommType>* master;
};


template <class CommType>
const char* MWWorkerID<CommType>::MWworker_statenames[] = {
    	"INITIALIZING", 
    	"IDLE", 
    	"BENCHMARKING/INITIAL DATA HANDLING",
    	"WORKING", 
    	"SUSPENDED",
    	"EXITED"
    	};


template <class CommType>
list<int> MWWorkerID<CommType>::misc_vid_nums;


template <class CommType>
int MWWorkerID<CommType>::max_vid_num = 0;



template <class CommType>
MWWorkerID<CommType>::MWWorkerID(MWAbstractDriver<CommType>* master_) 
  : runningtask(0),
    id1(-1),
    id2(-1),
    virtual_id(-1),
    arch(-1),
    exec_class(-1),
    executable_name(0),
    doomed(false),
    bench_result(-1.0),
    state(INITIALIZING),
    start_time(0.0),
    total_time(0.0),
    last_event(0.0),
    total_suspended(0.0),
    total_working(0.0),
    cpu_while_working(0.0),
    benchmarked_cpu_working_time(-1.0),
    normalized_cpu_working_time(0.0),
    sum_benchmark(0.0),
    num_benchmark(0),
    isBenchMark(false),
    isBenchMarkAvailable(false),
    group(1),
    master(master_)
{
assert(master != 0);
memset ( mach_name, 0, 64 ); 
// We use a negative number to signify that no benchmark has been set
set_vid ( get_next_vid() );
	
struct timeval timeVal;
gettimeofday ( &timeVal, NULL );
double currentTime = timeval_to_double ( timeVal );
networkLatency = 0.0;
networkLatency_lastMeasuredTime = currentTime;
}


template <class CommType>
void MWWorkerID<CommType>::printself( int level ) const
{
MWprintf ( level, "id1: %d id2: %d, vid: %d, name: \"%s\"\n",
			   id1, id2, virtual_id, mach_name ? mach_name : "" );
//MWprintf ( level, "Memory: %d, KFlops: %d, Mips: %d\n", Memory, KFlops, Mips );
MWprintf ( level, "arch: \"%d\" state: %s\n", 
			   arch, MWworker_statenames[state] );
	
if ( runningtask != NULL ) {
   MWprintf ( level, " -> Running task: " );
   runningtask->printself( level );
   }
else 
   MWprintf ( level, " -> There's no task on this worker.\n" );
}    


template <class CommType>
void MWWorkerID<CommType>::started()
{
// this is called when a worker first starts up.
if ( state != INITIALIZING && state != BENCHMARKING ) {
   MWprintf( 10, "Error:   started() worker whose "
			  "state != INITIALIZING && state != BENCHMARKING\n" );
   printself(10);
   }

state = IDLE;

//  To get parallel efficiency "right" on
//  MWfiles (with worker timeouts), we don't want to count
//  the machines that just sit there in INITIALIZING state
//  until they time out.  Thus, we do not start the counter here, but
//  only when we send a benchmark.
//
// struct timeval t;
// gettimeofday ( &t, NULL );
// start_time = timeval_to_double ( t );
}


template <class CommType>
void MWWorkerID<CommType>::benchmark() 
{
// this is called when a worker is sent the benchmark task
if ( state != IDLE ) {
   MWprintf( 10, "MWWorkerID::benchmark -- ERROR: benchmarking a worker "
				"state != IDLE\n" );
   printself(10);
   }

state = BENCHMARKING;
isBenchMark = true;
}


template <class CommType>
void MWWorkerID<CommType>::benchmarkOver()
{
if ( state != BENCHMARKING ) {
   MWprintf( 10, "MWWorkerID::benchmarkOver -- ERROR: benchmarking over of "
				" a worker whose state != BENCHMARKING\n" );
   printself(10);
   }

isBenchMark = false;
state = IDLE;

//  To get parallel efficiency "right" on
//  MWfiles (with worker timeouts), we don't want to count
//  the machines that just sit there in INITIALIZING state
//  until they time out.  Thus, we start the counter here

struct timeval t;
gettimeofday ( &t, NULL );
start_time = timeval_to_double ( t );
}


template <class CommType>
void MWWorkerID<CommType>::gottask( int tasknum )
{
state = WORKING;
struct timeval t;
gettimeofday ( &t, NULL );
last_event = timeval_to_double ( t );
}


template <class CommType>
void MWWorkerID<CommType>::completedtask( double wall_time, double cpu_time ) 
{
state = IDLE;
struct timeval t;
gettimeofday ( &t, NULL );
last_event = timeval_to_double( t );

total_working += wall_time;
cpu_while_working += cpu_time;

if (isBenchMarkAvailable){
   if ( (cpu_time * bench_result) > 0)
      normalized_cpu_working_time += cpu_time * bench_result;
   else 
      MWprintf ( 10, "PROBLEM !!! cpu_time = %lf ; bench = %lf\n", cpu_time, 
								bench_result);
   }
}


template <class CommType>
void MWWorkerID<CommType>::suspended()
{
state = SUSPENDED;
struct timeval t;
gettimeofday ( &t, NULL );
last_event = timeval_to_double ( t );
}


template <class CommType>
void MWWorkerID<CommType>::resumed()
{
struct timeval t;
gettimeofday ( &t, NULL );
double now = timeval_to_double ( t );

if ( state != SUSPENDED )
   MWprintf ( 10, "Got resume while not suspended!\n" );
else {
   if ( runningtask ) {
      total_suspended += now - last_event;
      state = WORKING;
      }
   else if ( isBenchMark == true )
      state = BENCHMARKING;
   else
      state = IDLE;
   }
last_event = now;
}


template <class CommType>
void MWWorkerID<CommType>::set_bench_result( double bres )
{
if ( bres < 1.0e-8 ) {
   if (bres < 0.0)
      MWprintf( 10, "Benchmark result must be a *positive* number\nSetting to 0\n");
   bench_result = 0.0;
   }
else {
   bench_result = bres;
   sum_benchmark += bres;
   num_benchmark++;
   isBenchMarkAvailable = true;
   }
}


template <class CommType>
void MWWorkerID<CommType>::ended()
{
struct timeval t;
gettimeofday ( &t, NULL );
double now = timeval_to_double ( t );

// 
// If state==WORKING then we're running
// a task, but MWDriver is finished.  Thus whatever we were doing wasn't
// important and we shouldn't count it towards our work.  (It's not
// part of "GoodPut", in other words.)
//
// However, we should count suspended work, since that was effort that 
// was arguably GoodPut, but it simply didn't finish in time... 
//
// TODO: evaluate the assumptions here!
//
if ( state == SUSPENDED )
   total_suspended += now - last_event;

state = EXITED;

if ( start_time > 1.0 )
   total_time = now - start_time;
else 
   total_time = 0.0;

release_vid ( get_vid() );

for ( size_t i = 0; i < master->workGroupWorkerNum.size(); i++ )
  if ( doesBelong ( i ) )
     deleteGroup ( i );
}


template <class CommType>
void MWWorkerID<CommType>::ckpt_stats( double *up, 
			     double *working, 
			     double *susp, 
			     double *cpu, 
			     double *norm, 
			     double *s_bench, 
			     int *n_bench )
{
// print duration, total_working, total_suspended 

struct timeval t;
gettimeofday ( &t, NULL );
double now = timeval_to_double ( t );

if ( start_time > 1.0 )
   *up = now - start_time;  
else
   *up = 0.0;   // possible if not yet started().

*working = total_working;
*susp = total_suspended;
*cpu = cpu_while_working;
*norm = normalized_cpu_working_time;

*s_bench = sum_benchmark;
*n_bench = num_benchmark;
}


template <class CommType>
void MWWorkerID<CommType>::addGroup ( int num )
{
if (doesBelong(num))
   return;
master->workGroupWorkerNum[num]++;
group.join(num);
}


template <class CommType>
void MWWorkerID<CommType>::deleteGroup ( int num )
{
if (!doesBelong(num))
   return;
master->workGroupWorkerNum[num]--;
group.leave(num);
}


#endif
