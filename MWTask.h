//
// MWTask.h
//

#ifndef __MWTask_h
#define __MWTask_h

#include "MWAbstractDriver.h"
#include "MWWorkerID.h"
#include "MWGroup.h"

//
// Forward declaration
//
template <class CommType>
class MWTask;


/** This class represents a unit of work for a MWWorker. 

    The task consits of two main components.  The "work" to be done,
    and the "result" of the completed task.  In order to create an
    application, the user must specify methods for packing and unpacking
    both the "work" and "result" portions of the task.

    When the task is being serviced by a worker, it contains a link
    to the ID of that instance of the worker.


    @see MWAbstractDriver
    @see MWWorker
    @author Mike Yoder, modified by Jeff Linderoth and Jean-Pierre Goux
*/

template <typename CommType>
class MWTask
{

  friend class MWAbstractDriver<CommType>;

public:   

  ///
  typedef enum { NORMAL=0, NWS=1, NUMTASKTYPES=2 } TaskType;

  /// Default constructor
  MWTask(MWAbstractDriver<CommType>* master_, CommType* RMC_);
  
  /// Default Destructor
  virtual ~MWTask();

  /// The task's number
  int number;

  /// The task's type.
  TaskType taskType;

  /** @name Time and usage data */
  //@{
  /** The time (wall clock) that it takes to run the 'execute_task'
		  function on the worker's side. */
  double working_time;

  /** The amount of user+system time taken by this task, measured
		  from start to finish of the 'execute_task' function. */
  double cpu_time;

  /// A status flag.  If true, then this task is finished.
  bool status;
  //@}


  /**@name Packing and Unpacking 
	 The user must pack and unpack the contents of the class
	 so that it can be sent to the worker and back.  These 
	 functions will make RMC->pack() and RMC->unpack() calls.
  */
  //@{
  /// Pack the work portion of the task
  virtual void pack_work( void ) = 0;

  /// Unpack the work portion of the task
  virtual void unpack_work( void ) = 0;

  /// Pack the result portion of the task
  virtual void pack_results( void ) = 0;

  /// Unpack the result portion of the task
  virtual void unpack_results( void ) = 0;

  /// Dump this task to the screen
  virtual void printself( int level = 60 )
		{MWprintf ( level, "  Task %d\n", number);}
  //@}


  /**@name Checkpointing Utilities
     These two functions are used when checkpointing.  Simply put, 
     they write/read the state of the task to this file pointer 
     provided.
  */
  //@{
  /// Write the state of this task out to an output stream
  virtual void write_ckpt_info(ostream& os) {};
  /** Read the state of this task from an input stream (overwriting
          any existing state */
  virtual void read_ckpt_info(istream& is) {};
  ///
  void write_group_info (ostream& os);
  ///
  void read_group_info (istream& is);
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
		{ return group.belong ( num ); }
  ///
  MWGroup& getGroup ( )
		{ return group; }

  ///
  MWGroup group;

  ///
  MWAbstractDriver<CommType>* master;

  /// A pointer to the worker ID executing this task (NULL if none)
  MWWorkerID<CommType> *worker;

  ///
  CommType* RMC;
  
};


/**
template <class CommType>
bool less<MWTask<CommType>*>::operator()(const MWTask<CommType>*& __x, const MWTask<CommType>*& __y) const
{
return __x->number < __y->number;
}
**/


template <class CommType>
MWTask<CommType>::MWTask(MWAbstractDriver<CommType>* master_, CommType* RMC_)
 : number(-1),
   taskType(NORMAL),
   working_time(0.0),
   cpu_time(0.0),
   status(false),
   group(1),
   worker(NULL),
   RMC(RMC_)
{
assert(RMC_ != 0);
if (master)
   initGroups(master_->workGroupTaskNum.size());
else
   initGroups(0);
}


template <class CommType>
MWTask<CommType>::~MWTask()
{
if ( worker )
   if ( worker->runningtask )
      worker->runningtask = NULL;
}


template <class CommType>
void MWTask<CommType>::addGroup ( int num )
{
if ( doesBelong ( num ) )
   return;
if (master)
   master->workGroupTaskNum[num]++;
group.join( num );
}


template <class CommType>
void MWTask<CommType>::deleteGroup ( int num )
{
if ( !doesBelong ( num ) )
   return;
if (master)
   master->workGroupTaskNum[num]--;
group.leave ( num );
}


template <class CommType>
void MWTask<CommType>::write_group_info (ostream& os)
{ group.write_checkpoint ( os ); }


template <class CommType>
void MWTask<CommType>::read_group_info (istream& is )
{
int num = (master ? master->workGroupTaskNum.size() : 1);
initGroups(num);
group.read_checkpoint ( is );

if (master) 
   for ( int i = 0; i < num; i++ )
     if ( doesBelong ( i ) )
        master->workGroupTaskNum[num]++;
}

#endif
