//
// MW.h
//

#ifndef __MW_h
#define __MW_h

#include <functional>
#include <ctime>
#include <sys/time.h>

/**@name Introduction.

   MW is a class library that can be a useful tool for building
   opportunistic, fault-tolerant applications for high throughput
   computing.  

   In order to build an application, there are three classes
   that the user {\rm must} rederive.

   \begin{itemize}
   \item \Ref{MWDriver}
   \item \Ref{MWWorker}
   \item \Ref{MWTask}
   \end{itemize}

   The documentation of these classes includes a description 
   of the pure virtual methods that must be implemented for 
   a user's particular application.

   Using the MW library allows the user to focus on the application
   specific implementation at hand.  All details related to
   being fault tolerant and opportunistic are implemented in the
   MW library.

   Also included is a small, naive, example of how to create
   an application with the MW class library.  The three classes

   \begin{itemize}
   \item \Ref{Driver_Fib}
   \item \Ref{Worker_Fib}
   \item \Ref{Task_Fib}
   \end{itemize}
   
   are concrete classes derived from MW's abstract classes.  
   Using these classes, a simple program that makes a lot of 
   fibonacci sequences is created.

 */
class MWBase {

public:

  /**
   * A list of the message tags that the Master-Worker application
   * will send and receive.  See the switch statement in master_mainloop 
   * for more information.
   */
  enum MWmessages{
	UNDEFINED = 0,
	HOSTDELETE = 1,   
	HOSTSUSPEND = 2, 
	HOSTRESUME = 3, 
	TASKEXIT = 4,   
	DO_THIS_WORK = 5, 
	RESULTS = 6,      
	INIT = 7,         
	INIT_REPLY = 8,   
	BENCH_RESULTS = 9,
	KILL_YOURSELF = 10,
	CHECKSUM_ERROR = 11,
	RE_INIT = 12,
	REFRESH = 13,
	HOSTADD = 33
	};

  /// This is the "key" by which the task list can be managed.
  typedef double MWKey;

  /** Possible return values from many virtual functions */
  enum MWReturn {
	/// Normal return
	OK, 
	/// We want to exit, not an error.
	QUIT, 
	/// We want to exit immediately; a bad error ocurred
	ABORT
	};

  ///
  enum MWRMCommErrors {
        WORKER_TIMEOUT = -50,
        UNABLE_TO_WAKE = -49,
        CANNOT_OPEN_INPUT_FILE = -48,
        SCANF_ERROR_ON_INPUT_FILE = -47,
        MESSAGE_SEQUENCE_ERROR = -46,
        UNKNOWN_DATA_TYPE = -45,
        WAITFILE_PROTOCOL_ERROR = -44,
        FAIL_MASTER_SEND = -43,
        CHECKSUM_ERROR_EXIT = -42,
        UNKNOWN_COMMAND = -41,
        INIT_REPLY_FAILURE = -40,
        UNPACK_FAILURE = -39
        };

  /** This is a list of the states in which a worker can be.  */
  enum MWworker_states {
    /// Some initialization; not ready for work
    INITIALIZING=0,
    /// Waiting for the master to send work
    IDLE=1,
    /// Benchmarking, or doing application initialization on the initial data
    BENCHMARKING=2,
    /// Working actively on its task
    WORKING=3,
    /// The machine has been suspended
    SUSPENDED=4,
    /// This worker needs to be removed from the workere pool..
    EXITED=5
    };

};


/** A helper function...  Returns double value of seconds in timeval t. */
inline double timeval_to_double( struct timeval t )
{ return (double) t.tv_sec + ( (double) t.tv_usec * (double) 1e-6 ); }


template <class CommType>
class MWWorkerID;

/** A function object for comparing two classes
  * using a given function that defines the key.
  */
template <typename T>
class MWKeyFuncLess : public std::binary_function < T , T , bool >
{
public:

  /// Typedef of functions
  typedef typename MWBase::MWKey (*funcType)(const T);
  
  /// Constructor
  MWKeyFuncLess(funcType keyFunc_=0 )
                : keyFunc(keyFunc_) {}

  ///
  bool operator()(const T arg1, const T arg2)
                {
                if (keyFunc)
                   return (*keyFunc)(arg1) < (*keyFunc)(arg2);
                else
                   return false;
                }

  ///
  operator bool()
                {return keyFunc != 0;}

  funcType keyFunc;
  
  funcType funcptr()
  		{return keyFunc;}

};



#endif

