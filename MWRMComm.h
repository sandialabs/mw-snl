//
// MWRWComm.h
//

#ifndef __MWRWComm_h
#define __MWRWComm_h

#include <vector>
#include <string>
using namespace std;
#include "MW.h"

//
// Forward declaration
//
template <class CommType>
class MWWorkerID;


/** This class is an abstract layer over a Resource Management (RM) and
	Communication (Comm) layer.  Hence the RMComm name.  It is designed
	to abstract away the differences between various systems.

	The member functions in this class fall into two categories:  
	Resource Management and Message Passing (Communication).  The "master"
	in a master-worker application will use both; the worker will only
	use the communication facilities.
*/

class MWRMComm : public MWBase {

public:

  /** @name Main Routines */
  //@{
  /// Constructor
  MWRMComm();

  /// Destructor...
  virtual ~MWRMComm();
  //@}

  /** @name Resource Management Functions

	Here are methods for managing a set of machines. 
  */
  //@{
  /** System shutdown.  Does not return.  
      @param exitval The value to call ::exit() with 
  */
  virtual void exit( int exitval );

  /** Initialization of the master process.  This will be called
	one time only when the master starts up.  
	@return 0 on success, -1 on failure
  */
  virtual int setup( int& argc, char**& argv, int *my_id, int *master_id ) = 0;

  ///
  int my_id()
	{return myid;}

  ///
  int master_id()
	{return masterid;}

  /** This returns the state of the "virtual machine" - aka the 
	set of workers under the master's control.  
	@param nhosts The number of hosts
	@param narches The number of architechture classes
	@param w A pointer to a pointer to an array of pointers.  
    This should be NULL when called; config() will allocate
    the memory using new and return it to you.  Don't forget
    to delete not only the elements in that array, but 
    also the array itself.  The array will have nhosts
    elements, and they will each represent a worker 
    machine.
	 @return 0 on success, -1 on error.
  */
  //TODO virtual int config( int *nhosts, int *narches, MWWorkerID ***w ) { return 0; };
	
  /** This routine is used to start up multiple workers at 
	the beginning of a run.  It should only be called one
	time.  It basically does a config() to find out what
	machines are available, and then starts a worker on 
	each of them.  You may want to check the implementations
	for details...
	@param nworkers The number of workers at the start (returned!)
	@param workers A pointer to a pointer to an array of
	 MWWorkerID pointers.  The memory management for this 
	 is the same as it is for the config() call - it should
	 be null when called and will point to allocated memory
	 when it comes back.
	@return 0 on success, -1 on failure
  */
  //TODO virtual int init_beginning_workers( int *nworkers, MWWorkerID ***workers ) = 0;

  /** Initialize the workers when starting up from scratch. */
  virtual int init_workers() = 0;

  /** Restart the workers after reading in a checkpoint file. */
  virtual int restart_workers() = 0;
  //@}


  /** @name Checkpointing Functions */
  //@{
  /// Write out internal state to an output stream
  virtual int write_checkpoint(ostream& os);

  /// Read in restart information from an input stream
  virtual int read_checkpoint(istream& is);

  /// Some Low level specific read/write functions
  virtual int read_RMstate(istream& ) {return 0;}
  ///
  virtual int write_RMstate (ostream& ) {return 0;}

  /** A function to restart the workers. This is called at restart from
      a ckpt file and it is meant to re-init all the workers.
  */
  //TODO virtual int restart_beginning_workers ( int *num, MWWorkerID ***tempWorkers, MWmessages msg ) = 0;
  //@}


  /** @name Executable Management Interface functions */
  //@{
  /// Set the number of executable classes
  void set_num_exec_classes ( int num );

  /// Returns the above number
  int get_num_exec_classes ( )
		{return exec_class_target_num_workers.size();}

  /// Set the number of arch classes
  void set_num_arch_classes( int n );

  /// set the arch attribute
  void set_arch_class_attributes (unsigned int arch_class, char *attr );

  /// Return the number of arch classes
  int get_num_arch_classes()
		{return arch_class_attributes.size();}

  /// Set the number of executables
  void set_num_executables ( int num );

  /** Set the name of the binary and the requirements string 
	for an arch class.  Technically, the requirements string 
	is not needed for MWPvmRC - it is defined in the submit
	file.  It *is* needed in the MWFileRC, however, for job
	submission.
	@param arch_class This specifies which arch class the above
	    requirements will apply to.
	@param exec_name The name of the worker executable for 
	    this arch class.
	@param requirements A string containing the "requirements" 
	    attribute of a given arch class of workers.  This will 
		be something that can be used in a submit file, like 
		"(arch == \"INTEL\" && opsys == \"SOLARIS26\")"
  */
  void add_executable ( int exec_class, unsigned int arch_class, 
				char *exec_name, char *requirements ); 

  /** If the RM software needs to "process" the executable name in
  	some way, this is done here.
  */
  virtual char* process_executable_name ( char* exec_name, int ex_cl, 
					int num_arc );

  /** Set whether or not you would like worker checkpointing 
  	(if the CommRM implementation has the capability)
  */
  virtual void set_worker_checkpointing( bool wc );
  //@}


  /** @name Host Management Members */
  //@{
  /** Set a "target" number of workers.  If exec_class is -1, then 
	set the target across all arches. This 
	is useful if you don't care how many you get of each arch... 
	@param num_workers The target number of workers 
  */
  void set_target_num_workers( int num_workers, int exec_class=-1 );
	
  /** This will figure out if we need to ask for more hosts
	or remove hosts.  It is called whenever a host is added
	or removed from the system, or set_target_num_workers()
	is called.
	@param num_workers A pointer to an array of length
	 num_arches that contains the number of workers for 
	 each arch class.
	@return If we have more workers than we need, we return a
	 positive number as the "excess" that can be deleted.  
  */
  virtual int hostaddlogic( vector<int>& num_workers ) = 0;

  /// I think that this is correct.... WEH  BUG - should count actual workers
  int& exec_class_num_workers(int num)
		{return exec_class_target_num_workers[num];}
  //@}


  /** @name The Communication Routines

	These message passing routines are very closely modeled on
	PVM's message passing facility.  They are, however, pretty
	generic in that any message passing interface should be 
	able to implement them.
  */
  //@{
  /** Initialize a buffer for sending some data.
	@param encoding Defined by each application.  0 = default */
  virtual int initsend ( int encoding = 0 )  = 0;

  /** Send the data that has been packed. 
	@param to_whom An identifier for the recipient
	@param msgtag A 'tag' to identify that type of message */
  virtual int send ( int to_whom, int msgtag )  = 0;

  /** Receive some data that has been packed.  Should make this 
	more PVM-independent; will do this sometime.
	@param from_whom From a specific source; -1 is from all
	@param msgtag With a certain tag; -1 is all.
	@param buf_id The id where the message is stored; -1 is all.
	The return value indicates whether or not a message is 
	received.
	*/
  virtual bool recv ( int from_whom, int msgtag, int& buf_id ) = 0;

  /** Provide info on the message just received */
  virtual int bufinfo ( int buf_id, int *len, int *tag, int *from ) = 0;

  /** For some system events like HOSTDELETE, TASKEXIT, etc, this will tell
  who was affected */
  virtual void who ( int *wh )  = 0;

  /** Needed only for MW-Independent */
  virtual void hostadd ( ) { };

  /// Return the number of bytes packed and unpacked
  virtual void get_packinfo(int& bytes_packed, int& bytes_unpacked) = 0;

  /** @name Pack Functions
			
	In the following pack() functions, there are some common themes.
	First, each stuffs some data into a buffer to be sent.  The
	nitem parameter is just a count of the number of items.  The 
	stride parameter specifies *which* items to pack.  1 implies
	all, 2 would be every 2nd item, 3 is every 3rd item, etc. 

	The return value is user defined.  It should be standardized, 
	but I'll do that later.
	*/
  //@{
  /// float
  virtual int pack ( float *f,            int nitem=1, int stride = 1 ) = 0;

  /// double
  virtual int pack ( double *d,           int nitem=1, int stride = 1 ) = 0;

  /// int
  virtual int pack ( int *i,              int nitem=1, int stride = 1 ) = 0;

  /// unsigned int
  virtual int pack ( unsigned int *ui,    int nitem=1, int stride = 1 ) = 0;

  /// short
  virtual int pack ( short *sh,           int nitem=1, int stride = 1 ) = 0;

  /// unsigned short
  virtual int pack ( unsigned short *ush, int nitem=1, int stride = 1 ) = 0;

  /// long
  virtual int pack ( long *l,             int nitem=1, int stride = 1 ) = 0;

  /// unsigned long
  virtual int pack ( unsigned long *ul,   int nitem=1, int stride = 1 ) = 0;

  /// Pack some bytes
  virtual int pack ( const char *bytes,   int nitem, int stride = 1 ) = 0;

  /// Pack a NULL-terminated string
  virtual int pack ( char *string ) = 0;

  /// Pack a const NULL-terminated string
  virtual int pack ( const char *string ) = 0;

  /// Pack an STL string object
  virtual int pack ( const string& str ) = 0;

  /** Explicitly copy these bytes into the buffer.
      This is a bit of a hack to support the independent packing of 
      bytes that are later inserted into the pack buffer.
   */
  virtual int pack_explicit( const char* bytes,  int nitem) = 0;
  //@}

  /** @name Unpack Functions
			
	These unpack functions unpack data packed with the pack() 
	functions.  See the pack() functions for more details.
  */
  //@{
  /// float
  virtual int unpack ( float *f,            int nitem=1, int stride = 1 ) = 0;

  /// double
  virtual int unpack ( double *d,           int nitem=1, int stride = 1 ) = 0;

  /// int
  virtual int unpack ( int *i,              int nitem=1, int stride = 1 ) = 0;

  /// unsigned int
  virtual int unpack ( unsigned int *ui,    int nitem=1, int stride = 1 ) = 0;

  /// short
  virtual int unpack ( short *sh,           int nitem=1, int stride = 1 ) = 0;

  /// unsigned short
  virtual int unpack ( unsigned short *ush, int nitem=1, int stride = 1 ) = 0;

  /// long
  virtual int unpack ( long *l,             int nitem=1, int stride = 1 ) = 0;

  /// unsigned long
  virtual int unpack ( unsigned long *ul,   int nitem=1, int stride = 1 ) = 0;

  /// bytes
  virtual int unpack ( char *bytes,         int nitem, int stride = 1 ) = 0;

  /// Unpack a NULL-terminated string
  virtual int unpack ( char *string ) = 0;

  /// Unpack an STL string object
  virtual int unpack ( string& str ) = 0;

  /// Used to hand off the unpack buffer to a seperate application
  virtual const char* unpack_buffer() = 0;

  /// Used to hand off the unpack buffer to a seperate application
  virtual int unpack_index() = 0;

  /// Used to hand off the unpack buffer to a seperate application
  virtual int unpack_buffer_size() = 0;

  /** Used to update the unpack buffer after it has been used by a seperate
      application.
  */
  virtual void unpack_index(int index) = 0;
  //@}
  //@}


protected:


  /** @name Protected Executable Management Interface functions */
  //@{
  /// Definition of an RMC executable
  struct RMC_executable {
	int arch_class;
	int exec_class;
	char *executable;
	char *executable_name;
	char *attributes;
	};

  /** The arch attributes */
  vector<char*> arch_class_attributes;

  /** The number of executables */
  int tempnum_executables;

  /** An array containing the {\bf full} pathnames of the executables.
	Element 0 is for arch "0", element 1 is for arch "1", etc.
	Usually read in get get_userinfo().
  */
  vector<struct RMC_executable*> worker_executables;

  /// Would you like the workers to be checkpointed
  bool worker_checkpointing;

  /// Keep my id around for further reference
  int myid;

  /// Keep master id around for further reference
  int masterid;
  //@}


  /** @name Protected Host Management Members */
  //@{
  /** The desired number of workers */
  int target_num_workers;

  /** The desired number of workers, exec_class wise */
  vector<int> exec_class_target_num_workers;
  //@}
  
};

#endif
