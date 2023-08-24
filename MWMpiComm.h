//
// MWMpiComm.h
//

#ifndef __MWMpiComm_h
#define __MWMpiComm_h

#include <vector>
#include "MWRMComm.h"
#ifdef USING_MPI
#include "mpi.h"
#endif


class MWMpiComm;



#ifdef USING_MPI

/** This class is used to manage the status of messages within 
  * MWMpiComm.
  */
class MsgStatus
{
  friend class MWMpiComm;
protected:
  MsgStatus(int size) {nbytes=0; buf.resize(size);}
public:
  void reset() {nbytes=0; index=0; id=-1;}
  void resize(unsigned int num)
		{
		if ((index+num) > buf.size())
		   buf.resize(std::max(2*buf.size(), index+num));
		}

  std::vector<char> buf;
  MPI_Request  request;
  int id;
  int nbytes;
  int index;
};



/** This class is an derived from the Resource Management (RM) and
	Communication (Comm) layer. 
*/

class MWMpiComm : public MWRMComm 
{
public:

  /** @name Main Routines */
  //@{
  /// Constructor.  Sets data to -1's.
  MWMpiComm();

  /// Destructor...
  virtual ~MWMpiComm() {}
  //@}


  /** @name Resource Management Functions */
  //@{
  ///
  void exit( int exitval );

  ///
  int setup( int& argc, char**& argv, int *my_id, int *master_id,
			MPI_Comm comm_);

  ///
  int setup( int& argc, char**& argv, int *my_id, int *master_id)
		{return setup(argc,argv,my_id,master_id,MPI_COMM_WORLD);}

  ///
  // TODO int config( int *nhosts, int *narches, MWWorkerID ***w ) {return 0;}
	
  /** Start a worker on a machine that has been given to you.
        This is really only important if the process of starting
        a worker is two-stage.  For instance, in pvm, you
        first get a machine.  THEN you have to spawn an
        executable on that machine.  For something like MW-files,
        once you get a worker it already has the executable
        started on it.  In that case this will basically be
        a no-op.
        @param w A pointer to a MWWorkerID.  This must point
         to allocated memory.  It is filled with info like the
         machine name, id2, and the arch.
        @return id2 on success, -1 on failure
  */
  virtual int start_worker( MWWorkerID<MWMpiComm>* w );

  ///
  int init_workers();

  ///
  int restart_workers() {return 0;}

  /** Configure a single worker */
  void config_worker(MWWorkerID<MWMpiComm>* ); // TODO

  /** Remove a worker from the virtual machine.  This call
        will delete w, so don't reference that memory again!
        @param w The MWWorkerID of the machine to remove.
        @return 0 on success, a negative number on failure
  */
  virtual int removeWorker( MWWorkerID<MWMpiComm>* w ) {return -1;} // TODO
  //@}


	
  /** @name Host Management Members */
  //@{
  ///
  int hostaddlogic( std::vector<int>&  )
		{return 0;}
  //@}


  /**@name Communication Routines */
  //@{
  /// TODO
  void hostadd ( )
		{ }
		
  ///
  void who ( int *wh );

  ///
  int initsend ( int encoding = 0 );

  ///
  int send ( int to_whom, int msgtag );

  ///
  bool recv ( int from_whom, int msgtag, int &buf_id );

  ///
  int bufinfo ( int buf_id, int *len, int *tag, int *from );

  ///
  void get_packinfo(int& bytes_packed, int& bytes_unpacked);


  /** @name Pack/UnPack Functions */
  //@{
  /// float
  int pack ( float *f,            int nitem=1, int stride = 1 )
		{return do_pack(f,nitem,stride);}

  /// double
  int pack ( double *d,           int nitem=1, int stride = 1 )
		{return do_pack(d,nitem,stride);}

  /// int
  int pack ( int *i,              int nitem=1, int stride = 1 )
		{return do_pack(i,nitem,stride);}

  /// unsigned int
  int pack ( unsigned int *ui,    int nitem=1, int stride = 1 )
		{return do_pack(ui,nitem,stride);}

  /// short
  int pack ( short *sh,           int nitem=1, int stride = 1 )
		{return do_pack(sh,nitem,stride);}

  /// unsigned short
  int pack ( unsigned short *ush, int nitem=1, int stride = 1 )
		{return do_pack(ush,nitem,stride);}

  /// long
  int pack ( long *l,             int nitem=1, int stride = 1 )
		{return do_pack(l,nitem,stride);}

  /// unsigned long
  int pack ( unsigned long *ul,   int nitem=1, int stride = 1 )
		{return do_pack(ul,nitem,stride);}

  /// Pack some bytes
  int pack ( const char *bytes,   int nitem, int stride = 1 )
		{return do_pack(bytes,nitem,stride);}

  /// Pack a NULL-terminated string
  int pack ( char* string );

  /// Pack a NULL-terminated string
  int pack ( const char *string );

  ///
  int pack ( const std::string& str )
		{return pack(str.data());}

  ///
  int pack_explicit( const char* bytes,  int nitem);

  /// float
  int unpack ( float *f,            int nitem=1, int stride = 1 )
		{return do_unpack(f,nitem,stride);}

  /// double
  int unpack ( double *d,           int nitem=1, int stride = 1 )
		{return do_unpack(d,nitem,stride);}

  /// int
  int unpack ( int *i,              int nitem=1, int stride = 1 )
		{return do_unpack(i,nitem,stride);}

  /// unsigned int
  int unpack ( unsigned int *ui,    int nitem=1, int stride = 1 )
		{return do_unpack(ui,nitem,stride);}

  /// short
  int unpack ( short *sh,           int nitem=1, int stride = 1 )
		{return do_unpack(sh,nitem,stride);}

  /// unsigned short
  int unpack ( unsigned short *ush, int nitem=1, int stride = 1 )
		{return do_unpack(ush,nitem,stride);}

  /// long
  int unpack ( long *l,             int nitem=1, int stride = 1 )
		{return do_unpack(l,nitem,stride);}

  /// unsigned long
  int unpack ( unsigned long *ul,   int nitem=1, int stride = 1 )
		{return do_unpack(ul,nitem,stride);}

  /// bytes
  int unpack ( char *bytes,         int nitem, int stride = 1 )
		{return do_unpack(bytes,nitem,stride);}

  /// Unpack a NULL-terminated string
  int unpack ( char *string );

  /// Unpack a string object
  int unpack ( string& str );

  /// Used to hand off the unpack buffer to a seperate application
  const char* unpack_buffer()
		{return &(received.msg->buf[0]);}

  /// Used to hand off the unpack buffer to a seperate application
  int unpack_index()
		{return received.index;}

  /// Used to hand off the unpack buffer to a seperate application
  int unpack_buffer_size()
		{return received.msg->nbytes;}

  /** Used to update the unpack buffer after it has been used by a seperate
      application.
  */
  void unpack_index(int index)
		{received.index = index;}
  //@}
  //@}

protected: 

  /// Pack some bytes
  template <class TYPE>
  int do_pack(TYPE* bytes, int nitem, int stride);

  /// Unpack some bytes
  template <class TYPE>
  int do_unpack(TYPE* data, int nitem, int stride);

  ///
  int bytes_packed;

  ///
  int bytes_unpacked;

#ifdef USING_MPI

	/// True if MWMpiComm calls MPI_Init()
  bool comm_initialized_locally;

	///
  int num_send_msgs;

	///
  int num_recv_msgs;

	///
  int bufsize;

	/// The set of messages that have been asynchronously sent out
  std::vector<MsgStatus*>  outgoing_msgs;

	/// The message that is being prepared to send out.
  MsgStatus* curr_send_msg;

	/// The most recently received message
  struct {
    public:
    MsgStatus* msg;
    int tag;
    int source;
    int index;
    } received;

	///
  int rank;
	///
  int size;
	///
  int iDoIO;
	///
  int ioProc;
	///
  MPI_Comm comm;

	///
  template <class TYPE>
  MPI_Datatype mpi_datatype()
		{return MPI_DATATYPE_NULL;}
#endif
  
	/// The ID of the next worker ... configured by config_worker
  int next_worker;
};



template <class TYPE>
inline int MWMpiComm::do_pack(TYPE* data, int nitem, int stride)
{
if (mpi_datatype<TYPE>() == MPI_DATATYPE_NULL)
   abort();

int size_;
MPI_Pack_size(1, mpi_datatype<TYPE>(), MPI_COMM_WORLD, &size_);
curr_send_msg->resize(size_*nitem);
int start = curr_send_msg->index;

for ( int i = 0; i < nitem; i++ ) {
  int tmpsize = curr_send_msg->buf.size();
  MPI_Pack((void*)(&data[i*stride]), 1, mpi_datatype<TYPE>(),
                &(curr_send_msg->buf[0]), tmpsize,
                &(curr_send_msg->index), comm);
  }

bytes_packed += (curr_send_msg->index-start);
MWprintf(90,"packing bytes=%d nitem=%d type=%d\n",
                (curr_send_msg->index-start), nitem, mpi_datatype<TYPE>());
return 0;
}


template <class TYPE>
int MWMpiComm::do_unpack(TYPE* data, int nitem, int stride)
{
int start = received.index;
for ( int i = 0; i < nitem; i++ ) {
  MPI_Unpack( &(received.msg->buf[0]),
                received.msg->nbytes,
                &(received.index),
                (void*)(&data[i*stride]),
                1, mpi_datatype<TYPE>(),
                comm);
  }

bytes_unpacked += (received.index - start);
MWprintf(90,"unpacked bytes=%d nitem=%d type=%d\n", 
			(received.index-start), nitem, mpi_datatype<TYPE>());
return 0;
}

#endif


#endif
