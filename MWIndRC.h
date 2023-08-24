//
// MWIndRC.h
//

#ifndef __MWIndRC_h
#define __MWIndRC_h

#include "MWRMComm.h"

/** This class is an derived from the Resource Management (RM) and
	Communication (Comm) layer. In MW-Ind, there is just one worker and one
	master and both are on the same machine and combined into the same
	process.  Send and Recv are merely memcpy. This class is useful
	for debugging purposes.  The API etc all will remain the same when using
	this layer. 
*/

class MWIndRC : public MWRMComm 
{
public:

  /** @name Main Routines */
  //@{
  /// Constructor.  Sets data to -1's.
  MWIndRC();

  /// Destructor...
  virtual ~MWIndRC() {}
  //@}


  /** @name Resource Management Functions */
  //@{
  ///
  void exit( int exitval ) {}

  ///
  int setup( int& argc, char**& argv, int *my_id, int *master_id);

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
  virtual int start_worker( MWWorkerID<MWIndRC>* w );

  ///
  int init_workers()
		{return 0;}

  ///
  int restart_workers() {return 0;}

  /** Configure a single worker */
  void config_worker(MWWorkerID<MWIndRC>* ); // TODO

  /** Remove a worker from the virtual machine.  This call
        will delete w, so don't reference that memory again!
        @param w The MWWorkerID of the machine to remove.
        @return 0 on success, a negative number on failure
  */
  virtual int removeWorker( MWWorkerID<MWIndRC>* w ) {return -1;} // TODO
  //@}


	
  /** @name Host Management Members */
  //@{
  ///
  int hostaddlogic( vector<int>&  )
		{return 0;}
  //@}


  /**@name Communication Routines */
  //@{
  ///
  void hostadd ( )
		{ last_tag = HOSTADD; }
		
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

  /** @name Pack Functions */
  //@{
  /// Pack some bytes
  int pack ( const char *bytes,         int nitem, int stride = 1 );

  /// float
  int pack ( float *f,            int nitem=1, int stride = 1 );

  /// double
  int pack ( double *d,           int nitem=1, int stride = 1 );

  /// int
  int pack ( int *i,              int nitem=1, int stride = 1 );

  /// unsigned int
  int pack ( unsigned int *ui,    int nitem=1, int stride = 1 );

  /// short
  int pack ( short *sh,           int nitem=1, int stride = 1 );

  /// unsigned short
  int pack ( unsigned short *ush, int nitem=1, int stride = 1 );

  /// long
  int pack ( long *l,             int nitem=1, int stride = 1 );

  /// unsigned long
  int pack ( unsigned long *ul,   int nitem=1, int stride = 1 );

  /// Pack a NULL-terminated string
  int pack ( char *string );

  /// Pack a NULL-terminated string
  int pack ( const char *string );

  /// Pack an STL string object
  int pack ( const std::string& str )
		{return pack(str.data());}

  /// 
  int pack_explicit( const char* bytes,  int nitem)
		{return pack(bytes,nitem);}
  //@}

  /** @name Unpack Functions */
  //@{
  /// Unpack some bytes
  int unpack ( char *bytes,         int nitem, int stride = 1 );

  /// float
  int unpack ( float *f,            int nitem=1, int stride = 1 );

  /// double
  int unpack ( double *d,           int nitem=1, int stride = 1 );

  /// int
  int unpack ( int *i,              int nitem=1, int stride = 1 );

  /// unsigned int
  int unpack ( unsigned int *ui,    int nitem=1, int stride = 1 );

  /// short
  int unpack ( short *sh,           int nitem=1, int stride = 1 );

  /// unsigned short
  int unpack ( unsigned short *ush, int nitem=1, int stride = 1 );

  /// long
  int unpack ( long *l,             int nitem=1, int stride = 1 );

  /// unsigned long
  int unpack ( unsigned long *ul,   int nitem=1, int stride = 1 );

  /// Unpack a NULL-terminated string
  int unpack ( char *str );

  /// Unpack an STL string object
  int unpack ( std::string& str );

  /// Used to hand off the unpack buffer to a seperate application
  const char* unpack_buffer()
		{return ind_recvbuf.buf;}

  /// Used to hand off the unpack buffer to a seperate application
  int unpack_index()
		{return ind_recvbuf.num_sent;}

  /// Used to hand off the unpack buffer to a seperate application
  int unpack_buffer_size()
		{return ind_recvbuf.buf_size;}

  /** Used to update the unpack buffer after it has been used by a seperate
      application.
  */
  void unpack_index(int index)
		{ind_recvbuf.num_sent = index;}
  //@}
  //@}

protected: 

  ///
  int bytes_packed;

  ///
  int bytes_unpacked;

  /// A simple structure for the send/receive buffers
  class SendBuf {
	public:
	static unsigned int buf_size;
	int num_sent;
	char* buf;
	SendBuf() { buf = new char[buf_size];}
	~SendBuf() {delete buf;}
	};

  /// The send buffer
  struct SendBuf ind_sendbuf;

  /// The receive buffer
  struct SendBuf ind_recvbuf;

  ///
  int last_tag;

  ///
  int curr_buf_tag;

  ///
  int curr_buf_len;

  ///
  int curr_buf_id;

  /// The id of the current 
  int curr_from_id;
};

#endif
