//
// Note: this code assumes that another message is not sent to 
// destination X until a response has been received from X.
//

#include <assert.h>
#include "MWMpiComm.h"
#include "MWprintf.h"
#include "MWWorkerID.h"

#ifdef USING_MPI

template <>
MPI_Datatype MWMpiComm::mpi_datatype<int>()
{return MPI_INT;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<unsigned int>()
{return MPI_UNSIGNED;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<long>()
{return MPI_LONG;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<unsigned long>()
{return MPI_UNSIGNED_LONG;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<short>()
{return MPI_SHORT;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<unsigned short>()
{return MPI_UNSIGNED_SHORT;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<char>()
{return MPI_CHAR;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<char const>()
{return MPI_CHAR;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<unsigned char>()
{return MPI_UNSIGNED_CHAR;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<unsigned char const>()
{return MPI_UNSIGNED_CHAR;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<float>()
{return MPI_FLOAT;}

template <>
MPI_Datatype MWMpiComm::mpi_datatype<double>()
{return MPI_DOUBLE;}



MWMpiComm::MWMpiComm() 
 : comm_initialized_locally(false),
   num_send_msgs(0),
   num_recv_msgs(0),
   bufsize(1024),
   curr_send_msg(0),
   rank(-1),
   size(0),
   iDoIO(0),
   ioProc(-1),
   comm(MPI_COMM_WORLD),
   next_worker(1)
{
received.msg = 0;
bytes_packed=0;
bytes_unpacked=0;
}


void MWMpiComm::exit(int exitval)
{
if (received.msg)
   delete received.msg;
if (curr_send_msg)
   delete curr_send_msg;
for (unsigned int i=0; i<outgoing_msgs.size(); i++)
     delete outgoing_msgs[i];
if (comm_initialized_locally)
   MPI_Finalize();
}



int MWMpiComm::setup ( int& argc, char **&argv, int *my_id, int *master_id,
				MPI_Comm comm_ ) 
{
int status;
int running;
MPI_Initialized(&running);
if (!running) {
   comm_initialized_locally=true;
   status = MPI_Init(&argc,&argv);
   if (status) {
      MWprintf(10, "MPI_Init failed!  Code= %d\n", status);
      }
   comm = MPI_COMM_WORLD;
   }
else
   comm = comm_;

//
// Set rank and size
//
status = MPI_Comm_rank(comm,&rank);
if (status)
   MWprintf(10, "MPI_Comm_rank failed!  Code= %d\n", status);
status = MPI_Comm_size(comm,&size);
if (status)
   MWprintf(10, "MPI_Comm_size failed!  Code= %d\n", status);
*master_id = 0;
*my_id = rank;

#if 0
//
// Set ioProc and iDoIO
//
// Note: it's not clear that we need to do this, so I've uncommented it
//	for now.
//
int flag,result;
int* mpiIOP;
 errorCode = MPI_Attr_get(MPI_COMM_WORLD,MPI_IO,&mpiIOP,&flag);
if (errorCode || !flag)
   MWprintf(10,"MPI_Attr_get(MPI_IO) failed, code %d",errorCode);
MPI_Comm_compare(comm, MPI_COMM_WORLD, &result);
if (result==MPI_IDENT || result==MPI_CONGRUENT) // no mapping of MPI_IO reqd.
   ioProc = *mpiIOP;
else { // MPI_IO can only be mapped to comm in special cases
   int world_rank;
   errorCode = MPI_Comm_rank(MPI_COMM_WORLD,&world_rank);
   if (errorCode)
      Warning(errmsg("MPI_Comm_rank failed, code %d",errorCode));
   if (*mpiIOP == world_rank) // MPI_IO processor is this processor
      ioProc = rank; // MPI_IO in MPI_COMM_WORLD maps to rank in comm
   else
      ioProc = size; // no mapping of MPI_IO to comm is possible.  Assign size
                     // so that reduce works properly.
   }
int elected;
reduce(&ioProc,&elected,1,MPI_INT,MPI_MIN,0);
ioProc = elected;
broadcast(&ioProc,1,MPI_INT,0);
if ((ioProc < 0) || (ioProc >= size))
   ioProc = 0;
iDoIO = (rank == ioProc);
#endif

//
// Create message objects
//
received.msg = new MsgStatus(bufsize);
if (rank == 0) {
   outgoing_msgs.resize(size);
   for (int i=0; i<size; i++)
     outgoing_msgs[i] = new MsgStatus(bufsize);
   }
curr_send_msg = new MsgStatus(bufsize);

return 0; 
};


int MWMpiComm::start_worker (MWWorkerID<MWMpiComm>* w) 
{ 
//
// TODO
//
return -1;
};


void MWMpiComm::config_worker(MWWorkerID<MWMpiComm>* w )
{
if (!w) return;
if (rank == 0)
   w->set_id1 ( next_worker++ );
else
   w->set_id1 ( rank );
w->set_id2 ( 0 );
//
// Do we need to set these?  MPI can be run across multiple
// architectures, but I'm not sure that we can query to get
// architecture info once it is running.
//
w->set_arch ( 0 );
w->set_exec_class(0);
}


int MWMpiComm::initsend ( int encoding  )
{ 
//
// Nothing to do, since curr_send_msg is initialized before.
//
curr_send_msg->reset();
return 0;
}


int MWMpiComm::send ( int to_whom, int msgtag ) 
{ 
assert(to_whom >= 0);
assert(to_whom < size);

// TODO?
//curr_send_msg->id = num_send_msgs++;
//pack(curr_send_msg->id,1);

MPI_Isend( (void*)(&(curr_send_msg->buf[0])),
	curr_send_msg->buf.size(),
	MPI_PACKED,
	to_whom,
	msgtag,
	comm,
	&(curr_send_msg->request));
MWprintf(90,"MPI_Isend - sending to=%d tag=%d, len=%d\n",to_whom,
			msgtag, curr_send_msg->buf.size());
if (rank == 0)
   swap(curr_send_msg, outgoing_msgs[to_whom]);
return 0;
}


bool MWMpiComm::recv( int from_whom, int msgtag, int& buf_id ) 
{ 
MWprintf(90,"Receive requested: from=%d tag=%d\n",from_whom,msgtag);

buf_id=-1;
bool flag=false;
MPI_Status status;

int test_val;
if (from_whom < 0)
   from_whom = MPI_ANY_SOURCE;
if (msgtag < 0)
   msgtag = MPI_ANY_TAG;
test_val = MPI_Probe(from_whom, msgtag, comm, &status);

MWprintf(90,"Probed message: from=%d tag=%d error=%d\n", status.MPI_SOURCE,
					status.MPI_TAG, status.MPI_ERROR);
if (test_val == MPI_SUCCESS) {
   //
   // Receive the buffer .. if needed
   //
   int tmp;
   MPI_Get_count(&status, MPI_PACKED, &tmp);
   if (received.msg->buf.size() < (unsigned int)tmp)
      received.msg->buf.resize(tmp);
   //
   // Receive the message
   //
   MPI_Status recv_status;
   test_val = MPI_Recv((void*)(&(received.msg->buf[0])),
	  received.msg->buf.size(),
	  MPI_PACKED,
	  status.MPI_SOURCE,
	  status.MPI_TAG,
	  comm, &recv_status);
   if (test_val != MPI_SUCCESS) {
      MWprintf(10,"ERROR - MPI_Recv returned %d\n", test_val);
      exit(1);
      }
   //
   // Process new message.
   //
   // Note: we assume here that MW receives a message and 
   // processes it before calling recv again!
   //
   MPI_Get_count(&recv_status, MPI_PACKED, &tmp);
   received.msg->nbytes = tmp;
   received.msg->index = 0;
   received.msg->id = num_recv_msgs++;
   received.index=0;
   flag=true;
   buf_id = received.msg->id;
   received.tag = recv_status.MPI_TAG;
   received.source = recv_status.MPI_SOURCE;
   MWprintf(90,"MPI_Recv results: tag=%d source=%d len=%d\n",received.tag,
			received.source, received.msg->nbytes);
   }
else {
   MWprintf(10,"ERROR - MPI_Probe returned %d\n", test_val);
   exit(1);
   }

return flag;
}


int MWMpiComm::bufinfo ( int buf_id, int *len, int *tag, int *from ) 
{ 
assert(buf_id == received.msg->id);

*len  = received.msg->buf.size();
*tag  = received.tag;
*from = received.source;

return 0;
}



void MWMpiComm::who ( int *wh )
{ unpack ( wh ); }


void MWMpiComm::get_packinfo(int& bytes_packed_, int& bytes_unpacked_)
{
bytes_packed_ = bytes_packed;
bytes_unpacked_ = bytes_unpacked;
}


///
int MWMpiComm::pack_explicit( const char* bytes,  int nitem)
{
curr_send_msg->resize(nitem);
memcpy(&(curr_send_msg->buf[curr_send_msg->index]), bytes, nitem*sizeof(char));
curr_send_msg->index += nitem;
bytes_packed += nitem;
return 0;
}



int MWMpiComm::pack(char* data)
{
//
// Pack the length
//
int len = strlen(data);
pack(&len);
MWprintf(90,"Packing char len=%d\n",len);
//
// Call do_pack
//
do_pack(data,len,1);

return 0;
}


int MWMpiComm::pack( const char* data)
{
//
// Pack the length
//
int len = strlen(data);
pack(&len);
MWprintf(90,"Packing char len=%d\n",len);

//
// call do_pack
//
do_pack(data,len,1);
/*** This doesn't seem to work!
//
// Resize the pack buffer if needed
//
int size;
MPI_Pack_size(len, mpi_datatype<char>(), MPI_COMM_WORLD, &size);
curr_send_msg->resize(size);
//
// Send the message
//
MPI_Pack((void*)data, len, mpi_datatype<char>(), 
		&(curr_send_msg->buf[curr_send_msg->index]),
		curr_send_msg->buf.size(), &(curr_send_msg->index), comm);
bytes_packed += size;
**/

return 0;
}



int MWMpiComm::unpack(char* data)
{
//
// Unpacking length
//
int len;
unpack(&len);
MWprintf(90,"Unpacking string len=%d\n",len);

do_unpack(data,len,1);
/*** This doesn't seem to work
//
// Unpack the string itself
//
int start = received.index;
MPI_Unpack( &(received.msg->buf[0]),
		received.msg->nbytes,
		&(received.index),
		(void*)data, 
		len, mpi_datatype<char>(), 
		comm);

bytes_unpacked += (received.index - start);
MWprintf(90,"Unpacked string \"%s\" %c %c bytes=%d\n",data,data[0],data[1],
			(received.index - start));
***/
return 0;
}


int MWMpiComm::unpack(std::string& str)
{
//
// Unpacking length
//
int len;
unpack(&len);
MWprintf(90,"Unpacking string len=%d\n",len);

str.resize(len+1);
do_unpack(str.data(),len,1);
return 0;
}


int MWMpiComm::init_workers()
{
int status = MPI_Comm_size(comm,&size);
if (status)
   MWprintf(10, "MPI_Comm_size failed!  Code= %d\n", status);
return (size-1);
}

#endif
