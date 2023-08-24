//
//
//

#include <cstring>
#include "MWIndRC.h"
#include "MWWorkerID.h"
#include <assert.h>
//MWRMComm * GlobalRMComm = new MWIndRC ( );
//MWRMComm * MWTask::RMC = GlobalRMComm;
//MWRMComm * MWDriver::RMC = GlobalRMComm;
//MWRMComm * MWWorker::RMC = GlobalRMComm;

unsigned int MWIndRC::SendBuf::buf_size = 1024*1024*64;


MWIndRC::MWIndRC() 
{
last_tag = UNDEFINED;
bytes_packed=0;
bytes_unpacked=0;
// memset ( worker_requirements, 0, (16 * 1024 ));
curr_buf_id = 0;
}


int MWIndRC::setup ( int& argc, char**& argv, int *my_id, int *master_id ) 
{
*my_id = myid = 2;
*master_id = masterid = 0;
return 0; 
};


int MWIndRC::start_worker ( MWWorkerID<MWIndRC>* w ) 
{ 
return -1;
};


void MWIndRC::config_worker(MWWorkerID<MWIndRC>* w )
{
if (!w) return;
w->set_id1 ( 2 );
w->set_id2 ( 0 );
w->set_arch ( 0 );
w->set_exec_class(0); 	// XXX should have done this!
}


int MWIndRC::initsend ( int encoding  )
{ 
ind_sendbuf.num_sent = 0;
return 0;
}


int MWIndRC::send ( int to_whom, int msgtag ) 
{ 
last_tag = msgtag; 
if (to_whom == masterid)
   curr_from_id = -1;
else
   curr_from_id = masterid;
return 0;
}


bool MWIndRC::recv ( int from_whom, int msgtag, int& buf_id ) 
{ 
buf_id = -1;
if (last_tag == UNDEFINED)
   return false;
if (from_whom != curr_from_id)
   return false;

memcpy ( ind_recvbuf.buf, ind_sendbuf.buf, ind_sendbuf.num_sent * sizeof(char) );
buf_id = ++curr_buf_id;
curr_buf_tag = last_tag;
curr_buf_len = ind_sendbuf.num_sent;

ind_recvbuf.num_sent = 0;
last_tag = UNDEFINED;
return true;
}


int MWIndRC::bufinfo ( int buf_id, int *len, int *tag, int *from ) 
{ 
assert(buf_id == curr_buf_id);

*tag = curr_buf_tag; 
*from=2;
*len = curr_buf_len;

return 0;
}


void MWIndRC::who ( int *wh )
{ unpack ( wh, 1, 1 ); }


void MWIndRC::get_packinfo(int& bytes_packed_, int& bytes_unpacked_)
{
bytes_packed_ = bytes_packed;
bytes_unpacked_ = bytes_unpacked;
}


int MWIndRC::pack ( const char *bytes,         int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( char ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &bytes[ i * stride ], sizeof(char) );
  ind_sendbuf.num_sent += sizeof(char);
  num_sent += sizeof(char);
  }
return 0;
}


int MWIndRC::pack ( float *f,            int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( float ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &f[ i * stride ], sizeof(float) );
  ind_sendbuf.num_sent += sizeof(float);
  num_sent += sizeof(float);
  }
return 0;
}


int MWIndRC::pack ( double *d,           int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( double ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &d[ i * stride ], sizeof(double) );
  ind_sendbuf.num_sent += sizeof(double);
  num_sent += sizeof(double);
  }
return 0;
}


int MWIndRC::pack ( int *in,              int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( int ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &in[ i * stride ], sizeof(int) );
  ind_sendbuf.num_sent += sizeof(int);
  num_sent += sizeof(int);
  }
return 0;
}
		

int MWIndRC::pack ( unsigned int *ui,    int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( unsigned int ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &ui[ i * stride ], sizeof(unsigned int) );
  ind_sendbuf.num_sent += sizeof(unsigned int);
  num_sent += sizeof(unsigned int);
  }
return 0;
}


int MWIndRC::pack ( short *sh,           int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( short ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &sh[ i * stride ], sizeof(short) );
  ind_sendbuf.num_sent += sizeof(short);
  num_sent += sizeof(short);
  }
return 0;
}


int MWIndRC::pack ( unsigned short *ush, int nitem, int stride)
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( unsigned short ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &ush[ i * stride ], sizeof(unsigned short) );
  ind_sendbuf.num_sent += sizeof(unsigned short);
  num_sent += sizeof(unsigned short);
  }
return 0;
}


int MWIndRC::pack ( long *l,             int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( long ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &l[ i * stride ], sizeof(long) );
  ind_sendbuf.num_sent += sizeof(long);
  num_sent += sizeof(long);
  }
return 0;
}


int MWIndRC::pack ( unsigned long *ul,   int nitem, int stride )
{
int num_sent = ind_sendbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  // ASSERT ( num_sent + sizeof ( unsigned long ) <= 1024 * 64 );
  memcpy ( &(ind_sendbuf.buf[num_sent]), &ul[ i * stride ], sizeof(unsigned long) );
  ind_sendbuf.num_sent += sizeof(unsigned long);
  num_sent += sizeof(unsigned long);
  }
return 0;
}


int MWIndRC::pack ( char *string )
{
int num_sent = ind_sendbuf.num_sent;
// ASSERT ( num_sent + sizeof(int) + sizeof( char ) * strlen (string) <= 1024 * 64 );

int len = strlen(string);
memcpy ( &(ind_sendbuf.buf[num_sent]), &len, sizeof(int) );
num_sent += sizeof(int);

memcpy ( &(ind_sendbuf.buf[num_sent]), string, len * sizeof(char) );
num_sent += sizeof(char) * len;

ind_sendbuf.num_sent = num_sent;
return 0;
}


int MWIndRC::pack ( const char *string )
{
int num_sent = ind_sendbuf.num_sent;
// ASSERT ( num_sent + sizeof(int) + sizeof( char ) * strlen (string) <= 1024 * 64 );

int len = strlen(string);
memcpy ( &(ind_sendbuf.buf[num_sent]), &len, sizeof(int) );
num_sent += sizeof(int);

memcpy ( &(ind_sendbuf.buf[num_sent]), string, len * sizeof(char) );
num_sent += sizeof(char) * len;

ind_sendbuf.num_sent = num_sent;
return 0;
}


int MWIndRC::unpack ( char *bytes, int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  char temp;
  memcpy ( &temp,&(ind_recvbuf.buf[num_sent]), sizeof(char) );
  ind_recvbuf.num_sent += sizeof(char);
  num_sent += sizeof(char);
  bytes[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( float *f, int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  float temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(float) );
  ind_recvbuf.num_sent += sizeof(float);
  num_sent += sizeof(float);
  f[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( double *d,           int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  double temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(double) );
  ind_recvbuf.num_sent += sizeof(double);
  num_sent += sizeof(double);
  d[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( int *i,              int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int ii = 0; ii < nitem; ii++ ) {
  int temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(int) );
  ind_recvbuf.num_sent += sizeof(int);
  num_sent += sizeof(int);
  i[ii * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( unsigned int *ui,    int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  unsigned int temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(unsigned int) );
  ind_recvbuf.num_sent += sizeof(unsigned int);
  num_sent += sizeof(unsigned int);
  ui[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( short *sh,           int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  short temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(short) );
  ind_recvbuf.num_sent += sizeof(short);
  num_sent += sizeof(short);
  sh[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( unsigned short *ush, int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  unsigned short temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(unsigned short) );
  ind_recvbuf.num_sent += sizeof(unsigned short);
  num_sent += sizeof(unsigned short);
  ush[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( long *l,             int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  long temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(long) );
  ind_recvbuf.num_sent += sizeof(long);
  num_sent += sizeof(long);
  l[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( unsigned long *ul,   int nitem, int stride )
{
int num_sent = ind_recvbuf.num_sent;
for ( int i = 0; i < nitem; i++ ) {
  unsigned long temp;
  memcpy ( &temp, &(ind_recvbuf.buf[num_sent]), sizeof(unsigned long) );
  ind_recvbuf.num_sent += sizeof(unsigned long);
  num_sent += sizeof(unsigned long);
  ul[i * stride] = temp;
  }
return 0;
}


int MWIndRC::unpack ( char *string )
{
int num_sent = ind_recvbuf.num_sent;
int len;
memcpy ( &len, &(ind_recvbuf.buf[num_sent]), sizeof(int) );
ind_recvbuf.num_sent += sizeof(int);
num_sent += sizeof(int);
memcpy ( string, &(ind_recvbuf.buf[num_sent]), len * sizeof(char) );
ind_recvbuf.num_sent += sizeof(char) * len;
num_sent += sizeof(char) * len;
string[len] = '\0';
return 0;
}


int MWIndRC::unpack ( std::string& str )
{
int num_sent = ind_recvbuf.num_sent;
int len;
memcpy ( &len, &(ind_recvbuf.buf[num_sent]), sizeof(int) );
str.resize(len+1);
ind_recvbuf.num_sent += sizeof(int);
num_sent += sizeof(int);
memcpy ( (void*)str.data(), &(ind_recvbuf.buf[num_sent]), len * sizeof(char) );
ind_recvbuf.num_sent += sizeof(char) * len;
num_sent += sizeof(char) * len;
str[len] = '\0';
return 0;
}


