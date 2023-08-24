//
// MWRMComm.h
//

#include <iostream>

using namespace std;

#include "MWRMComm.h"
#include "MWprintf.h"


MWRMComm::MWRMComm()
{
tempnum_executables = -1;
worker_checkpointing = false;
}


MWRMComm::~MWRMComm()
{
int i;

if ( arch_class_attributes.size() > 0 ) {
   int num = arch_class_attributes.size();
   for ( i = 0; i < num; i++ )
     if ( arch_class_attributes[i] )
	delete arch_class_attributes[i];
   }

if ( worker_executables.size() > 0 ) {
   int num = worker_executables.size();
   for ( i = 0; i < num; i++ )
     if ( worker_executables[i] ) {
	delete worker_executables[i]->executable;
	delete worker_executables[i]->executable_name;
	delete worker_executables[i];
	}
   }
}


void MWRMComm::exit( int exitval )
{ ::exit ( exitval ); }


int MWRMComm::write_checkpoint(ostream& os)
{
MWprintf ( 80, "Writing checkpoint for MWPvmRC layer.\n" );

os << exec_class_target_num_workers.size() << endl;
os << arch_class_attributes.size() << endl;
for (unsigned int i = 0 ; i<arch_class_attributes.size() ; i++ ) {
  if ( arch_class_attributes[i] )
     os << "1 " << strlen(arch_class_attributes[i])
		<< arch_class_attributes[i]  << endl;
  else
     os << 0 << endl;
  }

os << worker_executables.size() << endl;
for (unsigned int i = 0; i < worker_executables.size(); i++ ) {
  os <<  worker_executables[i]->arch_class << " " << 
	 worker_executables[i]->exec_class << " " <<
	 worker_executables[i]->executable << " " <<
	 worker_executables[i]->executable_name << endl;
  if ( worker_executables[i]->attributes != NULL )
     os << "1 " << strlen(worker_executables[i]->attributes) << " " <<
			worker_executables[i]->attributes  << endl;
  else
     os << 0 << endl;
  }

for (unsigned int i = 0; i < exec_class_target_num_workers.size(); i++ )
  os << exec_class_target_num_workers[i] << " ";
os << endl;

write_RMstate ( os );
return 0;
}



int MWRMComm::read_checkpoint(istream& is)
{
int tmp;
MWprintf ( 50, "Reading checkpoint in MWPvmRC layer.\n" );

is >> tmp;
set_num_exec_classes(tmp);
is >>  tmp;
set_num_arch_classes(tmp);

MWprintf ( 50, "Target num workers: %d    Num arches %d.\n", 
				target_num_workers, tmp );

for (unsigned int i = 0; i < arch_class_attributes.size(); i++ ) {
  is >> tmp;
  if ( tmp == 1 ) {
     is >> tmp;
      arch_class_attributes[i] = new char[tmp+1];
      is.read ( arch_class_attributes[i], tmp + 1 );
      }
  else
     arch_class_attributes[i] = NULL;
  }

is >> tmp;
set_num_executables(tmp);

MWprintf ( 50, "%d Worker executables:\n", tmp );

for (unsigned int i = 0 ; i < worker_executables.size(); i++ ) {
  worker_executables[i] = new struct RMC_executable;
  worker_executables[i]->executable = new char[_POSIX_PATH_MAX];
  worker_executables[i]->executable_name = new char[_POSIX_PATH_MAX];
  is >> worker_executables[i]->arch_class >>
	worker_executables[i]->exec_class >>
	worker_executables[i]->executable >>
	worker_executables[i]->executable_name;
  is >> tmp;
  if ( tmp == 1 ) {
     is >> tmp;
     worker_executables[i]->attributes = new char[tmp+1];
     is.read ( worker_executables[i]->attributes, tmp + 1 );
     }
  else
     worker_executables[i]->attributes = NULL;
  }

for (unsigned int i = 0; i < exec_class_target_num_workers.size(); i++ ) {
  is >> exec_class_target_num_workers[i];
  target_num_workers += exec_class_target_num_workers[i];
  // TODO:::   MW_exec_class_num_workers[i] = 0;
  }

read_RMstate ( is );
return 0;
}



void MWRMComm::set_num_exec_classes ( int num )
{
exec_class_target_num_workers.resize(num);
// TODO MW_exec_class_num_workers = new int[num];
for ( int i = 0; i < num; i++ ) {
  exec_class_target_num_workers[i] = 0;
  // TODO MW_exec_class_num_workers[i] = 0;
  }
target_num_workers=0;
}


void MWRMComm::set_num_arch_classes ( int num )
{
arch_class_attributes.resize(num);
for ( int i = 0; i < num; i++ )
  arch_class_attributes[i] = NULL;
}


void MWRMComm::set_arch_class_attributes (unsigned int arch_class, char *attr )
{
if ( arch_class >= arch_class_attributes.size() )
   return;

if (arch_class_attributes[arch_class])
   delete arch_class_attributes[arch_class];
arch_class_attributes[arch_class] = new char[strlen(attr)+1];
strcpy ( arch_class_attributes[arch_class], attr );
}


void MWRMComm::set_num_executables ( int num )
{
tempnum_executables = 0;
worker_executables.resize(num);
for ( int i = 0; i < num; i++ )
  worker_executables[i] = NULL;
}


void MWRMComm::add_executable( int exec_class, unsigned int arch_class, 
					char *exec_name, char *requirements )
{
if ( !exec_name )
   return;

if( arch_class >= arch_class_attributes.size() ) {
  MWprintf( 10, "set_worker_attributes(): incrementing num_arches to %d\n", 
							arch_class + 1 );
  arch_class_attributes.resize(arch_class_attributes.size()+1);
  arch_class_attributes[arch_class] = 0;
  }

MWprintf(31, "tempnum_executables = %d\n", tempnum_executables);

worker_executables[tempnum_executables] = new struct RMC_executable;
worker_executables[tempnum_executables]->arch_class = arch_class;
worker_executables[tempnum_executables]->exec_class = exec_class;
worker_executables[tempnum_executables]->executable = new char [ strlen(exec_name) + 1 ];
strcpy ( worker_executables[tempnum_executables]->executable, exec_name );
worker_executables[tempnum_executables]->executable_name = process_executable_name ( worker_executables[tempnum_executables]->executable, exec_class, arch_class );
if ( !requirements )
   worker_executables[tempnum_executables]->attributes = NULL;
else
   {
   worker_executables[tempnum_executables]->attributes = new char [ strlen(requirements) + 1 ];
   strcpy ( worker_executables[tempnum_executables]->attributes, requirements );
  }
tempnum_executables++;
}


char* MWRMComm::process_executable_name( char *exec_name, int ex_cl, int ar_cl )
{
char *newone = new char[strlen(exec_name) + 1];
strcpy ( newone, exec_name );
return newone;
}


void MWRMComm::set_target_num_workers ( int num_workers, int exec_class)
{
if ( exec_class_target_num_workers.size() <= 0 ) {
   exec_class_target_num_workers.resize(1);
   exec_class_target_num_workers[0] = 0;
   }

if ( exec_class < 0 ) {
   exec_class_target_num_workers[0] = num_workers;
   target_num_workers += exec_class_target_num_workers[0];
   return;
   }

target_num_workers += num_workers - exec_class_target_num_workers[exec_class];
exec_class_target_num_workers[exec_class] = num_workers;
}


void MWRMComm::set_worker_checkpointing( bool wc )
{
if (wc == true)
   MWprintf( 10, "Warning!  Worker checkpointing not available in this CommRM implementation\n" );
worker_checkpointing = false;
}

