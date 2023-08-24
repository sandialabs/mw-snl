//
// nQueensDriver.h
//

#ifndef __nQueensDriver_h
#define __nQueensDriver_h

#include <math.h>
#include "MWDriver.h"
#include "nQueensTask.h"

/** The master class derived from the MWDriver class for this application.

    In particular, this application is a very simple one that solves
    the nQueens problem. It does this by dividing the search space into
    subspaces and distributing to each worker one subspace at a time
    to search.  The workers will work on that subspace and if a solution
    is found will return the solution. Once the solution is found the
    master will stop all the workers and exit.

    Also this example is void of any comments since it is a very simple example.

    @author Sanjeev Kulkarni, William Hart
*/

template <class CommType>
class nQueensDriver : public MWDriver<CommType> {

 public:

    nQueensDriver() : N(-1) {}

    MWReturn get_userinfo( int argc, char *argv[] );

    MWReturn setup_initial_tasks( vector<MWTask<CommType>*>& tasks );

    MWReturn act_on_completed_task( MWTask<CommType> * );

    MWReturn pack_worker_init_data( void );

    void printresults();

    MWTask<CommType>* allocate_task()
		{return new nQueensTask<CommType>(this,RMC);}

 private:

    int N;
    vector<int> solution;
    const static int partition_factor;

};



template <class CommType>
const int nQueensDriver<CommType>::partition_factor = 5;


template <class CommType>
typename nQueensDriver<CommType>::MWReturn nQueensDriver<CommType>::get_userinfo( int argc, char *argv[] ) 
{

/* File format of the input file (which is stdin)
     # arches worker_executables ...

 num_rowsA num_rowsB num_colsB
 A B C
*/

istream* istr = &cin;
ifstream ifstr;

if (argc > 1) {
   ifstr.open(argv[1]);
   istr = &ifstr;
   }

RMC->set_num_exec_classes ( 1 );

if (argc == 1)
   cout << "Please type the number of machines: " << endl;
int  numarches;
(*istr) >> numarches;

RMC->set_num_arch_classes( numarches );
for (int i=0 ; i<numarches ; i++ ) {
  if ( i == 0 )
     RMC->set_arch_class_attributes ( i, "((Arch==\"INTEL\") && (Opsys==\"LINUX\") )" );
  else
     RMC->set_arch_class_attributes ( i, "((Arch==\"INTEL\") && (Opsys==\"SOLARIS26\") )" );
  } 

if (argc == 1)
   cout << "Please type the number of executables: " << endl;
int num_exe;
(*istr) >> num_exe;
RMC->set_num_executables( num_exe );

for (int i = 0; i < num_exe; i++ ) {
  char exec[_POSIX_PATH_MAX];
  int j;
  if (argc == 1)
   cout << "Please type the executable name and id: " << endl;
  (*istr) >> exec >> j;
  MWprintf ( 30, " %s\n", exec );
  RMC->add_executable( 0, j, exec, "");
  } 

if (argc == 1)
   cout << "Please type the dimension of the problem: " << endl;
(*istr) >> N;
solution.resize(N);
set_checkpoint_frequency ( 100 );
RMC->set_target_num_workers( 10 );

return OK;
}


template <class CommType>
typename nQueensDriver<CommType>::MWReturn nQueensDriver<CommType>::setup_initial_tasks(vector<MWTask<CommType>*>& init_tasks) 
{
//
// Basically we have to tell MW Layer of the number of tasks and what they are 
//
int num_tasks = partition_factor >= N ? 1 : 
			(int)pow((double)(N - partition_factor), 
					    (double)(N - partition_factor));
init_tasks.resize(num_tasks);

//
// And now we make the Task_nQueens instances.  They will basically contain the 
// rows to operate upon.
//
for (int i = 0 ; i < num_tasks ; i++ ) {
  init_tasks[i] = new nQueensTask<CommType>(this, RMC, N, i );
  MWprintf ( 10, "Added the task %p\n", init_tasks[i] );
  }

return OK;
}


template <class CommType>
typename nQueensDriver<CommType>::MWReturn nQueensDriver<CommType>::act_on_completed_task( MWTask<CommType> *t ) 
{

int i;

nQueensTask<CommType>* tf = dynamic_cast<nQueensTask<CommType>*> ( t );

if ( tf->results.size() > 0 ) {
		// Result has been computed.
   for ( i = 0; i < N; i++ )
     solution[i] = tf->results[i];

   printresults ( );
   RMC->exit(0);
   ::exit(0);
   }
	
return OK;
}


template <class CommType>
typename nQueensDriver<CommType>::MWReturn nQueensDriver<CommType>::pack_worker_init_data( void ) 
{
int part = partition_factor;
// We pass the N.
RMC->pack( &N, 1 );
RMC->pack( &part, 1 );

return OK;
}


template <class CommType>
void nQueensDriver<CommType>::printresults() 
{
MWprintf ( 10, "The resulting Matrix is as follows\n");

for ( int i = 0; i < N; i++ )
  MWprintf ( 10, "%d ", solution[i] );
}

#endif
