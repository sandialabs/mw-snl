//
// nQueensTask.h
//

#ifndef TASK_NQUEENS_H
#define TASK_NQUEENS_H

#include <vector>
#include "MWTask.h"

template <class CommType>
class nQueensWorker;

template <class CommType>
class nQueensDriver;


template <class CommType>
class nQueensTask : public MWTask<CommType> {

  friend class nQueensWorker<CommType>;

  friend class nQueensDriver<CommType>;

public:
    
    nQueensTask(MWAbstractDriver<CommType>* master, CommType* comm)
		: MWTask<CommType>(master,comm), N(-1), num(0) {}

    nQueensTask(MWAbstractDriver<CommType>* master, CommType* comm,  int N_, int num_ )
		: MWTask<CommType>(master,comm), N(N_), num(num_) {}

    void pack_work( void )
		{
		RMC->pack( &N );
		RMC->pack( &num );
		}
    
    void unpack_work( void )
		{
		RMC->unpack( &N );
		RMC->unpack( &num );
	 	results.resize(N);
		}
    
    void pack_results( void );
    
    void unpack_results( void );

protected:

    int N;
    int num;
    vector<int> results;

};


template <class CommType>
void nQueensTask<CommType>::pack_results( void ) 
{
int res;
if ( results.size() > 0 ) res = 1;
else res = 0;
RMC->pack ( &res );

if ( res == 1 )
   RMC->pack( &(results[0]), N );
}


template <class CommType>
void nQueensTask<CommType>::unpack_results( void ) 
{
int res;
RMC->unpack ( &res );

if ( res == 1 ) {
   results.resize(N);
   RMC->unpack( &(results[0]), N );
   }
}

#endif
