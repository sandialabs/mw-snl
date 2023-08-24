//
// nQueensWorker.h
//

#ifndef WORKER_NQUEENS_H
#define WORKER_NQUEENS_H

#include <vector>
using namespace std;

#include "MWWorker.h"
#include "nQueensTask.h"

template <class CommType>
class nQueensTask;


template <class CommType>
class nQueensWorker : public MWWorker<CommType>
{

  friend class nQueensTask<CommType>;
  
public:

    nQueensWorker(CommType* RMC_)
		: MWWorker<CommType>(RMC_), N(-1) 
		{ task = new nQueensTask<CommType>(0,RMC_); }

    MWReturn unpack_init_data( void )
		{
		RMC->unpack ( &N, 1 );
		RMC->unpack ( &partition_factor, 1 );
		return OK;
		}
    
    void execute_task( MWTask<CommType> * );

    void search_subspace ( vector<int>& perm, int N, int givenPerm, vector<int>&res );

    void isCorrectPermutation ( vector<int>& perm, int N, vector<int>& res );


protected:

     int N;
     int partition_factor;
};


template <class CommType>
void nQueensWorker<CommType>::execute_task( MWTask<CommType> *t ) 
{
MWprintf(1,"nqueens info: N=%d, pfact=%d\n", N,partition_factor);
vector<int> permutation(N);
int givenPerm = partition_factor >= N ? 0 : N - partition_factor;

nQueensTask<CommType> *tf = (nQueensTask<CommType> *) t;
int index = tf->num;
MWprintf(1,"nqueens info: tf=%d index=%d\n", tf, index);

for (int i = 0; i < givenPerm; i++ ) {
  permutation[i] = index % N;
  index = index / N;
  }

search_subspace ( permutation, N, givenPerm,tf->results  );
MWprintf(1,"nqueens execute_task finishes\n");
}


template <class CommType>
void nQueensWorker<CommType>::search_subspace ( vector<int>& permutation, int N, int givenPerm, vector<int>& res )
{
MWprintf(1,"nqueens search_subspace N=%d givenPerm=%d\n",N, givenPerm);
if ( givenPerm == N )
   return isCorrectPermutation ( permutation, N, res );

for ( int i = 0; i < N; i++ ) {
  permutation[givenPerm] = i;
  search_subspace ( permutation, N, givenPerm + 1, res );
  if (res.size() > 0) return;
  }
}


template <class CommType>
void nQueensWorker<CommType>::isCorrectPermutation ( vector<int>& permutation, int N, vector<int>& res )
{
MWprintf(1,"nqueens isCorrectPermutation\n");
for (int i = 0; i < N; i++ ) {
  for (int j = 0; j < N; j++ ) {
    if ( i == j ) continue;

    if ( permutation[i] == permutation[j] )
       return;

    if ( permutation[i] + ( j - i ) == permutation[j] )
       return;

    }
  }

MWprintf(1,"nqueens copying result\n");
for (int i = 0; i < N; i++ )
  res[i] = permutation[i];
}

#endif
