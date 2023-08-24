/* WorkerMain-nQueens.C

   The main() for the worker.  Simply instantiate the Worker_nQueens
   class and call go().

*/

#include "nQueensDriver.h"
#include "nQueensWorker.h"

#include "MWExecInd.h"
#include "MWExecMpi.h"


int main( int argc, char *argv[] ) 
{
set_MWprintf_level(99);
MWprintf(10,"Starting nqueens\n");

#ifdef USING_MPI
MWExec< nQueensDriver<MWMpiComm>, nQueensWorker<MWMpiComm>, MWMpiComm > driver;
MWAbstractDriver<MWMpiComm>::master_slowdown=2;
#else
MWExec< nQueensDriver<MWIndRC>, nQueensWorker<MWIndRC>, MWIndRC > driver;
#endif

driver.go(argc,argv);

MWprintf(10,"Finishing nqueens\n");
return 0;
}
