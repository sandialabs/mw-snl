
#include "MWTask.h"
#include "MWMpiComm.h"
#include "MWWorker.h"
#include "MWDriver.h"
#include "MWMaster.h"

MWTask<MWMpiComm>* foo;
MWAbstractMaster<MWMpiComm>* amaster;
MWWorker<MWMpiComm>* worker;

//#include "MWStats.h"
//MWStats<MWMpiComm>* stats;

MWMaster<MWMpiComm>* masster;

MWDriver< MWMaster<MWMpiComm>, MWWorker<MWMpiComm> >* tmp;

void FOO()
{
//tmp->go();
}
