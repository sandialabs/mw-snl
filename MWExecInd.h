//
// MWExecInd.h
//

#ifndef __MWExecInd_h
#define __MWExecInd_h

#include "MWIndRC.h"
#include "MWExec.h"

class MWIndRC;


template <class MType, class WType>
class MWExec<MType, WType, MWIndRC> 
	: public _MWExec_base<MType, WType, MWIndRC>
{
public:

  /// Constructor.
  MWExec() 
		: _MWExec_base<MType, WType, MWIndRC>(true) {}

  ///
  void go(int argc, char* argv[]);

};


template <class MType, class WType>
void MWExec<MType, WType, MWIndRC>::go(int argc, char* argv[])
{
MWIndRC comm;

//
// Launch the Master/Worker logic
//
if (!master) {
   master = new MType;
   master->setRMC(&comm);
   }
if (!worker)
   worker = new WType(&comm);
master->set_local_worker(worker);
master->go(argc,argv);
}


#endif
