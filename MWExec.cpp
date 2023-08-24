//
// MWExec.cpp
//

#include <cstdlib>
#include "MWprintf.h"


extern "C" void MWsignal_handler(int sig)
{
MWprintf(1,"Entering MWsignal handler: signal=%d\n",sig);
abort();
}

