//
// MWGroup.h
//
// Implements a fixed-size group.  This could be done equally well
// with a STL class like bvector, but it isn't clear how standard that
// class is, and the set operations on it are not defined.
//

#ifndef __MWGroup_h
#define __MWGroup_h

#include <iostream>

using namespace std;

class MWGroup
{
public:
  ///
  MWGroup ( int grp );
  ///
  ~MWGroup ( );
  ///
  void join ( int num );
  ///
  void leave ( int num );
  ///
  bool belong ( int num );
  ///
  bool doesOverlap ( MWGroup *grp );
  ///
  void write_checkpoint (ostream& os);
  ///
  void read_checkpoint (istream& is);
  ///
  void init( int num);

protected:
  ///
  bool *group;
  ///
  int  maxGroups;
};

#endif
