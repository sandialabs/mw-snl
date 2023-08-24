//
// MWGroup.cpp
//

#include "MWGroup.h"


MWGroup::MWGroup ( int grps )
{
maxGroups = grps;
group = new bool[maxGroups];
for ( int i = 0; i < maxGroups; i++ )
  group[i] = false;
}


MWGroup::~MWGroup ( )
{ delete [] group; }


void MWGroup::join ( int num )
{
if ( num >= 0 && num < maxGroups )
   group[num] = true;
}


void MWGroup::leave ( int num )
{
if ( num >= 0 && num < maxGroups )
   group[num] = false;
}


bool MWGroup::belong ( int num )
{
if ( num >= 0 && num < maxGroups )
   return group[num];

return false;
}


bool MWGroup::doesOverlap ( MWGroup *grp )
{
for ( int i = 0; i < maxGroups; i++ ) {
  if ( belong ( i ) && grp->belong ( i ) )
     return true;
  }

return false;
}


void MWGroup::init ( int num )
{
if (num != maxGroups) {
   delete [] group;
   maxGroups = num;
   group = new bool [maxGroups];
   }
for (int i=0; i<maxGroups; i++)
  group[i] = false; 
}


void MWGroup::write_checkpoint (ostream& os)
{
int num = 0;
int i;
for ( i = 0; i < maxGroups; i++ )
  if ( belong ( i ) )
     num++;
os << num;

for ( i = 0; i < maxGroups; i++ )
  if ( belong ( i ) )
     os << i;
os << endl;
}


void MWGroup::read_checkpoint (istream& is)
{
int i, temp;
int gp;

is >> temp;
for ( i = 0; i < temp; i++ ) {
  is >> gp;
  join ( gp );
  }
}
