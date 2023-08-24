/* MWprintf.C - The place for the MWPrintf stuff. */

#include <iostream>
#include <cstring>
#include <ctime>
#include <cstdio>

using namespace std;

#include <stdarg.h>
#include "MWprintf.h"

static int MWprintf_level = 50;
static int MWprintf_id=-1;
static ostream* MWprintf_os = &cout;

int set_MWprintf_id ( int id )
{
int foo = MWprintf_id;
MWprintf_id = id;
return foo;
}


int set_MWprintf_level ( int level )
{
int foo = MWprintf_level;
if ( (level<0) || (level>99) )
   MWprintf( 10, "Bad arg \"%d\" in set_MWprintf_level().\n", level );
else
   MWprintf_level = level;
return foo;
}


void MWprintf ( int level, const char *fmt, ... )
{
static bool printTime = true;
static char str[512]; 

level = (level <=99 ? level : 99);
if ( level > MWprintf_level )
   return;

if ( printTime ) {
   char *t;
   time_t ct = time(0);
   t = ctime( &ct );
   t[19] = '\0';
   (*MWprintf_os) << &t[11] << " ";
   if (MWprintf_id != -1)
      (*MWprintf_os) << "(" << MWprintf_id << ") ";
   }

va_list ap;
va_start( ap, fmt );
vsnprintf(str, 512, fmt, ap );
va_end( ap );
(*MWprintf_os) << str;
(*MWprintf_os).flush();

if ( fmt[strlen(fmt)-1] == '\n' ) 
   printTime = true;
else
   printTime = false;
}


void set_MWprintf_ostream(ostream& os)
{ MWprintf_os = &os; }
