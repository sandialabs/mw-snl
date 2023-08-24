//
// MWprintf.h
//
// A definition of the MWprintf functions
//

#ifndef __MWprintf_h
#define __MWprintf_h

#include <iostream>

/** @name MWPrintf
		This functions control the amount of printed information
		in MW.  This is controlled through "levels", where
		a level of 0 is the most important and 99 is the least 
		important.  You can set the debug level to only print 
		levels n and below.

		Yes, this *is* a global function.  However, everyone needs 
		to use it, and making it a static member of a class would 
		mean that you'd have to type something like 
		MWDriver::MWprintf(), which is awkward.
		
		Here's a proposed layout of how the numbers should work:
		\begin{itemize}
		\item 10 : Big errors, major events
		\item 20 : Statistics at the end, user results
		\item 30 : User-defined results, info, etc
		\item 40 : Hosts up/down, other minor events
		\item 50 : Checkpointing information
		\item 60 : Sending/Receiving work
		\item 70 : Misc messges...
		\item 80 : Pointers!
		\item 90 : Even more pointers, debugging info, etc
		\end{itemize}

		Remember, you've got 9 levels in between each of these, so
		feel free to be creative....
*/

//@{

/** A regular printf, with debugging level. */
void MWprintf ( int level, const char *fmt, ... );

/** Set the debug level for the MWprintf function.  The default
	upon startup is 50.
	@return The old level */
int set_MWprintf_level( int level );

/** Set the process id for the MWprintf function.  The default
	upon startup is -1, which is ignored.
	@return The old level */
int set_MWprintf_id( int id );

/** Set an alternative output stream instead of cout. */
void set_MWprintf_ostream(std::ostream& os);
//@}

#endif
