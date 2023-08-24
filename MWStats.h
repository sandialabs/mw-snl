//
// MWStats.h
//

#ifndef __MWStats_h
#define __MWStats_h

#include <vector>
#include <cstdlib>
#include <cstdio>
#include <ctime>

#include <values.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "MW.h"
#include "MWWorkerID.h"


template <class TYPE>
inline double mean(std::vector<TYPE>& vec)
{
double sum=0;

std::vector<TYPE>::iterator curr = vec.begin();
std::vector<TYPE>::iterator last = vec.end();
while (curr != last) {
  sum += *curr;
  curr++;
  }

return (sum / vec.size());
}


template <class TYPE>
inline double var(std::vector<TYPE>& vec, double mean )
{
double ret=0;
double diff=0;

std::vector<TYPE>::iterator curr = vec.begin();
std::vector<TYPE>::iterator last = vec.end();
while (curr != last) {
  diff = *curr - mean;
  ret += ( diff * diff );
  curr++;
  }
if ( vec.size() > 1 )
   ret /= vec.size()-1;
return ret;
}


/** This class holds statistics from worker classes after before they go to die.
    It holds the workers until a makestats() call is made, at which
    point it generates some statistics, given the workers.  
*/
template <class Worker, class CommType>
class MWStatistics 
{
public:

  /// Default constructor
  MWStatistics();

  /// Destructor
  ~MWStatistics() {}

  /** Take the stats from this worker and store 
      them in the stats class.  This is done when the worker
      dies and during a checkpoint.
  */
  void gather(Worker* );

  /** This method is called at the end of a run.  It prints out some
      statistical information on that run.
  */
  void makestats();

  /** This function is just like makestats(), except it return the relative
      statistics information to the caller. 
  */
  void get_stats(double *average_bench,
		 double *equivalent_bench,
		 double *min_bench,
		 double *max_bench,
		 double *av_present_workers,
		 double *av_nonsusp_workers,
		 double *av_active_workers,
		 double *equi_pool_performance,
		 double *equi_run_time,
		 double *parallel_performance,
		 double *wall_time,
		 map<int,Worker*>& workers );

  /** Everytime we get a new bench factor, we update the max and 
      min ones if necessary.
  */
  void update_best_bench_results( double bres ); 

  /** We find that we should save stats across checkpoints.  Here
      we're given the ckpt ostream and the list of running workers.
      Basically, we write out the sum of the old stats we've got
      plus the stats of the running workers. */
  void write_checkpoint(ostream& os, map<int,Worker*>& workers );
  
  /// Read in stats information across the checkpoint
  void read_checkpoint(istream& is);

 private:

  ///
  double duration;

  ///
  double starttime;

  /// The total run time of the checkpoints before...
  double previoustime;

  /// Array of times that the workerIDs have been "up"
  std::vector<double> uptimes;

  /// Array of times that the workerIDs have been working
  std::vector<double> workingtimes;

  /// Array of times holding the CPU times of the workerIDs
  std::vector<double> cputimes;

  /// Array of times that the workerIDs have been suspended
  std::vector<double> susptimes;

  /**@name Normalized Statistics.  MW computes "normalized" statistics
     about the runs performance based on the relative speed of the machines
     that the user obtained during the course of the run.
  */
  //@{
  /// The total working time normalized by benchmark factor
  std::vector<double> workingtimesnorm;

  /// Min benchmark result -- the "slowest" machine we ever had.
  double max_bench_result;

  /// Max benchmark result -- the "faster" machine we ever had.
  double min_bench_result;

  /// The sum of benchmark factors for this vid
  std::vector<double> sumbenchmark;     

  /// The number of benchmar factors for this vid
  std::vector<int> numbenchmark;        
  //@}

  /** What is the maximum "virtuid ID" that we ever assigned to a worker.
      This amounts to asking how many workers did we ever have in ALL states
      at any one time during the course of the run.
  */
  int get_max_vid()
		{return uptimes.size();}

  ///
  void resize(int vid);
};


template <class Worker, class CommType>
MWStatistics<Worker,CommType>::MWStatistics()
 : duration(0),
   previoustime(0.0),
   max_bench_result(0.0),
   min_bench_result(MAXDOUBLE)
{
//
// start in the constructor...
//
struct timeval t;
gettimeofday ( &t, NULL );
starttime = (double) t.tv_sec + ( (double) t.tv_usec * 1e-6 );
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::gather(Worker* w)
{
if ( w == NULL ) {
   MWprintf ( 10, "Tried to gather NULL worker!\n" );
   return;
   }

int vid = w->get_vid();
resize(vid);
MWprintf ( 80, "Gathering: worker vid %d\n", vid );

//JTL 8/25/00
// We need to update the total time to include this time,
// because now we can call gather() without calling ended()

//  This is starting top get a bit kludgy, but the main
//  sticking point is that we don't want the "kill workers"
//  time in our efficiency numbers.

struct timeval t;
gettimeofday ( &t, NULL );
double now = (double) t.tv_sec + ( (double) t.tv_usec * (double) 1e-6 );
	
if ( w->start_time > 1.0 )
   w->total_time = now - w->start_time;
	
uptimes[vid] += w->total_time;
workingtimes[vid] += w->total_working;
susptimes[vid] += w->total_suspended;	
cputimes[vid] += w->cpu_while_working;

workingtimesnorm[vid] += (w->normalized_cpu_working_time);

sumbenchmark[vid] += (w->sum_benchmark);
numbenchmark[vid] += (w->num_benchmark);

// Resets all the timing and benchmark stats to 0 so we can call gather 
// more than once without affecting the stats validity
w->reset_stats();
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::makestats()
{
int numWorkers = get_max_vid();
double sumuptimes = 0.0;
double sumworking = 0.0;
double sumcpu = 0.0;
double sumsusp = 0.0;

double sumworknorm = 0.0;

double sum_sum_benchmark = 0.0;
int    sum_num_benchmark = 0;

MWprintf ( 20, "**** Statistics ****\n" );

MWprintf ( 20, "Dumping raw stats:\n" );
MWprintf ( 20, "Vid    Uptimes     Working     CpuUsage   Susptime\n" );
for (int i=0 ; i<numWorkers ; i++ ) {
  if ( uptimes[i] > 34560000 ) { /* 400 days! */
     MWprintf ( 20, "Found odd uptime[%d] = %12.4f\n", i, uptimes[i] );
     uptimes[i] = 0;
     }
		
  MWprintf ( 20, "%3d  %10.4f  %10.4f  %10.4f %10.4f %10.4f %10.4f %d \n",
	   i, 
	   uptimes[i], 
	   workingtimes[i], 
	   cputimes[i], 
	   susptimes[i], 
	   workingtimesnorm[i], 
	   sumbenchmark[i], 
	   numbenchmark[i] );


  sumuptimes += uptimes[i];
  sumworking += workingtimes[i];
  sumcpu += cputimes[i];
  sumsusp += susptimes[i];
  sumworknorm  += workingtimesnorm[i];
  sum_sum_benchmark += sumbenchmark[i];
  sum_num_benchmark += numbenchmark[i];
  }
	
// find duration of this run (+= in case of checkpoint)
struct timeval t;
gettimeofday ( &t, NULL );
double now = (double) t.tv_sec + ( (double) t.tv_usec * (double) 1e-6 );
duration = now - starttime;
duration += previoustime;  // so it's sum over all checkpointed work

MWprintf ( 20, "\n" );
MWprintf ( 20, "Number of (different) workers:            %d\n", 
		numWorkers);
MWprintf ( 20, "Wall clock time for this job:             %10.4f\n", 
		duration );
MWprintf ( 20, "Total time workers were alive (up):       %10.4f\n", 
		sumuptimes );
MWprintf ( 20, "Total wall clock time of workers:         %10.4f\n", 
		sumworking );
MWprintf ( 20, "Total cpu time used by all workers:       %10.4f\n", 
		sumcpu );
MWprintf ( 20, "Total time workers were suspended:        %10.4f\n", 
		sumsusp );

double average_bench = ( sum_sum_benchmark / sum_num_benchmark );
double equivalent_bench = ( sumworknorm / sumcpu );

MWprintf (20, "Averaged      benchmark   factor    :      %10.4f\n", 
		average_bench);
MWprintf (20, "Equivalent    benchmark   factor    :      %10.4f\n", 
		equivalent_bench);
MWprintf (20, "Minimum       benchmark   factor    :      %10.4f\n", 
		min_bench_result);
MWprintf (20, "Maximum       benchmark   factor    :      %10.4f\n\n", 
		max_bench_result);

double av_present_workers = ( sumuptimes / duration ) ;
double av_nonsusp_workers = ( ( sumuptimes - sumsusp) / duration ) ;
double av_active_workers  = ( ( sumcpu ) / duration ) ;

MWprintf (20, "Average Number Present Workers      :      %10.4f\n", 
		av_present_workers);
MWprintf (20, "Average Number NonSuspended Workers :      %10.4f\n", 
		av_nonsusp_workers);
MWprintf (20, "Average Number Active Workers       :      %10.4f\n", 
		av_active_workers);

double equi_pool_performance = ( equivalent_bench * av_active_workers );
double equi_run_time = ( sumworknorm );
double parallel_performance  = (( sumworking ) / ( sumuptimes - sumsusp)) ;

MWprintf (20, "Equivalent Pool Performance         :      %10.4f\n", 
		equi_pool_performance);
MWprintf (20, "Equivalent Run Time                 :      %10.4f\n\n", 
		equi_run_time);
MWprintf (20, "Overall Parallel Performance        :      %10.4f\n", 
		parallel_performance);
MWprintf (20, "Total Number of benchmark tasks     :      %10d\n\n",  
		sum_num_benchmark);

double uptime_mean = mean( uptimes );
double uptime_var  = var ( uptimes, uptime_mean );
double wktime_mean = mean( workingtimes );
double wktime_var  = var ( workingtimes, wktime_mean );
double cputime_mean= mean( cputimes );
double cputime_var = var ( cputimes, cputime_mean );
double sptime_mean = mean( susptimes );
double sptime_var  = var ( susptimes, sptime_mean );

MWprintf ( 20,"Mean & Var. uptime for the workers:       %10.4f\t%10.4f\n",
             uptime_mean, uptime_var );
MWprintf ( 20,"Mean & Var. working time for the worker:  %10.4f\t%10.4f\n",
             wktime_mean, wktime_var );
MWprintf ( 20,"Mean & Var. cpu time for the workers:     %10.4f\t%10.4f\n",
             cputime_mean, cputime_var );
MWprintf ( 20,"Mean & Var. susp. time for the workers:   %10.4f\t%10.4f\n",
             sptime_mean, sptime_var );
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::get_stats(double *average_bench,
			     double *equivalent_bench,
			     double *min_bench,
			     double *max_bench,
			     double *av_present_workers,
			     double *av_nonsusp_workers,
			     double *av_active_workers,
			     double *equi_pool_performance,
			     double *equi_run_time,
			     double *parallel_performance,
			     double *wall_time,
			     map<int,Worker*>& workers
			     )
{
int    numWorkers = get_max_vid();
double sumuptimes = 0.0;
double sumworking = 0.0;
double sumsusp = 0.0;
double sumcpu = 0.0;
double sumworknorm = 0.0;
double sum_sum_benchmark = 0.0;
int    sum_num_benchmark = 0;
for (int i=0 ; i<numWorkers ; i++ ) {
  if ( uptimes[i] > 34560000 ) { /* 400 days! */
     MWprintf ( 20, "Found odd uptime[%d] = %12.4f\n", i, uptimes[i] );
     uptimes[i] = 0;
     }
  sumuptimes += uptimes[i];
  sumworking += workingtimes[i];
  sumsusp += susptimes[i];
  sumcpu += cputimes[i];
  sumworknorm  += workingtimesnorm[i];
  sum_sum_benchmark += sumbenchmark[i];
  sum_num_benchmark += numbenchmark[i];
  }

//
// Find duration of this run (+= in case of checkpoint)
//
struct timeval t;
gettimeofday ( &t, NULL );
double now = (double) t.tv_sec + ( (double) t.tv_usec * (double) 1e-6 );
duration = now - starttime;
duration += previoustime;  // so it's sum over all checkpointed work

map<int,MWWorkerID<CommType>*>::iterator currWorker = workers.begin();
map<int,MWWorkerID<CommType>*>::iterator lastWorker = workers.end();
while (currWorker != lastWorker) {
  MWWorkerID<CommType> *tempw = currWorker->second;
  if ( tempw->start_time > 0.1 )
     sumuptimes += now - tempw->start_time;

  sumworking += tempw->total_working;
  sumsusp += tempw->total_suspended;	
  sumcpu += tempw->cpu_while_working;

  sumworknorm += (tempw->normalized_cpu_working_time);

  sum_sum_benchmark += (tempw->sum_benchmark);
  sum_num_benchmark += (tempw->num_benchmark);

  MWprintf(10, "Stats, id: %d\t%lf, %lf, %lf, %lf, %lf, %lf, %d\n",
	     tempw->id1, sumuptimes, sumworking, sumsusp, sumcpu, sumworknorm, 
 	     sum_sum_benchmark, sum_num_benchmark );
  currWorker++;
  }
	
*wall_time = duration;

*average_bench = ( sum_sum_benchmark / sum_num_benchmark );
*equivalent_bench = ( sumworknorm / sumcpu );
*min_bench = min_bench_result;
*max_bench = max_bench_result;
  
*av_present_workers = ( sumuptimes / duration ) ;
*av_nonsusp_workers = ( ( sumuptimes - sumsusp ) / duration ) ;
*av_active_workers  = ( ( sumcpu ) / duration ) ;

*equi_pool_performance = ( *equivalent_bench * *av_active_workers );
*equi_run_time = ( sumworknorm );
*parallel_performance  = (( sumworking ) / ( sumuptimes - sumsusp)) ;
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::update_best_bench_results ( double bres )
{
MWprintf(10,"bench = %lf\n", bres);

if (bres > max_bench_result)
   max_bench_result = bres;

if (bres < min_bench_result)
   min_bench_result = bres;
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::write_checkpoint(ostream& os, 
				map<int,Worker*>& workers )
{
/* Write out stats for each worker Vid. 
   We have to find the maximum Vid in the system;
   in both the old stats and the running workers.  Then, for
   each Vid, we have to sum the old stats + the new running
   worker stats, and write them out one at a time.... */

int tmp = max(get_max_vid(), (int)(workers.size()));

MWprintf ( 10, "In checkpointing -- sn: %d, run:%d, max:%d\n", 
		get_max_vid(), workers.size(), tmp );

std::vector<double> u(tmp);
std::vector<double> w(tmp);
std::vector<double> s(tmp);
std::vector<double> c(tmp);
std::vector<double> wn(tmp);
std::vector<double> sb(tmp);
std::vector<int>    nb(tmp);

for (int i=0 ; i<tmp ; i++ ) {
  u[i] = uptimes[i];
  w[i] = workingtimes[i];
  s[i] = susptimes[i];
  c[i] = cputimes[i];
  wn[i] = workingtimesnorm[i];
  sb[i] = sumbenchmark[i];
  nb[i] = numbenchmark[i];
  }

double up, wo, su, cp,  tn, sumb;
int numb;
  
map<int,MWWorkerID<CommType>*>::iterator currWorker = workers.begin();
map<int,MWWorkerID<CommType>*>::iterator lastWorker = workers.end();
while (currWorker != lastWorker) {
  MWWorkerID<CommType>* wkr = currWorker->second;
  wkr->ckpt_stats( &up, &wo, &su, &cp,  &tn, &sumb, &numb );
  u[wkr->get_vid()] += up;
  w[wkr->get_vid()] += wo;
  s[wkr->get_vid()] += su;
  c[wkr->get_vid()] += cp;
  wn[wkr->get_vid()] += tn;
  sb[wkr->get_vid()] += sumb;
  nb[wkr->get_vid()] += numb;
  }

struct timeval t;
gettimeofday ( &t, NULL );
double now = (double) t.tv_sec + ( (double) t.tv_usec * (double) 1e-6 );
  
char str[256];
sprintf (str, "%d %15.5f\n", tmp, ((now-starttime)+previoustime) );
os << str;
for (int i=0 ; i<tmp ; i++ ) {
  sprintf (str, "%15.5f %15.5f %15.5f %15.5f %15.5f %15.5f %d\n", 
	      		u[i], w[i], s[i], c[i] , wn[i], sb[i], nb[i]);
  os << str;
  }

sprintf (str, "%15.5f %15.5f\n", max_bench_result, min_bench_result);
os << str;
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::read_checkpoint(istream& is)
{
int n=0;
is >> n >> previoustime;
MWprintf ( 10, "num stats to read: %d prev: %f\n", n, previoustime );

for (int i=0 ; i<n ; i++ ) {
  is >> uptimes[i] >> workingtimes[i] >> susptimes[i] >>
			 cputimes[i] >>
			 workingtimesnorm[i] >>
			 sumbenchmark[i] >>
			 numbenchmark[i];

  MWprintf ( 10, "%d %15.5f %15.5f %15.5f %15.5f %15.5f %15.5f %d\n", 
			   i,
			   uptimes[i], 
			   workingtimes[i], 
			   susptimes[i], 
			   cputimes[i],
			   workingtimesnorm[i],
			   sumbenchmark[i],
			   numbenchmark[i]);
  }

is >> max_bench_result >> min_bench_result;
}


template <class Worker, class CommType>
void MWStatistics<Worker,CommType>::resize(int vid)
{
if (vid >= ((int)uptimes.size())) {
   uptimes.resize(vid);
   workingtimes.resize(vid);
   cputimes.resize(vid);
   susptimes.resize(vid);
   workingtimesnorm.resize(vid);
   sumbenchmark.resize(vid);
   numbenchmark.resize(vid);
   }
}

#endif
