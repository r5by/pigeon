///////////////////////////////////Pigeon simulation code/////////////
//////////////////////////////////Defintion head file////////////////
//////////////////////////////////Setup parameters defined//////////

#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <iostream>
#include "List.cpp"
#include <string.h>
#include <assert.h>

using namespace std;

//system parameters
#define  ND  10                  //Number of job distributers (schedulers)
#define  NW  3000                //Number of workers (total number of workers in the cluster)
#define  NM  30                  //Number of masters (i.e., the total number of groups/masters in the cluster)
#define  MNW  100                //number of workers per group/master
#define short_cutoff  90.5811    //the short job cutoff time (in seconds)
#define DNUM   2                 //number of workers in a group reservered for short jobs only
#define FQW   100000                //fair queue weight (strick priority queuing with infinite FQW)

//propagation delay in seconds
#define net_delay     0.0005         //network propagation time between a distributer and a master or a worker (second)
#define loc_delay     0.0005         //network propagation time between a master and  a worker  (second)

///the following two parameters are simulation parameters 
#define NL 100                  //number of timing lists for workers (used to speed up simulation, not defined in the paper)
#define UN NW/NL                //number of workers per timing list, (used to speed up simulation, not defined in the paper)
#define  epsilon 10E-8          //infinity small number
#define  INF     2000000000     //infinity big number  
#define endtime  10000000       //max simulation time

/************global parameters ******/
long jnumber;                  //total number of jobs arrived             


/****************************************************************
define struct of job  (a task is also uses the same struct)
ID: job ID;                         fan: job fanout (number of tasks in a job)
etim: job executing time;           deadline: job deadline 
ltime: longest execution time of tasks in a job
nowtime: current time;              stime: job start time; 
dis: ID of distributor handling the job;    wrk: worker ID
msid: master ID for handing the job; 
type: job status type (used in a master or worker);   jb_type: short or long job
******************************************************************/
struct job {int ID; int fan; double etim; double ltime; double deadline; double nowtime; double stime; int dis; int wrk; int msid; int type; int jb_type;};


/*****************************************************************
record job jb, wk: list of worker executing/probing workers (some parameters may not be used )
type:  1 executing job 
nump: number of tasks, nume: number of executing workers, numc: number of completed workers
*****************************************************************/
struct jobworker{job jb; List<int> plst; List<double> let; int numt; int nume; int numc;}; 



/****************************************************
Random double number generator Function between 0 and 1.
***************************************************/
double get_random()
{
  unsigned int xx;
  double y;

  y=(double)rand()/(double)RAND_MAX;
  if(y==1.0) { y-=epsilon;}

  return y;
}


/*****************************************
A function to print job information
*******************************************/
void print_job(job j)
{
  cout<<" Job ID: "<<j.ID<<" Fan: "<<j.fan<<" Eexcution time: "<<j.etim<<" deadline: "<<j.deadline<<" worker: "<<" Longest task time: " <<j.ltime<<j.wrk<<endl;
  cout<<"Nowtime: "<<j.nowtime<<" start time: "<<j.stime<<" distributor: "<<j.dis<<" Master ID: "<<j.msid<<" type: "<<j.type<<endl;
}

/*****************************************
A function to print jobworker information
*******************************************/
void print_jobworker(jobworker xk)
{ print_job(xk.jb);  cout<<" numt:"<<xk.numt<<" executing: "<<xk.nume<<" complete "<<xk.numc<<" List: "<<endl;
 if(!xk.plst.Empty()){ xk.plst.SetToHead(); cout<<*xk.plst.Access()<<" ";
   while(xk.plst.Fore()) { cout<<*xk.plst.Access()<<" "; }
 }
 cout<<endl;
}
