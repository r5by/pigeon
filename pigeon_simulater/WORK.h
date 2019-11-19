
//class file for worker class

#include "MASTER.h"

template <class DType>
class worker
{
  public: 
         worker();
        ~worker();
        void Set_ID(int n);     //set the ID of the worker
	void Set_MasterID(int n);  //set the master Id of the worker
        int Get_ID();           //Get the ID of worker
	int Get_MasterID();         //Get the master Id of the worker
	double NextTime();          //return next event time;
	void AddTimeList(job jb);   //add a task event to time list
	bool IsActive();            //return IsBusy (true if it is busy and false if it is idle)
        int Run_Worker(job &jb, job &jbb);       //Run worker event
        void Show_Data();            //show all iformation in the worker     

    private: 
        int id;              //ID of the worker
	int masterid;            //Master ID of the worker
        int IdAct;            //Job ID of the executing task
	bool IsBusy;              //indicate if the worker is busy or idle
        double nexttime;      //time for next event
        List<job> TM;         //List all events of a wroker
        
};


template <class DType>
worker<DType>::worker()
{   id=IdAct=masterid=-1; IsBusy=false; TM; nexttime=INF; }

template <class DType>
worker<DType>::~worker()
{  id=IdAct=masterid=-1; IsBusy=false;  TM.kill(); }

//set worker ID;
template <class DType>
void worker<DType>::Set_ID(int n) { id=n;}

//set Master ID
template <class DType>
void worker<DType>::Set_MasterID(int n) { masterid=n;}

//return worker ID
template <class DType>
int worker<DType>::Get_ID() { return id;}

//return master ID
template <class DType>
int worker<DType>::Get_MasterID() { return masterid;}

//true if active, false if idle
template <class DType>
bool worker<DType>::IsActive() { return IsBusy;}

//return next event time
template <class DType>
double worker<DType>::NextTime() {return nexttime;}

//add a new event into time list (sorted by event time)
template <class DType>
void worker<DType>::AddTimeList(job jb)
{
  double tm, tim;
  
  tm=jb.nowtime;

  if(TM.Empty()) { TM.EnQueue(jb); nexttime=tm; return; }

  TM.SetToHead(); tim=TM.Access()->nowtime;
  TM.SetToTail(); 
  if(tm>=TM.Access()->nowtime){ TM.Insert(jb); nexttime=tim;  return;}
  while(TM.Back()){ if(tm>TM.Access()->nowtime) {TM.Insert(jb);  nexttime=tim; return; }}

  TM.EnQueue(jb);  nexttime=tm;
  
}

/**************************************************************************************
run leaf node events (main funtion of class worker):                                          
0: receive a task                                                      
1: finish a task                               

Return: 10 complete a task, send it back to the job distributor and a notice to the Master
        0 therwise           
*******************************************************************************************/
template <class DType>
int worker<DType>::Run_Worker(job &jb, job &jbb)
{  int  i,j,et,tp;
   job jj;
   double tm;

    et=0;

   if(TM.Empty()) { nexttime=INF; return et;}

   TM.SetToHead();
   jj=*TM.Access();
   TM.Remove();

   tp=jj.type; 
   tm=jj.nowtime;
   jb=jj; jbb=jj;

   
  if(tp==0) { //receive a task, executing it and record its finish time as the next event in time list
     IdAct=jj.ID; IsBusy=true; 
     jj.nowtime+=jj.etim; jj.type=1; jj.wrk=id; jj.msid=masterid; jb=jj;
     AddTimeList(jj);
   }
  else if(tp==1) { //finish an task and send it back to the distributor and an idle notice to its master
     et=10; jbb.type=0; jbb.msid=masterid; jbb.wrk=id; IsBusy=false; IdAct=-1;
 }
 
   if(TM.Empty()) { nexttime=INF; }
   else { TM.SetToHead(); jj=*TM.Access(); nexttime=jj.nowtime; }

   return et;
}


//show worker information 
template <class DType>
void worker<DType>::Show_Data() {
  int i=0;
  job ts;

  cout<<"Parameters in Worker:"<<" ID: "<<id<<" MasterID: "<<masterid<<" Is active: "<<IsBusy<<" Work Job ID: "<<IdAct<<endl;

  cout<<"List all Event: "<<endl;
  if(!TM.Empty()) { TM.SetToHead(); ts=*TM.Access(); print_job(ts); 
     while(TM.Fore()) { ts=*TM.Access(); print_job(ts);}
   }
   else { cout<<" No event for the worker."<<endl;}
  

   return;
}


