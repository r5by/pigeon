
//class file for master class

#include "DIST.h"

template <class DType>
class master
{
  public: 
         master();
        ~master();
        void Set_ID(int n);     //set the ID of the master 
        int Get_ID();           //Get the ID of master 
	// Initially Set up low (sw) and high (hsw) workers  as idle workers in the master
        void SetWorker(List<int> sw, List<int> hsw);   
	double NextTime();              //return next event time;
	void AddTimeList(job jb);       //add job into event list
	int Run_Master(job &jb);      //Run Master event
        void Show_Data();        //show all data in the master     

    private: 
        int id;             //ID of the master
        int wfq;            //weight of fair queue
        double nexttime;   //time for next event of the master
	List<int> IW;         //list of low priority ideal workers 
	List<int> HIW;       //list of high priority workers (only for short jobs)
        List<job> LTQ;      //queue for low priority tasks 
	List<job> HTQ;      //queue for high priority tasks 
        List<job> TM;      //List all events of the  master
        
};

template <class DType>
master<DType>::master()
{   id=-1; wfq=0;  LTQ; HTQ; TM; IW; HIW; nexttime=INF; }

template <class DType>
master<DType>::~master()
{  id=-1;  LTQ.kill(); HTQ.kill(); TM.kill(); IW.kill(); HIW.kill();}

//set up master ID
template <class DType>
void master<DType>::Set_ID(int n) { id=n;}

//return master ID
template <class DType>
int master<DType>::Get_ID() { return id;}

//return next event time
template <class DType>
double master<DType>::NextTime() {return nexttime;}

//Set up high and low priority workers in the master
//Initially, all workers are idle workers
template <class DType>
void master<DType>::SetWorker(List<int> sw, List<int> hsw) { IW=sw; HIW=hsw;}

//add a new event into time list (sorted by event time)
template <class DType>
void master<DType>::AddTimeList(job jb)
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

/*********************************************************************************************************
run leaf node events (main master funtion):                                          
0: receive a task from a distributor                                                                                  
1: receive an idle worker notice from a worker

return 
20: send a task to a worker    
0: otherwise  
***********************************************************************************************************/
template <class DType>
int master<DType>::Run_Master(job &jb)
{  int  ii,kk,et,tp,pri;
   job jj;
   double tm;
   int key;

    et=0;

   if(TM.Empty()) { nexttime=INF; return et;}

   TM.SetToHead();
   jj=*TM.Access();
   TM.Remove();

   tp=jj.type; 
   tm=jj.nowtime;
   jb=jj;
   pri=jb.jb_type;

   if(tp==0 ) { // receive a task from a distributor
      if(!IW.Empty()) { //Low priority idle worker list is not empty
	       IW.SetToTail(); ii=*IW.Access();	 //get worker ii from the list
           IW.Remove(); jb.wrk=ii; et=20;  //directly send the task to worker ii
	  } 
	  //Low priority idle worker list is empty, but high priority idle worker list is not empty
	  //if the task is a short job task, get one worker from high priority worker idle list 
	  //send the task to the worker
	  else if(!HIW.Empty() && pri==1){  
			HIW.SetToTail(); ii=*HIW.Access(); //get worker ii from the list
			HIW.Remove(); jb.wrk=ii;  et=20;  //directly send the task to worker ii
	  }
	  else { //the task is not sent to an idle worker, add to corresponding task queue
         if(pri==0) { //low priority job, put to the low priority task queue (FIFO queue)
            if(!LTQ.Empty()) { LTQ.SetToTail(); LTQ.Insert(jj); } 
		    else { LTQ.EnQueue(jj); } 
		 }
         else {  //high priorty job, put to high priority task queue (FIFO queue)
            if(!HTQ.Empty()) { HTQ.SetToTail(); HTQ.Insert(jj); } //put the task to the low priority task queue
		    else { HTQ.EnQueue(jj); } 
           }
	  }
    }//end if(tp==0)
   else if(tp==1) { //receive an idle worker notice
     ii=jb.wrk; kk=ii%MNW;  
     if(HTQ.Empty() && LTQ.Empty()) //No task in the both task queue, add the worker to the corresponding idle list
	 { if(kk<DNUM) { HIW.EnQueue(ii); }  //high prirotiy worker, the first DNUM workers in a master are high priority (reserved) workers
	   else { IW.EnQueue(ii); }          //low priority worker
	 }
     else { //task queue is not empty, send a task to the worker
            key=0;
            if(!HTQ.Empty()) //first check high priority task queue
           { HTQ.SetToHead(); jb=*HTQ.Access(); 
             if(kk<DNUM || wfq<=FQW || LTQ.Empty()) { HTQ.Remove(); wfq++; key=1; et=20; }
           }
  
           if(key==0){ //no high priroty task, send a low priority task to the worker
               if(kk>=DNUM) {LTQ.SetToHead(); jb=*LTQ.Access(); LTQ.Remove(); wfq=0; et=20;} //send a task to an idle worker
	      else {HIW.EnQueue(ii); }
	    }
		jb.nowtime=tm; jb.type=0; jb.wrk=ii;
      }
   }
 
   if(TM.Empty()) { nexttime=INF; }
   else { TM.SetToHead(); jj=*TM.Access(); nexttime=jj.nowtime; }
   // if(id==0) { Show_Data();}
   
   return et;
}

//show worker information 
template <class DType>
void master<DType>::Show_Data() {
  int i=0;
  job ts;

  cout<<"Parameters in Master:"<<" ID: "<<id<<" Nexttime: "<<nexttime<<endl;
 
  cout<<"List all Event: "<<endl;
  if(!TM.Empty()) { TM.SetToHead(); ts=*TM.Access(); print_job(ts); 
     while(TM.Fore()) { ts=*TM.Access(); print_job(ts);}
   }
   else { cout<<" No event for the master."<<endl;}
  
  cout<<"List all tasks in the high prioirty queue:"<<endl;
  if(HTQ.Empty()) { cout<<" No task in the queue."<<endl;}
  else {  HTQ.SetToHead(); ts=*HTQ.Access(); print_job(ts);
          while(HTQ.Fore()) { ts=*HTQ.Access(); print_job(ts); }
  }
  
   cout<<"List all tasks in the low prioirty queue:"<<endl;
  if(LTQ.Empty()) { cout<<" No task in the queue."<<endl;}
  else {  LTQ.SetToHead(); ts=*LTQ.Access(); print_job(ts);
          while(LTQ.Fore()) { ts=*LTQ.Access(); print_job(ts); }
  }
  
  cout<<endl<<"List all idle workers under the master:"<<endl;
  if(IW.Empty()) { cout<<" No idle workers."<<endl;}
  else {  IW.SetToHead(); i=*IW.Access(); cout<<" Idle worker(s): "<<i<<" -" ;
          while(IW.Fore()) { i=*IW.Access(); cout<<i<<" - "; }
  }
  cout<<endl<<"%%%%%%%%%%%%%%%%%%%%%%%%%%End of show data in master%%%%%%%%%%%%%%%%%%%%%%%"<<endl;

   return;
}


