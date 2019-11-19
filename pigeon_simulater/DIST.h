
//class file for a job distributor/scheduler

#include "job.h"

template <class DType>
class dist
{
  public:
            dist();
            ~dist();
            void Set_ID(int n);       //set the ID of distributor
            int Get_ID();             //Get the ID of distributor
	    int Num_Jobs();           // return number of jobs a distributer handling
            void Initial_Job(job jb, List<int> &lst);   //initail a job, return a list of masters to send tasks
            		
            double NextTime();            //return next event time;
            void AddTimeList(job j);      //add an event into event list
            int Run_Dist(jobworker &jw);  //main function for distributor     
            void Show_Data();             //show all data in the distributor     

  private:
           int id;               // ID of distributor          
           int num_job;          // number of handling jobs 
           double nexttime;      // time for next event
	   List<job> TM;         //event time list of distributor
           List<jobworker> LJ;   // list of jobs handled by the distributor
};

template <class DType>
dist<DType>::dist()
{ 
    id=0; num_job=0; TM; LJ;
    nexttime=INF; 
}

template <class DType>
dist<DType>::~dist()
{
 id=num_job=0; 
  TM.kill(); LJ.kill(); nexttime=INF; 
 }

//set distributor ID
template <class DType>
void dist<DType>::Set_ID(int n) { id=n;}

//return distributor ID
template <class DType>
int dist<DType>::Get_ID() { return id;}

//return number of jobs handling by the distributor
template <class DType>
int dist<DType>::Num_Jobs() { return num_job;}

//return next event time
template <class DType>
double dist<DType>::NextTime() {return nexttime;}

//start a job, and find a set of masters to send tasks
template <class DType>
void dist<DType>::Initial_Job(job jb, List<int> &lst)
{ 
  int i,j,k,n,nn;
  bool Isin[NM];
  job jj;
  jobworker jk;
      
  for(i=0;i<NM;i++) {Isin[i]=false; } 
  
  n=jb.fan; k=0; 
  nn=n/NM;
  while(nn>0) { //if the number of tasks is greater than NM, equally distribute the tasks to each Master
	  for(i=0;i<NM;i++) { lst.EnQueue(i); } //send one task to each master
	  nn--;     //send total nn round(s)
  }
  
  n=n%NM; //the remaining tasks are randomly sent to n masters
  while(n!=0) {  //randomly select n masters
     j=(int)(get_random()*NM); 
     if(Isin[j]==false) { Isin[j]=true; lst.EnQueue(j); k++; }
     if(k==n) { break;}
  }
  
  jj=jb; jk.numt=jb.fan; jk.nume=0; jk.numc=0;
  jk.jb=jj; jk.plst=lst; 
  LJ.EnQueue(jk);    //put the job to the job list

  return ;
}

//add new event to timing list, sorted by event time
template <class DType>
void dist<DType>::AddTimeList(job jb)
{
  double tm, tim;
  int tp;
  tm=jb.nowtime;

  if(TM.Empty()) { TM.EnQueue(jb); nexttime=tm; return; }

  TM.SetToHead(); tim=TM.Access()->nowtime;
  if(tm<tim) { TM.EnQueue(jb); nexttime=tm; return;}
  while(TM.Fore()) {
           if(tm<TM.Access()->nowtime) {TM.Back(); TM.Insert(jb); nexttime=tim; return;  } 
  }
        TM.Insert(jb); nexttime=tim; return;               
}

/***********************************************************************************************************
run distributor events:                   
0: receive a completed task from a worker                 
return: 22 indicating a job is completed        
************************************************************************************************************/
template <class DType>
int dist<DType>::Run_Dist(jobworker &jw)
{  int i,k,ii,kk,index;
   job jb1,jb2;
   jobworker jk;
   int et, tp;
   List<int> lt1, lt2;

    et=0;  //default return value

   if(TM.Empty()) { nexttime=INF; return et;}

   TM.SetToHead();
   jb1=*TM.Access();
   TM.Remove();
   tp=jb1.type; kk=jb1.wrk;
  
  if(tp==0) {//receive a complete task from a work 
      if(!LJ.Empty()) { LJ.SetToHead(); jw=*LJ.Access(); jb2=jw.jb; 
         if(jb1.ID==jb2.ID) { jw.numc++; 
	        if(jw.numc==jw.numt) { et=22; num_job--; LJ.Remove(); } //complete a job, remove it from the job list
		else {LJ.Remove(); LJ.EnQueue(jw); }
         }
	 else {
	       while(LJ.Fore()) { jw=*LJ.Access(); jb2=jw.jb; 
                   if(jb1.ID==jb2.ID) { jw.numc++; 
		          if(jw.numc==jw.numt) { et=22; num_job--; LJ.Remove(); }  //complete a job, remove it from the job list
			  else { LJ.Remove(); LJ.EnQueue(jw); }
			   break;
                     }		 
		 }//end while
	 }//end else
     }//end if(!LJ.Empty())
   }//end else if(tp==2)
    
   if(TM.Empty()) { nexttime=INF; }
   else {TM.SetToHead(); jb1=*TM.Access(); nexttime=jb1.nowtime;}

   return et;
}

template <class DType>
void dist<DType>::Show_Data() {
  int i=0,k;
  job jb;
  jobworker jw;

  cout<<endl<<"$$$$$$ List all jobs in the distributor  "<<id<<" nexttime: "<<nexttime<<endl;
  cout<<" List all event: "<<endl;
  if(TM.Empty()) { cout<<" no event!"<<endl; }
  else { TM.SetToHead(); jb=*TM.Access(); print_job(jb); 
         while(TM.Fore()) { jb=*TM.Access(); print_job(jb); }
  }
  
  cout<<"List all jobs:"<<endl;
  if(!LJ.Empty()) { LJ.SetToHead(); jw=*LJ.Access(); jb=jw.jb; print_job(jb); 
	   cout<<" numt: "<<jw.numt<<" numbe: "<<jw.nume<<" numc: "<<jw.numc<<endl;
	   if(!jw.plst.Empty()) { jw.plst.SetToHead(); k=*jw.plst.Access(); cout<<" master: "<<k<<"  "; i++;
	      while(jw.plst.Fore()) { k=*jw.plst.Access(); cout<<k<<"  "; i++; }
	   }
	   cout<<endl<<"--------------------------"<<endl;
         while(LJ.Fore()) { jw=*LJ.Access(); jb=jw.jb; print_job(jb); 
          cout<<" numt: "<<jw.numt<<" numbe: "<<jw.nume<<" numc: "<<jw.numc<<endl;
	      if(!jw.plst.Empty()) { jw.plst.SetToHead(); k=*jw.plst.Access(); cout<<" master: "<<k<<"  "; i++;
	          while(jw.plst.Fore()) { k=*jw.plst.Access(); cout<<k<<"  "; i++; }
	       }
            cout<<endl<<"------------------------"<<endl;
	   }
  }


  cout<<endl<<"&&&&&&&&&&&&&&&&&&&&End of show data in Distributor*********************"<<endl;

   return;
}

