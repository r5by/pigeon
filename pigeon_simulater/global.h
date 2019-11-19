
//this is a setup head file to initial simulation setups and define some global functions 
#include "WORK.h"

List<int> T_WK[NL], T_MS, T_DS;  //timing list for workers, masters  and distributors
dist<int> DS[ND];                //define ND number of distributors
worker<int> WK[NW];              //define NW number of workers
master<int> MS[NM];              //define NM number of masters

//Add a new event time in a worker timer list (NL worker list, a worker is set in a list based on its ID)
//the event is sorted by their next event time in increasing order from head to tail
void AddWorkerTime(int n)
{ int i, kk;

  kk=n/UN; //find the corresponding worker list
  if(T_WK[kk].Empty()) {T_WK[kk].EnQueue(n); return; }
  
   T_WK[kk].SetToTail();
   i=*T_WK[kk].Access();
   if(WK[n].NextTime()>=WK[i].NextTime()) { T_WK[kk].Insert(n); return;}

    while(T_WK[kk].Back()) { i=*T_WK[kk].Access();
       if(WK[n].NextTime()>=WK[i].NextTime()) { T_WK[kk].Insert(n); return; }
      }

    T_WK[kk].EnQueue(n);
}

//Add a new event time in the Master list 
//the event is sorted by their next event time in increasing order from head to tail
void AddMasterTime(int n)
{
  if(T_MS.Empty()){ T_MS.EnQueue(n); return; }
  
  int i; 
  T_MS.SetToTail();
  while(1) {
      i=*T_MS.Access();
      if(MS[n].NextTime()>=MS[i].NextTime()) {T_MS.Insert(n); return; }
      if(!T_MS.Back()) { break;}
    }
  
   T_MS.EnQueue(n);
}

//Add a new event time in the Distributor list 
//the event is sorted by their next event time in increasing order from head to tail
void AddDistTime(int n)
{
  if(T_DS.Empty()){ T_DS.EnQueue(n); return; }
  
  int i; 
  T_DS.SetToTail();
  while(1) {
      i=*T_DS.Access();
      if(DS[n].NextTime()>=DS[i].NextTime()) {T_DS.Insert(n); return; }
      if(!T_DS.Back()) { break;}
    }
  
   T_DS.EnQueue(n);
}

//Initially set up workers, masters and distributors
void Setup()
{  int i,j,k;
   double xx,tm;
   List<int> lst, hlst;

  jnumber=0; 
   
  for (i=0;i<ND; i++) {  //set up  parameters for ND distributors
       DS[i].Set_ID(i);            
   }
   
   for(i=0;i<NM; i++) { //set paramters for NM masters
       MS[i].Set_ID(i); lst.kill();
       for(j=0; j<MNW; j++) 
	   {  k=j+i*MNW; 
          if(j<DNUM) { hlst.EnQueue(k); }
		  else { lst.EnQueue(k); }
       }
       MS[i].SetWorker(lst,hlst); lst.kill();
    }

  for (i=0; i<NW; i++) { //set up parameters for NW workers
      WK[i].Set_ID(i); 
      j=i/MNW;  WK[i].Set_MasterID(j); 
   }
}
 
//Print timing list information
void Show_Time_List()
{ int i,n;

  i=0;
  cout<<"Show time list of distributors: "<<endl;
  if(T_DS.Empty()) {cout<<"No event for distributor."<<endl; }
  else {
     T_DS.SetToHead(); n=*T_DS.Access(); 
     cout<<" DIST: "<<n<<" even at: "<<DS[n].NextTime()<<endl;
     while(T_DS.Fore()) { n=*T_DS.Access(); cout<<" DIST: "<<n<<" event at:  "<<DS[n].NextTime()<<endl; } 
  }
 
 cout<<"Show time list of masters: "<<endl;
  if(T_MS.Empty()) {cout<<"No event for master."<<endl; }
  else {
     T_MS.SetToHead(); n=*T_MS.Access(); 
     cout<<" Master: "<<n<<" even at: "<<MS[n].NextTime()<<endl;
     while(T_MS.Fore()) { n=*T_MS.Access(); i++; if(i>5 && n!=0) { continue;}
            cout<<" Master: "<<n<<" event at:  "<<MS[n].NextTime()<<endl; } 
  }
 
  cout<<"&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"<<endl;
  cout<<"show time list of workers:"<<endl;  
  
  for(i=0; i<NL;i++) {
  if(T_WK[i].Empty()) { cout<<" No event for workers."<<endl;}
  else {
     T_WK[i].SetToHead(); n=*T_WK[i].Access();
     cout<<" WORK: "<<n<<" event at "<<WK[n].NextTime()<<endl;
     while(T_WK[i].Fore()) { n=*T_WK[i].Access(); 
            cout<<" WORK: "<<n<<" event at "<<WK[n].NextTime()<<endl; }
   } 
  }
}

