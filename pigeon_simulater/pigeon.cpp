
/*#########################################################################################
The simulator of pigeon------The University of Texas at Arlongton
Group members: Zhijun Wang, Huiyang Li, Zhongwei Li, Xiaocui Sun, Jia Rao, Hao Che and Hong Jiang
The man program of pigeon: pigeon.cpp
###########################################################################################*/

#include <iomanip>
#include <math.h>
#include <fstream>
#include <iostream>
#include "global.h"

using namespace std;

int main(int argc, char** argv)
{
        int i,ii,j,k,kk, n,nn, dis, code,key,ll,lll,run;
        double  xx,tt,ttt,tm,runtime,now_time;
        int  finish_time[5000], wait_time[5000];  //reord job completion  time in 5000 interval
        int  n99,n999, w99, w999;
        double total_act, total_cnt, cnt_act, cnt_cnt, total_time, t_time,w_time;
        double temp_sum, temp_z, qload, qload_t,RTM, NJT;
	long total_job, total_fin,job_num,zerow,jj;
        int ms_idle, ms_cnt[NM];
        double cntms, idms;
	job jb, jbb;
	jobworker jw;
	List<int> lt, lst;
        List<double> ltm;
        ofstream  myfile;
	ifstream  infile;

        //open output file, store the results in format: Job ID; job type (short or long);
        // fanout (number of tasks); job completion time;  job waiting time (queue plus communication time);
        myfile.open("output_file_name");
       //open the input file(tace file), specify the path and file name
	//input file format: arrival time; fanout degree; average task execution time; individual task execution time
	infile.open("path/input_file_name"); //input trace file path and name;

        //Initial parameters to 0
        n99=n999=w99=w999=0; total_act=total_cnt=0; total_job=total_fin=job_num=zerow=0;
        cnt_act=cnt_cnt=total_time=t_time=w_time=0;
        cntms=idms=0;

        //Set current time as random seed
        srand(time(NULL));

       //set up system parameters
        Setup();

        //Initiate performance metrics
        for(i=0;i<5000;i++) {finish_time[i]=wait_time[i]=0; }

        //read the first job arrival time from the input file
        infile>>runtime;

       //run=1, start a new job
        run=1;

        //RTM is the time to peridoical calculate the load percentage of workers, +10 means 1st calculate load at runtime+10 second
        RTM=runtime+10;

        //NJT: next job time
        NJT=0;  key=0;

         //Initial values of jj, ll and lll
         jj=0; ll=lll=0;

     //main process for pigeon
     while(runtime<endtime) {
         switch (run) {
            case (1):   //Start a new job
               ltm.kill(); //clean list

	 //read job information from file
	 // job type is set to 0, msid is set to 0
	 infile>>jb.fan; jb.ID=jnumber; jnumber++;
	 jb.nowtime=jb.stime=runtime; jb.deadline=0; jb.msid=0; jb.type=0;

         //xx the average task execution time for a job, job type is set based on xx and job type cutoff time
	 infile>>xx;
	 if(xx<=short_cutoff) { jb.jb_type=1; }   //set job to be short (1) or long (0)
	 else { jb.jb_type=0; }

       //random select a job distributor (i.e., job scheduler) to handle the job
	 jb.dis=(int)(ND*get_random());
         //Initial set 0 as task execion time, number of task, longest task exection time
         jb.etim=0; i=0; jb.ltime=0;

        //check if there are  more jobs in the trace or not, if no, set next job arrival time to infinite, break from reading a new job
         if(infile.eof()) { NJT=INF; break;}

        //read the task execution times of the job and store in the list ltm, record the longest task execution time
         while(!infile.eof()) {
            infile>>xx; ltm.EnQueue(xx);
	    if(xx>jb.ltime) { jb.ltime=xx;}
            i++;  if(i>=jb.fan) { break;}
	 }

                //The selected job distributor initiates the job
		dis=jb.dis;  total_job++; jb.type=0;
		lt.kill(); DS[dis].Initial_Job(jb, lt);  //lt: a list of selected masters to execute taks of the job

		if(lt.Empty()) { cout<<"Wrong: fanout is 0!"<<endl; exit(0);}
		else { //send a task to selected  master  from lt list
	              jb.nowtime=runtime+net_delay;
                      xx=*ltm.Access(); ltm.Remove(); jb.etim=xx;
		      lt.SetToHead(); k=*lt.Access(); jb.msid=k;
		      tt=MS[k].NextTime();  MS[k].AddTimeList(jb);  //send task to master k (group k)

		      if(tt>INF-epsilon) { AddMasterTime(k); }      //no previous event in the master, add the event (to receive task from distributor to timeng list)
		      else if(tt>MS[k].NextTime()) {                //some event in timnge list and add the new events in the list, sort the list in ascending order
			   if(T_MS.Empty()) { AddMasterTime(k); }
			   else { T_MS.SetToHead(); j=*T_MS.Access(); ii=0;
			          if(j==k) { T_MS.Remove(); AddMasterTime(k); ii++; }
				  else { while(T_MS.Fore()) { j=*T_MS.Access();
				                if(j==k) { T_MS.Remove(); AddMasterTime(k); ii++; break; }
				          }
				    }
				   if(ii==0) { AddMasterTime(k); }
				}//end 3rd else
			}

	    while(lt.Fore()) { k=*lt.Access(); //repeat the previous task delivery to a master for each task
              xx=*ltm.Access(); jb.etim=xx; ltm.Remove();
	      tt=MS[k].NextTime(); jb.msid=k;  MS[k].AddTimeList(jb);
	      if(tt>INF-epsilon) { AddMasterTime(k); }
	      else if(tt>MS[k].NextTime()) {
	           if(T_MS.Empty()) { AddMasterTime(k); }
	           else { T_MS.SetToHead(); j=*T_MS.Access(); ii=0;
	                  if(j==k) { T_MS.Remove(); AddMasterTime(k); ii++; }
		          else { while(T_MS.Fore()) { j=*T_MS.Access();
                                if(j==k) { T_MS.Remove(); AddMasterTime(k); ii++; break; }
		                 }
			  }
			   if(ii==0) { AddMasterTime(k); }
			}//end  last else
	           }//end else
		   }
	}//end 1st else

        if(infile.eof()) { NJT=INF; }  //End of file, set next time to be infinit
        else{ infile>>NJT;}            //read the nest job arrive time from the file

        break;

        case (2): //Distributor Event
	   // return code: 22 complete a job, and 0 otherwise
             code=DS[key].Run_Dist(jw);

	    if(code==22) {//complete a job, count the complete time
		  jb=jw.jb; total_fin++; tt=runtime-jb.stime; total_time+=tt;
		  job_num++; t_time+=tt; ttt=tt-jb.ltime; w_time+=ttt;
                  if(ttt<=2*net_delay+loc_delay+epsilon) {zerow++; }
		  n=(int)(tt/100);  if(n>4999) {n=4999;} finish_time[n]++;
                  n=(int)(ttt/10); if(n>4999) {n=4999; } wait_time[n]++;
                  myfile<<jb.ID<<"  "<<jb.jb_type<<"  "<<jb.fan<<"   "<<tt<<"  "<<ttt<<endl;
	    }//end if(code==22)

             tm=DS[key].NextTime();
             if(tm<INF-epsilon) { AddDistTime(key); } //add next event for Distributor
             break;

         case (3): //Master Event
	  // return code: 20 send a task to a worker, and 0 otherwise
	   code=MS[key].Run_Master(jb);

	  if(code==20) {//send a task to a worker
	         k=jb.wrk;  jb.nowtime=runtime+loc_delay; jb.type=0;
		 tt=WK[k].NextTime(); WK[k].AddTimeList(jb);

                //update tor timing list
               if(tt==INF) {AddWorkerTime(k); }  //new event is the only event
               else if (tt>WK[k].NextTime()) { nn=k/UN; //new event happens eariler than old event
                    if(T_WK[nn].Empty()) {AddWorkerTime(k); } //should not happen
                    else { T_WK[nn].SetToHead(); j=*T_WK[nn].Access(); ii=0;
                         if(j==k) { T_WK[nn].Remove(); AddWorkerTime(k); ii++;}
                         else { while(T_WK[nn].Fore()) { j=*T_WK[nn].Access();
                                       if(j==k) { T_WK[nn].Remove(); AddWorkerTime(k); ii++; break;}
                                 }
                         }
		      if(ii==0) { AddWorkerTime(k); }
                  }//end else
              }//end else if
	    }//end if(code==20)

             tm=MS[key].NextTime();
             if(tm<INF-epsilon) { AddMasterTime(key); } //add next event for the worker
             break;

         case (4): //worker event
	       //return code: 10send a completed task to a distributor and a idle worker notice to its master
	         code=WK[key].Run_Worker(jb, jbb);

              if(code==10 ) {  //send a task complete message to its distributor
                   k=jb.dis; jb.type=0;  jb.nowtime=runtime+net_delay;
                   tt=DS[k].NextTime();  //record the orginal nexttime
                   DS[k].AddTimeList(jb);

                  //update distributor timing list
                  if(tt>INF-epsilon) {AddDistTime(k); }  //new event is the only event
                  else if (tt>DS[k].NextTime()) {  //new event happens eariler than old even
                        if(T_DS.Empty()) {AddDistTime(k); } //should not happen
                        else { T_DS.SetToHead(); j=*T_DS.Access(); ii=0;
                               if(j==k) { T_DS.Remove(); AddDistTime(k); ii++;}
                               else { while(T_DS.Fore()) { j=*T_DS.Access();
                                      if(j==k) { T_DS.Remove(); AddDistTime(k); ii++; break;}
                                    }
                              }
		        if(ii==0) { AddDistTime(k); }
                       }//end else
                  }

                //send a idle notice to its master
	          kk=jbb.msid; jbb.nowtime=runtime+loc_delay;  jbb.type=1; jbb.wrk=key;
		  tt=MS[kk].NextTime();  //record the MS orginal nexttime
                  MS[kk].AddTimeList(jbb);

                  //update master timing list
                  if(tt>INF-epsilon) {AddMasterTime(kk); }  //new event is the only event
                  else if (tt>MS[kk].NextTime()) {  //new event happens eariler than old even
                        if(T_MS.Empty()) {AddMasterTime(kk); } //should not happen
                        else { T_MS.SetToHead(); j=*T_MS.Access(); ii=0;
                               if(j==kk) { T_MS.Remove(); AddMasterTime(kk); ii++;}
                                else { while(T_MS.Fore()) { j=*T_MS.Access();
                                       if(j==kk) { T_MS.Remove(); AddMasterTime(kk); ii++; break;}
                                       }
                                }
				if(ii==0) { AddMasterTime(kk); }
                         }//end else
                       }

	           }//end if(code==10 ||

             tm=WK[key].NextTime();
             if(tm<INF-epsilon) { AddWorkerTime(key); }//add next event for spine
              break;

         case (5): //calculate the load percentage, every 1000 s
            ms_idle=0; for(i=0;i<NM; i++) { ms_cnt[i]=0;}
	    for(i=0;i<NW;i++) {
		   total_cnt+=1.0; cnt_cnt+=1.0;
		   if(WK[i].IsActive()) { total_act+=1.0; cnt_act+=1.0; }
                   else { ii=i/MNW; ms_cnt[ii]++; }
	    }
            for(i=0;i<NM;i++) { if(ms_cnt[i]) { ms_idle++; } }
            cntms+=NM; idms+=ms_idle;
           // cout<<" Idle MS: "<<ms_idle<<" time: "<<runtime<<" idle ratio: "<<idms/cntms<<endl;
	      RTM+=1000;
		 break;

          }//end switch

           now_time=runtime; run=5; runtime=RTM;  //initial set up event for load calculation
          //find the nearnest event for next job arrival, distributor, master or worker
           if(runtime>NJT) { runtime=NJT; run=1; } //if true set to next job arrival time
           if(!T_DS.Empty()) { //if true, set to next distributr event
              T_DS.SetToHead(); key=*T_DS.Access();
              if(runtime>DS[key].NextTime()) { runtime=DS[key].NextTime(); run=2; }}

           if(!T_MS.Empty()) { //if true, set to next master event
               T_MS.SetToHead(); kk=*T_MS.Access();
               if(runtime>MS[kk].NextTime()) {runtime=MS[kk].NextTime(); run=3; key=kk;} }

          for(i=0;i<NL;i++){ //set to next worker event for a worker
           if(!T_WK[i].Empty()) { T_WK[i].SetToHead(); kk=*T_WK[i].Access();
               if(runtime>WK[kk].NextTime()) {runtime=WK[kk].NextTime(); run=4; key=kk; } }
        }

          //find the correpsonding ID for distributor, kaster or worker
          if(run==2) {T_DS.SetToHead(); T_DS.Remove(); }
          else if(run==3) { T_MS.SetToHead(); T_MS.Remove();}
          else if(run==4) { nn=key/UN; T_WK[nn].SetToHead(); T_WK[nn].Remove(); }

         if(now_time>runtime) {
	      cout<<" wrong: now time "<<now_time<<" runtime: "<<runtime<<" code: "<<run<<" Key: "<<key<<endl;
              break;
          }

          jj++; //print out statistic results periodically
	  if(jj%900000==0 && job_num!=0) {
             cout<<endl<<"runtime: "<<runtime<<"  "<<" Total jobs: "<<total_job<<endl;
             cout<<" Total completed jobs: "<<total_fin<<endl;
	     cout<<" Average job complete time: "<<total_time/total_fin<<endl;
             cout<<" waiting time: "<<w_time/total_fin<<endl;
             cout<<"Period job #: "<<job_num<<" Period average complete time: "<<t_time/job_num<<endl;
             cout<<" Zero wait ratio: "<<zerow*1.0/total_fin<<" zero wait #: "<<zerow<< endl<<endl;
	     cout<<"MS idle ratio: "<<idms/cntms<<endl;

	     n99=n999=0; temp_sum=temp_z=0; w99=w999=0;
             for(i=0;i<5000;i++) {
                if(finish_time[i]!=0) { temp_sum+=finish_time[i];
		  if(temp_sum*1.0/total_fin>=0.99 && n99==0) { n99=i; }
		  if(temp_sum*1.0/total_fin>=0.999 && n999==0) { n999=i; }
	     }
                if(wait_time[i]!=0) { temp_z+=wait_time[i];
                  if(temp_z*1.0/total_fin>=0.99 && w99==0) {w99=i; }
                  if(temp_z*1.0/total_fin>=0.999 && w999==0) { w999=i; }
            }
            //if(w999==0) { w999=5000;}
         }
	  cout<<" 99th percentile of job complete time: "<<n99*10<<" and 999th: "<<n999*10<<endl;
          cout<<" 99th percentile of job wait time: "<<w99<<" and 999th: "<<w999<<endl;
	  qload_t=total_act/total_cnt; qload=cnt_act/cnt_cnt;
          cout<<"queue load percentage: "<<qload_t<<" load in this period: "<<qload<<endl;
          cout<<"&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"<<endl;
              t_time=0; job_num=0; cnt_act=0; cnt_cnt=0;
         }
        }//end while

	  n99=n999=w99=w999=0; temp_sum=temp_z=0;
          for(i=0;i<5000;i++) {
             if(finish_time[i]!=0) { temp_sum+=finish_time[i];
		  if(temp_sum/total_fin>=0.99 && n99==0) { n99=i; }
		  if(temp_sum/total_fin>=0.999 && n999==0) { n999=i; }
	     }
            if(wait_time[i]!=0) { temp_z+=wait_time[i];
                 if(temp_z/total_fin>=0.99 && w99==00) { w99=i; }
                 if(temp_z/total_fin>=0.999 && w999==0) { w999=i; }
             }
         }

	//print out the final statitical results
	cout<<endl<<" The total number of finished jobs: "<<total_fin<<endl;
        cout<<" Average job complete time: "<<total_time/total_fin<<" zero wait ratio: "<<zerow*1.0/total_fin<<endl;
        cout<<" queue load percentage: "<<total_act/total_cnt<<" load in last period: "<<cnt_act/cnt_cnt<<endl;
	cout<<" 99th percentile of job complete time: "<<n99*10<<" and 999th: "<<n999*10<<endl;
        cout<<" 99th percentile of wait time: "<<w99<<" and 999th: "<<w999<<endl;

        return 0;

}

