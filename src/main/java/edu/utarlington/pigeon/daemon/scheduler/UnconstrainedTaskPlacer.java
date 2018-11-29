/*
 * PIGEON
 * Copyright 2018 Univeristy of Texas at Arlington
 *
 * Modified from Sparrow - University of California, Berkeley
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.utarlington.pigeon.daemon.scheduler;

import edu.utarlington.pigeon.daemon.util.ListUtils;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * A task placer for jobs whose tasks have no placement constraints.
 */
public class UnconstrainedTaskPlacer implements TaskPlacer {
    private static final Logger LOG = Logger.getLogger(UnconstrainedTaskPlacer.class);

    /** Specifications the scheduler for this task placer */
    private Scheduler scheduler;

    /**
     * Id of the request associated with this task placer.
     */
    String requestId;

    /** Used to uniquely identify number of completed tasks for this request */
    private int counter;
    private int totalTasks;

    private List<TTaskLaunchSpec> tasks;

    /** Used to keep track of number of tasks assigned to a master node, indexed by the master node's address */
    private HashMap<InetSocketAddress, Integer> numberOfTasksAssignedToMasters;

    UnconstrainedTaskPlacer(String requestId, int totalTasks, Scheduler scheduler) {
        this.requestId = requestId;
        this.scheduler = scheduler;
        this.totalTasks = totalTasks;
        counter = 0;
        tasks = new ArrayList<TTaskLaunchSpec>();
        numberOfTasksAssignedToMasters = new HashMap<InetSocketAddress, Integer>();
    }
    private void populateTasksForRequest(TSchedulingRequest schedulingRequest) throws TException {
        if (schedulingRequest.getTasks() == null || schedulingRequest.getTasks().isEmpty()) {
            throw new IncompleteRequestException("Invalid request, please verify the app.");
        }

        for (TTaskSpec task : schedulingRequest.getTasks()) {
            TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(), task.bufferForMessage(), task.isHT);
            tasks.add(taskLaunchSpec);
        }
    }

    //todo
    @Override
    public boolean allTasksPlaced() {
        return (counter == totalTasks);
    }

    /**
     * Assigning tasks of request to each group (master) based on Pigeon Scheduler policy
     * @param schedulingRequest
     * @param schedulerAddr
     * @throws Exception
     */
    @Override
    public void processRequest(TSchedulingRequest schedulingRequest, THostPort schedulerAddr) throws Exception {
        populateTasksForRequest(schedulingRequest);

        //Step1: set-up task priority level based on request info: average task execution time
        double avgTasksD = schedulingRequest.getAvgTasksExecDuration();
        if(avgTasksD == 0.0) {
            //calc average tasks duration by tasks list
            avgTasksD = ListUtils.mean(tasks);
            System.out.println("_____Average_____" + avgTasksD);
        }

        double cutOff = scheduler.getCutoff();
        boolean highPriority;
        if (avgTasksD < cutOff){
            highPriority = true;
        }
        else {
            highPriority = false;
        }

        //Set up task priorities based on the avg. task exec duration
        for (TTaskLaunchSpec spec: tasks) {
            spec.setIsHT(highPriority);
        }

        //Step2: split tasks to master groups
        int numOfGroups = scheduler.pigeonMasters.size();
        if (numOfGroups < 1) {
            throw new ServerNotReadyException("Master nodes need to be configured at Pigeon frondend side.");
        }

        List<List<TTaskLaunchSpec>> tasksSplitForGroups = ListUtils.split(tasks, numOfGroups);

        int i = 0;
        for (List<TTaskLaunchSpec> tasksTobeLaunched : tasksSplitForGroups) {
            TLaunchTasksRequest launchTasksRequest = new TLaunchTasksRequest(schedulingRequest.getApp(), schedulingRequest.getUser(), requestId, schedulerAddr,
                    tasksTobeLaunched);
            InetSocketAddress masterAddr = scheduler.getMaster(i++);
            assignLaunchTaskRequest(launchTasksRequest, masterAddr);

            int numberOfAssignedTasks = tasksTobeLaunched.size();
            numberOfTasksAssignedToMasters.put(masterAddr, numberOfAssignedTasks);
            LOG.debug(numberOfAssignedTasks + " of tasks from request: " + requestId + " has been assigned to master node: " + masterAddr);
        }
    }

    @Override
    public int completedTasks() {
        return counter;
    }

    @Override
    public int totalTasks() {
        return totalTasks;
    }

    @Override
    public int count(THostPort addr) {
        InetSocketAddress masterAddr = Network.thriftToSocketAddress(addr);
        int finishedTasks = numberOfTasksAssignedToMasters.get(masterAddr);
        counter += finishedTasks;
        return counter;
    }

    private void assignLaunchTaskRequest(TLaunchTasksRequest request, InetSocketAddress masterAddress) throws Exception {
        InternalService.AsyncClient client = scheduler.masterClientPool.borrowClient(masterAddress);
        client.launchTasksRequest(request, new LaunchTasksRequestCallBack(requestId, masterAddress));
    }

    /**
     * Callback for launchTaskRequest() that returns the client to the client pool.
     */
    private class LaunchTasksRequestCallBack
            implements AsyncMethodCallback<InternalService.AsyncClient.launchTasksRequest_call> {
        String requestId;
        InetSocketAddress masterAddress;
        long startTimeMillis;

        public LaunchTasksRequestCallBack(String requestId, InetSocketAddress masterAddress) {
            this.requestId = requestId;
            this.masterAddress = masterAddress;
            this.startTimeMillis = System.currentTimeMillis();
        }

        //Pigeon: take out the backend from HIW/LIW if reservation success
        @Override
        public void onComplete(InternalService.AsyncClient.launchTasksRequest_call response) {
            long totalTime = System.currentTimeMillis() - startTimeMillis;
            LOG.debug("Launch task request has been sent to " + masterAddress.getAddress().getHostAddress() +
                    " for request " + requestId + "; completed in " + totalTime + "ms");
            try {
                scheduler.masterClientPool.returnClient(masterAddress, (InternalService.AsyncClient) response.getClient());
            } catch (Exception e) {
                LOG.error("Error returning client to node monitor client pool: " + e);
            }

            return;
        }

        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing launchTasksRequest RPC:" + exception);
        }
    }
}

