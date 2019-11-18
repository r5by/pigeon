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

package edu.utarlington.pigeon.daemon.master;

import edu.utarlington.pigeon.daemon.master.TaskScheduler;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.ThriftClientPool;
import edu.utarlington.pigeon.thrift.RecursiveService;
import edu.utarlington.pigeon.thrift.THostPort;
import edu.utarlington.pigeon.thrift.TLaunchTasksRequest;
import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;
//import javafx.concurrent.Task;
import org.apache.log4j.Logger;
import org.apache.thrift.async.AsyncMethodCallback;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and uses a FIFO queue to determine the order to launch
 * tasks whenever outstanding tasks exceed this amount.
 */
public class FifoTaskScheduler extends TaskScheduler {
    private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);

    public int maxActiveTasks;

    //    /** Thrift client pool for communicating with Pigeon scheduler */
    ThriftClientPool<RecursiveService.AsyncClient> recursiveClientPool =
            new ThriftClientPool<RecursiveService.AsyncClient>(
                    new ThriftClientPool.RecursiveServiceMakerFactory());

    /** Available workers passed from master, should be invoked only at startup of master and this scheduler */
    public FifoTaskScheduler() {
        maxActiveTasks = -1;
    }

    @Override
    void handleSubmitTaskLaunchRequest(TaskSpec taskToBeLaucnhed) {
            makeTaskRunnable(taskToBeLaucnhed);
    }

    @Override
    protected boolean handleTaskFinished(String appId, String requestId, String taskId, THostPort schedulerAddress, InetSocketAddress backendAddress, PriorityType workerType) {
        LOG.debug("Worker: " + backendAddress + " now have an idle spot open, attempting to fetch a new task from master node: " + this.ipAddress);
        TaskSpec reservation = new TaskSpec(appId, requestId, taskId, schedulerAddress, backendAddress);

        TLaunchTasksRequest request = null;
        switch (workerType) {
            case HIGH:
                if(!HTQ.isEmpty())
                    request = HTQ.poll();
                break;
            case LOW:
                if (!HTQ.isEmpty()) {
                    request = HTQ.poll();
                } else if(!LTQ.isEmpty()) {
                    request = LTQ.poll();
                }
                break;
                default:
                    LOG.error("Unexpected idle worker is received!");
        }

        return attemptTaskLaunch(request, reservation);
    }

    @Override
    protected void handleNoTasksReservations(String appId, String requestId, InetSocketAddress scheduler, THostPort master) {
        InetSocketAddress schedulerAddress = Network.constructSocket(scheduler, 20507);
        try {
            RecursiveService.AsyncClient recursiveClient = recursiveClientPool.borrowClient(schedulerAddress);
            LOG.debug("Notifying the scheduler all tasks for request " + requestId + " have completed.");
            recursiveClient.tasksFinished(requestId, master, new TasksFinishedCallBack(requestId, schedulerAddress) );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class TasksFinishedCallBack
            implements AsyncMethodCallback<RecursiveService.AsyncClient.tasksFinished_call> {
        InetSocketAddress schedulerAddr;
        String requestId;
        long startTimeMillis;

        public TasksFinishedCallBack(String requestId, InetSocketAddress schedulerAddr) {
            this.requestId = requestId;
            this.schedulerAddr = schedulerAddr;
            this.startTimeMillis = System.currentTimeMillis();
        }

        @Override
        public void onComplete(RecursiveService.AsyncClient.tasksFinished_call response) {
            try {
                long totalTime = System.currentTimeMillis() - startTimeMillis;
                LOG.debug( "Scheduler: " + schedulerAddr + " has been notified that all tasks from request: " + requestId);
                recursiveClientPool.returnClient(schedulerAddr, (RecursiveService.AsyncClient) response.getClient());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onError(Exception e) {
            LOG.debug(e.getMessage());
        }
    }

    /**
     * Attempts to launch a new task.
     *
     * The parameters {@code lastExecutedRequestId} and {@code lastExecutedTaskId} are used purely
     * for logging purposes, to determine how long the node monitor spends trying to find a new
     * task to execute. This method needs to be synchronized to prevent a race condition with todo:?
     */

    private boolean attemptTaskLaunch(TLaunchTasksRequest request, TaskSpec reservation) {
        boolean isIdle = false;

        //If no more tasks need to be send, return a dummy feedback to nm to inform it no more tasks for it
        if (request == null) {
            LOG.debug("No more tasks need to be assigned to this worker: " + reservation.appBackendAddress + ", prepare putting it to idle worker list.");
            isIdle = true;
        } else {
            //Launch the proper task on the worker
            reservation.requestId = request.requestID;
            reservation.taskSpec = request.tasksToBeLaunched.get(0);
            makeTaskRunnable(reservation);
        }

        return isIdle;
    }

    @Override
    protected int getMaxActiveTasks() {
        return maxActiveTasks;
    }

    @Override
    protected synchronized void enqueue(TLaunchTasksRequest task) {
        TTaskLaunchSpec taskSpec = task.tasksToBeLaunched.get(0);
        LOG.debug("Enqueue task_" + taskSpec.taskId + " of priority: " + taskSpec.isHT + " for request: " + task.requestID);
        if(taskSpec.isHT)
            HTQ.add(task);
        else
            LTQ.add(task);
    }
}
