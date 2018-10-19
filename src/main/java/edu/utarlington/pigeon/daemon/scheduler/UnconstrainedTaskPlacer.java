package edu.utarlington.pigeon.daemon.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.async.AsyncMethodCallback;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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

    UnconstrainedTaskPlacer(String requestId, int totalTasks, Scheduler scheduler) {
        this.requestId = requestId;
        this.scheduler = scheduler;
        this.totalTasks = totalTasks;
        counter = 0;
    }

    //todo
    @Override
    public boolean allTasksPlaced() {
        return (counter == totalTasks);
    }

    @Override
    public void enqueueRequest(TSchedulingRequest schedulingRequest, THostPort schedulerAddr) throws Exception {
        //Pigeon decides whether to send the task launch request or not based on the available resources
        int enqueuedHTCnt = 0;
        int enqueuedLTCnt = 0;

        //TODO: Send multiple tasks to node monitors based on the available resources
        for (TTaskSpec task : schedulingRequest.getTasks()) {
            TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(), task.bufferForMessage());
            List<TTaskLaunchSpec> tasksToBeLaunched = Lists.newArrayList(taskLaunchSpec);
            TLaunchTaskRequest launchTaskRequest = new TLaunchTaskRequest(schedulingRequest.getApp(), schedulingRequest.getUser(), requestId, schedulerAddr,
                    tasksToBeLaunched);

            if(task.isIsHT()) {//For high priority tasks
                if (!scheduler.isLIWEmpty()) {
                    //If low priority idle queue is not empty, pick up one and send the request to the nm
                    assignLaunchTaskRequest(launchTaskRequest, scheduler.getWorker(false));
                } else if (!scheduler.isHIWEmpty()) {
                    //O.w. if  high priority idle queue is not empty, pick up one and send the request to that nm
                    assignLaunchTaskRequest(launchTaskRequest, scheduler.getWorker(true));
                } else {//else enqueue the task in HTQ
                    scheduler.enqueueLaunchTaskRequest(launchTaskRequest, true);
                    enqueuedHTCnt++;
                }
            } else {//For low priority tasks
                if (!scheduler.isLIWEmpty()) {
                    //If low priority idle queue is not empty, pick up one and send the request to the nm
                    assignLaunchTaskRequest(launchTaskRequest, scheduler.getWorker(false));
                } else {
                    scheduler.enqueueLaunchTaskRequest(launchTaskRequest, false);
                    enqueuedLTCnt++;
                }
            }
        }
        LOG.debug("Request: " + requestId + " has " + enqueuedHTCnt + " of tasks queued in HTQ, and " + enqueuedLTCnt + " tasks queued in LTQ");
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
    public int count() {
        return ++counter;
    }

    private void assignLaunchTaskRequest(TLaunchTaskRequest request, InetSocketAddress nodeMonitorAddress) throws Exception {
        InternalService.AsyncClient client = scheduler.nodeMonitorClientPool.borrowClient(nodeMonitorAddress);
        client.launchTaskRequest(request, new LaunchTaskRequestCallBack(requestId, nodeMonitorAddress));
    }

    /**
     * Callback for launchTaskRequest() that returns the client to the client pool.
     */
    private class LaunchTaskRequestCallBack
            implements AsyncMethodCallback<InternalService.AsyncClient.launchTaskRequest_call> {
        String requestId;
        InetSocketAddress nodeMonitorAddress;
        long startTimeMillis;

        public LaunchTaskRequestCallBack(String requestId, InetSocketAddress nodeMonitorAddress) {
            this.requestId = requestId;
            this.nodeMonitorAddress = nodeMonitorAddress;
            this.startTimeMillis = System.currentTimeMillis();
        }

        //Pigeon: take out the backend from HIW/LIW if reservation success
        //TODO: ADD node monitor back to HIW/LIW when task is actually completed (RPC from nodemonitor)
        @Override
        public void onComplete(InternalService.AsyncClient.launchTaskRequest_call response) {
            long totalTime = System.currentTimeMillis() - startTimeMillis;
            LOG.debug("Launch task request has been sent to " + nodeMonitorAddress.getAddress().getHostAddress() +
                    " for request " + requestId + "; completed in " + totalTime + "ms");
            try {
                scheduler.nodeMonitorClientPool.returnClient(nodeMonitorAddress, (InternalService.AsyncClient) response.getClient());
            } catch (Exception e) {
                LOG.error("Error returning client to node monitor client pool: " + e);
            }

            return;
        }

        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
        }
    }
}

