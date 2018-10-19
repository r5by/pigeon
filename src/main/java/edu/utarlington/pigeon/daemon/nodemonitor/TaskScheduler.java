package edu.utarlington.pigeon.daemon.nodemonitor;

import com.google.common.collect.Lists;
import edu.utarlington.pigeon.daemon.util.Network;
//import edu.utarlington.pigeon.thrift.TEnqueueTaskReservationsRequest;
import edu.utarlington.pigeon.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.utarlington.pigeon.daemon.scheduler.Scheduler;

/**
 * A TaskScheduler is a buffer that holds task reservations until an application backend is
 * available to run the task. When a backend is ready, the TaskScheduler requests the task
 * from the {@link Scheduler} that submitted the reservation.
 *
 * Each scheduler will implement a different policy determining when to launch tasks.
 *
 * Schedulers are required to be thread safe, as they will be accessed concurrently from
 * multiple threads.
 */
//TODO: Logging
public abstract class TaskScheduler {

    protected class TaskSpec {
        public String appId;
        public TUserGroupInfo user;
        public String requestId;

        public InetSocketAddress schedulerAddress;
        public InetSocketAddress appBackendAddress;

        /**
         * ID of the task that previously ran in the slot this task is using. Used
         * to track how long it takes to fill an empty slot on a slave. Empty if this task was launched
         * immediately, because there were empty slots available on the slave.  Filled in when
         * the task is launched.
         */
        public String previousRequestId;
        public String previousTaskId;

        /** Filled in after the getTask() RPC completes. */
        /** For pigeon, taskSpec is filled in when nm get the launch task request*/
        public TTaskLaunchSpec taskSpec;

        /** Used to construct a dummy reservation to stimulate TaskLaunchService fetching a new task from Pigeon scheduler */
        public TaskSpec(String previousRequestId, String previousTaskId, THostPort schedulerAddress, InetSocketAddress appBackendAddress) {
            requestId = previousRequestId;
            this.previousRequestId = previousRequestId;
            this.previousTaskId = previousTaskId;
            this.schedulerAddress = Network.thriftToSocketAddress(schedulerAddress);
            this.appBackendAddress = appBackendAddress;
        }

        public TaskSpec(TLaunchTaskRequest request, InetSocketAddress appBackendAddress) {
            appId = request.getAppID();
            user = request.getUser();
            requestId = request.getRequestID();
            taskSpec = unWrapLaunchTaskRequest(request);
            schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(),
                    request.getSchedulerAddress().getPort());
            this.appBackendAddress = appBackendAddress;
            previousRequestId = "";
            previousTaskId = "";
        }
    }

    private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
//    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
    private String ipAddress;

    protected Configuration conf;
    private final BlockingQueue<TaskSpec> runnableTaskQueue =
            new LinkedBlockingQueue<TaskSpec>();

    /** Initialize the task scheduler, passing it the current available resources
     *  on the machine. */
    void initialize(Configuration conf, int nodeMonitorPort) {
        this.conf = conf;
        this.ipAddress = Network.getIPAddress(conf);
    }

    /**
     * Get the next task available for launching. This will block until a task is available.
     */
    TaskSpec getNextTask() {
        TaskSpec task = null;
        try {
            task = runnableTaskQueue.take();
        } catch (InterruptedException e) {
            LOG.fatal(e);
        }
        return task;
    }

    /**
     * Returns the current number of runnable tasks (for testing).
     */
    int runnableTasks() {
        return runnableTaskQueue.size();
    }

    void tasksFinished(List<TFullTaskId> finishedTasks, InetSocketAddress backendAddress) {
        for (TFullTaskId t : finishedTasks) {
//            AUDIT_LOG.info(Logging.auditEventString("task_completed", t.getRequestId(), t.getTaskId()));
            LOG.debug("Task: " + t.taskId + " for request: " + t.requestId + " has finished by worker:" + backendAddress);
            handleTaskFinished(t.getRequestId(), t.getTaskId(), t.getSchedulerAddress(), backendAddress);
        }
    }

    void noTaskForReservation(TaskSpec taskReservation) {
//        AUDIT_LOG.info(Logging.auditEventString("node_monitor_get_task_no_task",
//                taskReservation.requestId,
//                taskReservation.previousRequestId,
//                taskReservation.previousTaskId));
        handleNoTaskForReservation(taskReservation);
    }

    protected void makeTaskRunnable(TaskSpec task) {
        try {
            LOG.debug("Putting reservation for request " + task.requestId + " in runnable queue");
            runnableTaskQueue.put(task);
        } catch (InterruptedException e) {
            LOG.fatal("Unable to add task to runnable queue: " + e.getMessage());
        }
    }

//    public synchronized void submitTaskReservations(TLaunchTaskRequest request,
//                                                    InetSocketAddress appBackendAddress) {
//        TaskSpec reservation = new TaskSpec(request, appBackendAddress);
//        int queuedReservations = handleSubmitTaskReservation(reservation);
//        LOG.debug("reservation enqueued at " + ipAddress + " for " + request.requestID + "Queued reservations: " + queuedReservations);
//
//    }

    public synchronized int submitLaunchTaskRequest(TLaunchTaskRequest request,
                                                        InetSocketAddress appBackendAddress) {
        TaskSpec taskToBeLaunched = new TaskSpec(request, appBackendAddress);
        LOG.debug("Launching taskID: " + taskToBeLaunched.taskSpec.taskId + " for request: " + request.requestID);
        return handleSubmitTaskLaunchRequest(taskToBeLaunched);
    }

    private TTaskLaunchSpec unWrapLaunchTaskRequest(TLaunchTaskRequest request) {
        if(request.tasksToBeLaunched != null && request.tasksToBeLaunched.size() == 1)
            return request.tasksToBeLaunched.get(0);
        else {//TODO: Handling more than one tasks
            LOG.debug("Fetching more than one tasks for the request.");
            return null;
        }
    }

    // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING.

    /**
     * Handles a task reservation. Returns the number of queued reservations.
     */
//    abstract int handleSubmitTaskReservation(TaskSpec taskReservation);

    /**
     * Handles the launch task request, returns the number of tasks to be launched (or ... reservations?)
     * @param taskToBeLaucnhed
     * @return
     */
    abstract int handleSubmitTaskLaunchRequest(TaskSpec taskToBeLaucnhed);

    /**
     * Cancels all task reservations with the given request id. Returns the number of task
     * reservations cancelled.
     */
//    abstract int cancelTaskReservations(String requestId);

    /**
     * Handles the completion of a task that has finished executing.
     */
    protected abstract void handleTaskFinished(String requestId, String taskId, THostPort schedulerAddress, InetSocketAddress backendAddress);

    /**
     * Handles the case when the node monitor tried to launch a task for a reservation, but
     * the corresponding scheduler didn't return a task (typically because all of the corresponding
     * job's tasks have been launched).
     */
    protected abstract void handleNoTaskForReservation(TaskSpec taskSpec);

    /**
     * Returns the maximum number of active tasks allowed (the number of slots).
     *
     * -1 signals that the scheduler does not enforce a maximum number of active tasks.
     */
    abstract int getMaxActiveTasks();
}
