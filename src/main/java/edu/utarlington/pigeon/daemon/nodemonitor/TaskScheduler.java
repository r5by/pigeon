package edu.utarlington.pigeon.daemon.nodemonitor;

import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.thrift.TEnqueueTaskReservationsRequest;
import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;
import edu.utarlington.pigeon.thrift.TUserGroupInfo;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
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
public abstract class TaskScheduler {
    private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
    //TODO: Add pigeon logging support
//    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
    private String ipAddress;

    protected Configuration conf;
    private final BlockingQueue<TaskSpec> runnableTaskQueue =
            new LinkedBlockingQueue<TaskSpec>();

    //=======================================
    // Constructors
    //=======================================
    /** Initialize the task scheduler, passing it the current available resources
     *  on the machine. */
    void initialize(Configuration conf, int nodeMonitorPort) {
        this.conf = conf;
        this.ipAddress = Network.getIPAddress(conf);
    }

    //=======================================
    // Methods
    //=======================================
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

    //=======================================
    // Abstract Methods
    //=======================================
    /**
     * Returns the maximum number of active tasks allowed (the number of slots).
     *
     * -1 signals that the scheduler does not enforce a maximum number of active tasks.
     */
    abstract int getMaxActiveTasks();

    /**
     * Handles the case when the node monitor tried to launch a task for a reservation, but
     * the corresponding scheduler didn't return a task (typically because all of the corresponding
     * job's tasks have been launched).
     */
    protected abstract void handleNoTaskForReservation(TaskSpec taskSpec);

    //=======================================
    // Inner Class
    //=======================================
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
        public TTaskLaunchSpec taskSpec;

        public TaskSpec(TEnqueueTaskReservationsRequest request, InetSocketAddress appBackendAddress) {
            appId = request.getAppId();
            user = request.getUser();
            requestId = request.getRequestId();
            schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(),
                    request.getSchedulerAddress().getPort());
            this.appBackendAddress = appBackendAddress;
            previousRequestId = "";
            previousTaskId = "";
        }
    }
}
