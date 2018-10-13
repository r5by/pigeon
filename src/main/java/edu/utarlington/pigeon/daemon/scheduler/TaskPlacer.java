package edu.utarlington.pigeon.daemon.scheduler;

import edu.utarlington.pigeon.thrift.TEnqueueTaskReservationsRequest;
import edu.utarlington.pigeon.thrift.THostPort;
import edu.utarlington.pigeon.thrift.TSchedulingRequest;
import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/***
 * A TaskPlacer is responsible for assigning the tasks in a job to backends. Assigning tasks to
 * backends occurs in two phases:
 *   (1) Task reservations are enqueued on backends. Typically more task reservations are enqueued
 *       than there are tasks that need to run (the ratio of task reservations to tasks is set
 *       using {@link PigeonConf.SAMPLE_RATIO} and {@link PigeonConf.SAMPLE_RATIO_CONSTRAINED}).
 *   (2) When backends are ready to run a task, they reply to the scheduler with a GetTask()
 *       RPC. The scheduler passes this call on to the task placer; if there are tasks remaining
 *       that can be run on that machine, the TaskPlacer responds with a specification for the
 *       task.
 * A TaskPlacer is responsible for determining where to enqueue task reservations, and how to
 * assign tasks to backends once a backend signals that it's ready to execute a task. TaskPlacers
 * are created per-job and persist state across these two phases.
 *
 * TaskPlacers are not thread safe; access to a particular TaskPlacer should be serialized.
 */
public interface TaskPlacer {
    /**
     * Returns a mapping of node monitor socket addresses to {@link TEnqueueTaskReservationRequest}s
     * that should be send to those node monitors. The caller is responsible for ensuring that
     * {@link schedulingRequest} is properly filled out.
     */
    public Map<InetSocketAddress, TEnqueueTaskReservationsRequest>
    getEnqueueTaskReservationsRequests(
            TSchedulingRequest schedulingRequest, String requestId,
            Collection<InetSocketAddress> nodes, THostPort schedulerAddress);

    /**
     * Returns a List of {@link TTaskLaunchSpec}s describing tasks that should be launched from the
     * give node monitor.  Always returns either 0 or 1 tasks.
     */
    public List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress);

    /** Returns true if all of the job's tasks have been placed. */
    public boolean allTasksPlaced();

    /** Returns the node monitors with outstanding reservations for this request.
     *
     * After this method is called once, the TaskPlacer assumes all those node monitors were sent
     * cancellation messages, so it will not return any node monitors in the future. */
    public Set<THostPort> getOutstandingNodeMonitorsForCancellation();
}
