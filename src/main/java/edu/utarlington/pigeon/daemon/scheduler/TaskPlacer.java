package edu.utarlington.pigeon.daemon.scheduler;

//import edu.utarlington.pigeon.thrift.TEnqueueTaskReservationsRequest;
import edu.utarlington.pigeon.thrift.THostPort;
import edu.utarlington.pigeon.thrift.TLaunchTaskRequest;
import edu.utarlington.pigeon.thrift.TSchedulingRequest;
import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

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
    /** Returns true if all of the job's tasks have been placed. */
    public boolean allTasksPlaced();

    /** Returns the node monitors with outstanding reservations for this request.
     *
     * After this method is called once, the TaskPlacer assumes all those node monitors were sent
     * cancellation messages, so it will not return any node monitors in the future. */
//    public Set<THostPort> getOutstandingNodeMonitorsForCancellation();

    /**
     * Enqueue task request */
    void enqueueRequest(TSchedulingRequest request, THostPort address) throws Exception;

    /** Records of completed/total tasks of the request */
    public int completedTasks();
    public int totalTasks();
    public int count();
}
