package edu.utarlington.pigeon.daemon.scheduler;

//import edu.utarlington.pigeon.thrift.TEnqueueTaskReservationsRequest;
import edu.utarlington.pigeon.thrift.THostPort;
import edu.utarlington.pigeon.thrift.TSchedulingRequest;
import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/***
 * TODO: add comments
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
    void processRequest(TSchedulingRequest request, THostPort address) throws Exception;

    /** Records of completed/total tasks of the request */
    public int completedTasks();
    public int totalTasks();
    public int count(THostPort addr);
}
