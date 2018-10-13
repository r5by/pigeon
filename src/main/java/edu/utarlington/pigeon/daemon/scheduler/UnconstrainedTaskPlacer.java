package edu.utarlington.pigeon.daemon.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.utarlington.pigeon.thrift.*;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * A task placer for jobs whose tasks have no placement constraints.
 */
public class UnconstrainedTaskPlacer implements TaskPlacer {
    private static final Logger LOG = Logger.getLogger(UnconstrainedTaskPlacer.class);

    /** Specifications for tasks that have not yet been launched. */
    List<TTaskLaunchSpec> unlaunchedTasks;

    /**
     * For each node monitor where reservations were enqueued, the number of reservations that were
     * enqueued there.
     */
    private Map<THostPort, Integer> outstandingReservations;

    /** Whether the remaining reservations have been cancelled. */
    boolean cancelled;

    /**
     * Id of the request associated with this task placer.
     */
    String requestId;

    private double probeRatio;

    UnconstrainedTaskPlacer(String requestId, double probeRatio) {
        this.requestId = requestId;
        this.probeRatio = probeRatio;
        unlaunchedTasks = new LinkedList<TTaskLaunchSpec>();
        outstandingReservations = new HashMap<THostPort, Integer>();
        cancelled = false;
    }

    @Override
    public Map<InetSocketAddress, TEnqueueTaskReservationsRequest>
    getEnqueueTaskReservationsRequests(
            TSchedulingRequest schedulingRequest, String requestId,
            Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {
//        LOG.debug(Logging.functionCall(schedulingRequest, requestId, nodes, schedulerAddress));

        int numTasks = schedulingRequest.getTasks().size();
        int reservationsToLaunch = (int) Math.ceil(probeRatio * numTasks);
        LOG.debug("Request " + requestId + ": Creating " + reservationsToLaunch +
                " task reservations for " + numTasks + " tasks");

        // Get a random subset of nodes by shuffling list.
        List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
        Collections.shuffle(nodeList);
        if (reservationsToLaunch < nodeList.size())
            nodeList = nodeList.subList(0, reservationsToLaunch);

        for (TTaskSpec task : schedulingRequest.getTasks()) {
            TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(),
                    task.bufferForMessage());
            unlaunchedTasks.add(taskLaunchSpec);
        }

        HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();

        int numReservationsPerNode = 1;
        if (nodeList.size() < reservationsToLaunch) {
            numReservationsPerNode = reservationsToLaunch / nodeList.size();
        }
        StringBuilder debugString = new StringBuilder();
        for (int i = 0; i < nodeList.size(); i++) {
            int numReservations = numReservationsPerNode;
            if (reservationsToLaunch % nodeList.size() > i)
                ++numReservations;
            InetSocketAddress node = nodeList.get(i);
            debugString.append(node.getAddress().getHostAddress() + ":" + node.getPort());
            debugString.append(";");
            // TODO: this needs to be a count!
            outstandingReservations.put(
                    new THostPort(node.getAddress().getHostAddress(), node.getPort()),
                    numReservations);
            TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(
                    schedulingRequest.getApp(), schedulingRequest.getUser(), requestId,
                    schedulerAddress, numReservations);
            requests.put(node, request);
        }
        LOG.debug("Request " + requestId + ": Launching enqueueReservation on " +
                nodeList.size() + " node monitors: " + debugString.toString());
        return requests;
    }

    @Override
    public List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress) {
        Integer numOutstandingReservations = outstandingReservations.get(nodeMonitorAddress);
        if (numOutstandingReservations == null) {
            LOG.error("Node monitor " + nodeMonitorAddress +
                    " not in list of outstanding reservations");
            return Lists.newArrayList();
        }
        if (numOutstandingReservations == 1) {
            outstandingReservations.remove(nodeMonitorAddress);
        } else {
            outstandingReservations.put(nodeMonitorAddress, numOutstandingReservations - 1);
        }

        if (unlaunchedTasks.isEmpty()) {
            LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
                    ": Not assigning a task (no remaining unlaunched tasks).");
            return Lists.newArrayList();
        } else {
            TTaskLaunchSpec launchSpec = unlaunchedTasks.get(0);
            unlaunchedTasks.remove(0);
            LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
                    ": Assigning task");
            return Lists.newArrayList(launchSpec);
        }
    }

    @Override
    public boolean allTasksPlaced() {
        return unlaunchedTasks.isEmpty();
    }

    @Override
    public Set<THostPort> getOutstandingNodeMonitorsForCancellation() {
        if (!cancelled) {
            cancelled = true;
            return outstandingReservations.keySet();
        }
        return new HashSet<THostPort>();
    }
}

