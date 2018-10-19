package edu.utarlington.pigeon.daemon.scheduler;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.Serialization;
import edu.utarlington.pigeon.daemon.util.ThriftClientPool;
import edu.utarlington.pigeon.thrift.*;
import edu.utarlington.pigeon.thrift.InternalService.AsyncClient;
import edu.utarlington.pigeon.thrift.InternalService.AsyncClient.launchTaskRequest_call;
import edu.utarlington.pigeon.thrift.FrontendService.AsyncClient.frontendMessage_call;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements the Sparrow scheduler functionality.
 */
//TODO: Scheduling more than 1 task to be sent to node monitor with sufficient resources
public class Scheduler {
    private final static Logger LOG = Logger.getLogger(Scheduler.class);
//    private final static Logger AUDIT_LOG = Logging.getAuditLogger(Scheduler.class);

    /** Used to uniquely identify requests arriving at this scheduler. */
    private AtomicInteger counter = new AtomicInteger(0);

    /** How many times the special case has been triggered. */
//    private AtomicInteger specialCaseCounter = new AtomicInteger(0);

    private THostPort address;

    /** Socket addresses for each frontend. */
    HashMap<String, InetSocketAddress> frontendSockets =
            new HashMap<String, InetSocketAddress>();

    /**
     * Service that handles cancelling outstanding reservations for jobs that have already been
     * scheduled.  Only instantiated if {@code SparrowConf.CANCELLATION} is set to true.
     */
//    private CancellationService cancellationService;
//    private boolean useCancellation;

    /** Thrift client pool for communicating with node monitors */
    ThriftClientPool<InternalService.AsyncClient> nodeMonitorClientPool =
            new ThriftClientPool<InternalService.AsyncClient>(
                    new ThriftClientPool.InternalServiceMakerFactory());

    /** Thrift client pool for communicating with front ends. */
    private ThriftClientPool<FrontendService.AsyncClient> frontendClientPool =
            new ThriftClientPool<FrontendService.AsyncClient>(
                    new ThriftClientPool.FrontendServiceMakerFactory());

    /** Information about cluster workload due to other schedulers. */
    private SchedulerState state;

    /** Probe ratios to use if the probe ratio is not explicitly set in the request. */
//    private double defaultProbeRatioUnconstrained;
//    private double defaultProbeRatioConstrained;

    /**
     * For each request, the task placer that should be used to place the request's tasks. Indexed
     * by the request ID.
     */
    private ConcurrentMap<String, TaskPlacer> requestTaskPlacers;

    /**
     * Pigeon
     * High/low priority idle worker lists
     */
    public List<InetSocketAddress> HIW;
    public List<InetSocketAddress> LIW;
    /**
     * Pigeon
     * High/low priority task list
     */
    public final Queue<TLaunchTaskRequest> HTQ = new LinkedList<TLaunchTaskRequest>();
    public final Queue<TLaunchTaskRequest> LTQ = new LinkedList<TLaunchTaskRequest>();

    /**
     * When a job includes SPREAD_EVENLY in the description and has this number of tasks,
     * Sparrow spreads the tasks evenly over machines to evenly cache data. We need this (in
     * addition to the SPREAD_EVENLY descriptor) because only the reduce phase -- not the map
     * phase -- should be spread.
     */
    //TODO: Enable evenly spread tasks
//    private int spreadEvenlyTaskSetSize;

    private Configuration conf;

    public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {
        address = Network.socketAddressToThrift(socket);
        String mode = conf.getString(PigeonConf.DEPLYOMENT_MODE, "unspecified");
        this.conf = conf;
        //TODO: Mode support
        if (mode.equals("standalone")) {
//            state = new StandaloneSchedulerState();
        } else if (mode.equals("configbased")) {
            state = new ConfigSchedulerState();
        } else {
            throw new RuntimeException("Unsupported deployment mode: " + mode);
        }

        state.initialize(conf);

        HIW =new ArrayList<InetSocketAddress>(state.getBackends(true));
        LIW =new ArrayList<InetSocketAddress>(state.getBackends(false));
        requestTaskPlacers = Maps.newConcurrentMap();
    }

    public boolean registerFrontend(String appId, String addr) {
//        LOG.debug(Logging.functionCall(appId, addr));
        Optional<InetSocketAddress> socketAddress = Serialization.strToSocket(addr);
        if (!socketAddress.isPresent()) {
            LOG.error("Bad address from frontend: " + addr);
            return false;
        }
        frontendSockets.put(appId, socketAddress.get());
        return state.watchApplication(appId);
    }

    /** Adds constraints such that tasks in the job will be spread evenly across the cluster.
     *
     *  We expect three of these special jobs to be submitted; 3 sequential calls to this
     *  method will result in spreading the tasks for the 3 jobs across the cluster such that no
     *  more than 1 task is assigned to each machine.
     */
    //TODO: Add evenly spread tasks constraints support for pigeon
//    private TSchedulingRequest addConstraintsToSpreadTasks(TSchedulingRequest req)
//            throws TException {
//        LOG.info("Handling spread tasks request: " + req);
//        int specialCaseIndex = specialCaseCounter.incrementAndGet();
//        if (specialCaseIndex < 1 || specialCaseIndex > 3) {
//            LOG.error("Invalid special case index: " + specialCaseIndex);
//        }
//
//        // No tasks have preferences and we have the magic number of tasks
//        TSchedulingRequest newReq = new TSchedulingRequest();
//        newReq.user = req.user;
//        newReq.app = req.app;
//        newReq.probeRatio = req.probeRatio;
//
//        //TODO: Add constraints (backends hw/lw)
//        List<InetSocketAddress> allBackends = Lists.newArrayList();
//        List<InetSocketAddress> backends = Lists.newArrayList();
//        // We assume the below always returns the same order (invalid assumption?)
//        for (InetSocketAddress backend : state.getBackends(req.app, false)) {
//            allBackends.add(backend);
//        }
//
//        // Each time this is called, we restrict to 1/3 of the nodes in the cluster
//        for (int i = 0; i < allBackends.size(); i++) {
//            if (i % 3 == specialCaseIndex - 1) {
//                backends.add(allBackends.get(i));
//            }
//        }
//        Collections.shuffle(backends);
//
//        if (!(allBackends.size() >= (req.getTasks().size() * 3))) {
//            LOG.error("Special case expects at least three times as many machines as tasks.");
//            return null;
//        }
//        LOG.info(backends);
//        for (int i = 0; i < req.getTasksSize(); i++) {
//            TTaskSpec task = req.getTasks().get(i);
//            TTaskSpec newTask = new TTaskSpec();
//            newTask.message = task.message;
//            newTask.taskId = task.taskId;
//            newTask.isHT = task.isHT;
//            newTask.preference = new TPlacementPreference();
//            newTask.preference.addToNodes(backends.get(i).getHostName());
//            newReq.addToTasks(newTask);
//        }
//        LOG.info("New request: " + newReq);
//        return newReq;
//    }

    /** Checks whether we should add constraints to this job to evenly spread tasks over machines.
     *
     * This is a hack used to force Spark to cache data in 3 locations: we run 3 select * queries
     * on the same table and spread the tasks for those queries evenly across the cluster such that
     * the input data for the query is triple replicated and spread evenly across the cluster.
     *
     * We signal that Sparrow should use this hack by adding SPREAD_TASKS to the job's description.
     */
    //TODO: Spread task support
    private boolean isSpreadTasksJob(TSchedulingRequest request) {
//        if ((request.getDescription() != null) &&
//                (request.getDescription().indexOf("SPREAD_EVENLY") != -1)) {
//            // Need to check to see if there are 3 constraints; if so, it's the map phase of the
//            // first job that reads the data from HDFS, so we shouldn't override the constraints.
//            for (TTaskSpec t: request.getTasks()) {
//                if (t.getPreference() != null && (t.getPreference().getNodes() != null)  &&
//                        (t.getPreference().getNodes().size() == 3)) {
//                    LOG.debug("Not special case: one of request's tasks had 3 preferences");
//                    return false;
//                }
//            }
//            if (request.getTasks().size() != spreadEvenlyTaskSetSize) {
//                LOG.debug("Not special case: job had " + request.getTasks().size() +
//                        " tasks rather than the expected " + spreadEvenlyTaskSetSize);
//                return false;
//            }
//            if (specialCaseCounter.get() >= 3) {
//                LOG.error("Not using special case because special case code has already been " +
//                        " called 3 more more times!");
//                return false;
//            }
//            LOG.debug("Spreading tasks for job with " + request.getTasks().size() + " tasks");
//            return true;
//        }
        LOG.debug("Not special case: description did not contain SPREAD_EVENLY");
        return false;
    }

    public void submitJob(TSchedulingRequest request) throws TException {
        // Short-circuit case that is used for liveness checking
        if (request.tasks.size() == 0) { return; }
        if (isSpreadTasksJob(request)) {
            //TODO: Impl constraints
//            handleJobSubmission(addConstraintsToSpreadTasks(request));
        } else {
            handleJobSubmission(request);
        }
    }

    //TODO: 1) Impl Constrained scenario 2) Audit logging support
    public void handleJobSubmission(TSchedulingRequest request) throws TException {
//        LOG.debug(Logging.functionCall(request));
        long start = System.currentTimeMillis();
        String requestId = getRequestId();
        int totalTasks = request.getTasksSize();

        TaskPlacer taskPlacer = new UnconstrainedTaskPlacer(requestId, totalTasks, this);
        requestTaskPlacers.put(requestId, taskPlacer);

        try {
            LOG.debug("Enqueueing and sending task launch requests for request: " + requestId);
            taskPlacer.enqueueRequest(request, address);
        } catch (Exception e) {
            LOG.error("Error processing task launch request at scheduler: " + address +":" + e);
        }

        long end = System.currentTimeMillis();
        LOG.debug("All tasks are sent or enqueued for request " + requestId + ". Taking total time: " +
                (end - start) + " milliseconds");
    }

    //TODO: 1) Impl Constrained scenario 2) Audit logging support
    public TLaunchTaskRequest getTask(String requestId, THostPort nodeMonitorAddress) {
        LOG.debug("Processing getTask() RPC call from node monitor: " + nodeMonitorAddress);

        TaskPlacer taskPlacer = requestTaskPlacers.get(requestId);

        if (taskPlacer != null) {
            //Only keep records here for logging purpose
            taskPlacer.count();
            LOG.debug(taskPlacer.completedTasks() + "/" + taskPlacer.totalTasks() + " of request: " + requestId + " has been completed.");

            if (taskPlacer.allTasksPlaced()) {
                LOG.debug("All tasks for request:" + requestId + " has been completed!");
                requestTaskPlacers.remove(requestId);
            }
        }

        /** H/LIW and H/LTQ should be accessed in synchronized ways across the lifycyle of this working scheduler service */
        synchronized (state) {
            InetSocketAddress worker = Network.thriftToSocketAddress(nodeMonitorAddress);

//            List<TLaunchTaskRequest> taskLaunchSpecs = Lists.newArrayList();
            TLaunchTaskRequest request = null;

            if (isHW(worker)) {//If received idle worker is from a high priority worker list
                if (!isHTQEmpty()) {
                    request = HTQ.poll();
                } else
                    addWorker(worker);
            } else {//If received idle worker is from a low priority worker list
                if (!isHTQEmpty()) {
                    request = HTQ.poll();
                } else if (!isLTQEmpty()) {
                    request = LTQ.poll();
                } else
                    addWorker(worker);
            }

            if(request == null) {
                LOG.debug("No more tasks need to be assigned to this node monitor: " + nodeMonitorAddress + ", putting it to idle worker list.");
                //If no more tasks need to be send, return a dummy feedback to nm to inform it no more tasks for it
                request = new TLaunchTaskRequest();
            }

            return request;
        }
    }

    /**
     * Returns an ID that identifies a request uniquely (across all Pigeon schedulers).
     *
     * This should only be called once for each request (it will return a different
     * identifier if called a second time).
     *
     * TODO: Include the port number, so this works when there are multiple schedulers
     * running on a single machine.
     */
    private String getRequestId() {
        /* The request id is a string that includes the IP address of this scheduler followed
         * by the counter.  We use a counter rather than a hash of the request because there
         * may be multiple requests to run an identical job. */
        return String.format("%s_%d", Network.getIPAddress(conf), counter.getAndIncrement());
    }

    private class sendFrontendMessageCallback implements
            AsyncMethodCallback<frontendMessage_call> {
        private InetSocketAddress frontendSocket;
        private FrontendService.AsyncClient client;
        public sendFrontendMessageCallback(InetSocketAddress socket, FrontendService.AsyncClient client) {
            frontendSocket = socket;
            this.client = client;
        }

        public void onComplete(frontendMessage_call response) {
            try { frontendClientPool.returnClient(frontendSocket, client); }
            catch (Exception e) { LOG.error(e); }
        }

        public void onError(Exception exception) {
            // Do not return error client to pool
            LOG.error("Error sending frontend message callback: " + exception);
        }
    }

    public void sendFrontendMessage(String app, TFullTaskId taskId,
                                    int status, ByteBuffer message) {
//        LOG.debug(Logging.functionCall(app, taskId, message));
        InetSocketAddress frontend = frontendSockets.get(app);
        if (frontend == null) {
            LOG.error("Requested message sent to unregistered app: " + app);
        }
        try {
            FrontendService.AsyncClient client = frontendClientPool.borrowClient(frontend);
            client.frontendMessage(taskId, status, message,
                    new sendFrontendMessageCallback(frontend, client));
        } catch (IOException e) {
            LOG.error("Error launching message on frontend: " + app, e);
        } catch (TException e) {
            LOG.error("Error launching message on frontend: " + app, e);
        } catch (Exception e) {
            LOG.error("Error launching message on frontend: " + app, e);
        }
    }



    /**
     * Pigeon enqueue launch task request
     */
    public boolean isLIWEmpty() { return LIW.isEmpty(); }
    public boolean isHIWEmpty() { return HIW.isEmpty(); }
    public boolean isHTQEmpty() { return HTQ.isEmpty(); }
    public boolean isLTQEmpty() { return LTQ.isEmpty(); }

    public int getUnlaunchedTaskQueueSize(boolean isHT) { return (isHT ? HTQ.size() : LTQ.size()); }

    public void enqueueLaunchTaskRequest(TLaunchTaskRequest request, boolean isHT) {
        if(isHT)
            HTQ.add(request);
        else
            LTQ.add(request);
    }

    /**
     * Returns an idle worker
     * @param isFromHIW
     * @return
     * @throws Exception
     */
    public InetSocketAddress getWorker(boolean isFromHIW) throws Exception {
        if(!isHIWEmpty() && isFromHIW) {
            Collections.shuffle(HIW);
            return HIW.remove(0);
        } else if(!isLIWEmpty() && !isFromHIW) {
            Collections.shuffle(LIW);
            return LIW.remove(0);
        } else
            throw new Exception("High priority workers are unavailable!");
    }

    public void addWorker(InetSocketAddress worker) {
        if (isHW(worker))
            HIW.add(worker);
        else
            LIW.add(worker);
    }

    public boolean isHW(InetSocketAddress worker) {
        return state.isHW(worker);
    }

}
