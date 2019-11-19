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

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.ListUtils;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.Serialization;
import edu.utarlington.pigeon.daemon.util.ThriftClientPool;
import edu.utarlington.pigeon.thrift.*;
import edu.utarlington.pigeon.thrift.FrontendService.AsyncClient.frontendMessage_call;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;



/**
 * This class implements the Sparrow scheduler functionality.
 */
//TODO: Scheduling more than 1 task to be sent to node monitor with sufficient resources
public class Scheduler {
    private final static Logger LOG = Logger.getLogger(Scheduler.class);

    /** Used to uniquely identify requests arriving at this scheduler. */
    private AtomicInteger counter = new AtomicInteger(0);


    private THostPort address;
    private double cutoff;

    /** Socket addresses for each frontend. */
    HashMap<String, InetSocketAddress> frontendSockets =
            new HashMap<String, InetSocketAddress>();

    /** Thrift client pool for communicating with node monitors */
    ThriftClientPool<InternalService.AsyncClient> masterClientPool =
            new ThriftClientPool<InternalService.AsyncClient>(
                    new ThriftClientPool.InternalServiceMakerFactory());

    /** Thrift client pool for communicating with front ends. */
    private ThriftClientPool<FrontendService.AsyncClient> frontendClientPool =
            new ThriftClientPool<FrontendService.AsyncClient>(
                    new ThriftClientPool.FrontendServiceMakerFactory());

    /** Information about cluster workload due to other schedulers. */
    private SchedulerState state;

    /**
     * For each request, the task placer that should be used to place the request's tasks. Indexed
     * by the request ID.
     */
    private ConcurrentMap<String, TaskPlacer> requestTaskPlacers;

    /**
     * Pigeon
     * Master nodes list
      */
    public List<InetSocketAddress> pigeonMasters;

    /**
     * Pigeon
     * High/low priority task list
     */
//    public final Queue<TLaunchTasksRequest> HTQ = new LinkedList<TLaunchTasksRequest>();
//    public final Queue<TLaunchTasksRequest> LTQ = new LinkedList<TLaunchTasksRequest>();

    /**
     * When a job includes SPREAD_EVENLY in the description and has this number of tasks,
     * Sparrow spreads the tasks evenly over machines to evenly cache data. We need this (in
     * addition to the SPREAD_EVENLY descriptor) because only the reduce phase -- not the map
     * phase -- should be spread.
     */

    private Configuration conf;

    public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {
        address = Network.socketAddressToThrift(socket);
        String mode = conf.getString(PigeonConf.DEPLYOMENT_MODE, "unspecified");
        cutoff = conf.getDouble(PigeonConf.TR_CUTOFF, PigeonConf.TR_CUTOFF_DEFAULT);
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

        pigeonMasters = new ArrayList<InetSocketAddress>(state.getMasters());

        requestTaskPlacers = Maps.newConcurrentMap();
    }

    public boolean registerFrontend(String appId, String addr) {
        Optional<InetSocketAddress> socketAddress = Serialization.strToSocket(addr);
        if (!socketAddress.isPresent()) {
            LOG.error("Bad address from frontend: " + addr);
            return false;
        }
        frontendSockets.put(appId, socketAddress.get());
        return state.watchApplication(appId);
    }


    /** Checks whether we should add constraints to this job to evenly spread tasks over machines.
     *
     * This is a hack used to force Spark to cache data in 3 locations: we run 3 select * queries
     * on the same table and spread the tasks for those queries evenly across the cluster such that
     * the input data for the query is triple replicated and spread evenly across the cluster.
     *
     * We signal that Sparrow should use this hack by adding SPREAD_TASKS to the job's description.
     */
    private boolean isSpreadTasksJob(TSchedulingRequest request) {
        LOG.debug("Not special case: description did not contain SPREAD_EVENLY");
        return false;
    }

    public void submitJob(TSchedulingRequest request) throws TException {
        // Short-circuit case that is used for liveness checking
        if (request.tasks.size() == 0) { return; }
        if (isSpreadTasksJob(request)) {
         } else {
            handleJobSubmission(request);
        }
    }

    public void handleJobSubmission(TSchedulingRequest request) throws TException {
        long start = System.currentTimeMillis();
        String requestId = getRequestId();
        int totalTasks = request.getTasksSize();


        TaskPlacer taskPlacer = new UnconstrainedTaskPlacer(requestId, totalTasks, this);
        requestTaskPlacers.put(requestId, taskPlacer);

        try {
            LOG.debug("Enqueueing and sending task launch requests for request: " + requestId);
            taskPlacer.processRequest(request, address);
        } catch (Exception e) {
            LOG.error("Error processing task launch request at scheduler: " + address +":" + e);
        }

        long end = System.currentTimeMillis();
        LOG.debug("All tasks are sent or enqueued for request " + requestId + ". Taking total time: " +
                (end - start) + " milliseconds");
    }


    //TODO: 1) Impl Constrained scenario 2) Audit logging support
    public void tasksFinished(String requestId, THostPort masterAddress) {
        LOG.debug("Tasks completed by master node:" + masterAddress);

        TaskPlacer taskPlacer = requestTaskPlacers.get(requestId);
        if (taskPlacer != null) {
            synchronized (taskPlacer) {
                //Only keep records here for logging purpose, note the counter need to be atomic with allTasksPlaced() for mutil-access purpose
                taskPlacer.count(masterAddress);
                LOG.debug(taskPlacer.completedTasks() + "/" + taskPlacer.totalTasks() + " of request: " + requestId + " has been completed.");

                if (taskPlacer.allTasksPlaced()) {
                    LOG.debug("All tasks for request:" + requestId + " has been completed!");
                    //TODO: merging with logging support: write-out the request execution time for requestId
                    long latency = taskPlacer.getExecDurationMillis(System.currentTimeMillis());
                    LOG.debug("Request: " + requestId + " Request Type: " + taskPlacer.getPriority() + " exec latency: " + latency);
                    String requestInfo = "Request: " + requestId + " Request Type: " + taskPlacer.getPriority() + " exec latency: " + latency;
                    CreateNewTxt(requestInfo);
                    requestTaskPlacers.remove(requestId);
                }
            }

        }
    }

    /*Output txt file*/
    public void CreateNewTxt(String requestInfo){
        BufferedWriter output = null;
        try {
            File file = new File("requestInfo.txt");
            output = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true), "utf-8"));
            output.write(requestInfo+"\r\n");
            output.close();
        } catch ( IOException e ) {
            e.printStackTrace();
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

        //When a task is completed, remove the task from the request
        String requestId = taskId.requestId;

        InetSocketAddress frontend = frontendSockets.get(app);
        if (frontend == null) {
            LOG.error("Requested message sent to unregistered app: " + app);
        }
        try {
            FrontendService.AsyncClient client = frontendClientPool.borrowClient(frontend);
        } catch (IOException e) {
            LOG.error("Error launching message on frontend: " + app, e);
        } catch (TException e) {
            LOG.error("Error launching message on frontend: " + app, e);
        } catch (Exception e) {
            LOG.error("Error launching message on frontend: " + app, e);
        }
    }

    public InetSocketAddress getMaster(int i) throws Exception {
        if(i == 0)
            Collections.shuffle(pigeonMasters);

        return pigeonMasters.get(i);
    }

    public double getCutoff() {
        return this.cutoff;
    }

    //===============================
    // Extension: Instruments
    //===============================

    /**
     * Per-request related reservation book-keeping. Used to instrument Sparrow so we know the elapsed time for every request (the time starting from incoming request gets scheduled, to the last of its task finished)
     */
    class RequestTasksRecords {
        long start;
        long end;
        int remainingTasks;
        boolean shortjob;

        RequestTasksRecords(long pStart, int tasksSize, boolean isShortjob) {
            start = pStart;
            remainingTasks = tasksSize;
            shortjob = isShortjob;
        }

        /* When a task complete, decrease the reservations; if no reservations left record the end time stamp */
        void handleTaskComplete(boolean isHT) {
            remainingTasks--;
            if(isHT != shortjob) shortjob = isHT;
            if(remainingTasks ==0)
                end = System.currentTimeMillis();
        }

        boolean allTasksCompleted() {
            if(remainingTasks == 0) {
                remainingTasks = -1; //Reset the flag to avoid multiple access the critical section
                return true;
            } else
                return false;
        }

        long elapsed() {
            return end - start;
        }

        boolean isShortjob() {
            return shortjob;
        }
    }

}
