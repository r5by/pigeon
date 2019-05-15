package edu.utarlington.pigeon.daemon.master;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.Serialization;
import edu.utarlington.pigeon.daemon.util.ThriftClientPool;
import edu.utarlington.pigeon.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * A Pigeon master which is responsible for communicating with application
 * backends and scheduler. This class is wrapped by multiple thrift servers, so it may
 * be concurrently accessed when handling multiple function calls
 * simultaneously.
 *
 * 1) It maintain lists of available workers
 *
 * 2) It delegates the assignments of requests to taskScheduler
 */
public class PigeonMaster {

    private final static Logger LOG = Logger.getLogger(PigeonMaster.class);
//    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);

    private static PigeonMasterState state;

    //* --- Thread-Safe Fields --- *//
    // A map to hw/lw, filled in when backends register themselfs with their master
    private HashMap<String, List<InetSocketAddress>> appSocketsHIWs;
    private HashMap<String, List<InetSocketAddress>> appSocketsLIWs;
    //A map of occupied workers, keyed by the worker socket, the value is the worker's priority type
    private HashMap<InetSocketAddress, PriorityType> occupiedWorkers;
    //Record of number of tasks from the same requests
    private ConcurrentMap<String, Integer> requestNumberOfTasks =
            Maps.newConcurrentMap();
    //* --- Thread-Safe Fields (END)--- *//

    // Map to scheduler socket address for each request id.
    private ConcurrentMap<String, InetSocketAddress> requestSchedulers =
            Maps.newConcurrentMap();

    private ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool =
            new ThriftClientPool<SchedulerService.AsyncClient>(
                    new ThriftClientPool.SchedulerServiceMakerFactory());

    private TaskScheduler scheduler;
    private TaskLauncherService taskLauncherService;
    private String ipAddress;
    private int masterInternalPort;

    public void initialize(Configuration conf, int masterInternalPort) {
        String mode = conf.getString(PigeonConf.DEPLYOMENT_MODE, "unspecified");
        if (mode.equals("standalone")) {
            //TODO: Other mode
//            state = new StandaloneNodeMonitorState();
        } else if (mode.equals("configbased")) {
            state = new ConfigMasterState();
        } else {
            throw new RuntimeException("Unsupported deployment mode: " + mode);
        }
        try {
            state.initialize(conf);
        } catch (IOException e) {
            LOG.fatal("Error initializing node monitor state.", e);
        }

        //TODO: Maybe we can check whether the statically configured features matches the run-time environment?
//        int cores = Resources.getSystemCPUCount(conf);
//        LOG.info("Using core allocation: " + cores);
//        int mem = Resources.getSystemMemoryMb(conf);
//        LOG.info("Using memory allocation: " + mem);

        ipAddress = Network.getIPAddress(conf);
        this.masterInternalPort = masterInternalPort;

        String task_scheduler_type = conf.getString(PigeonConf.NM_TASK_SCHEDULER_TYPE, "fifo");
        //TODO: Other scheduler
        if (task_scheduler_type.equals("round_robin")) {
//            scheduler = new RoundRobinTaskScheduler(cores);
        } else if (task_scheduler_type.equals("fifo")) {
            scheduler = new FifoTaskScheduler();
        } else if (task_scheduler_type.equals("priority")) {
//            scheduler = new PriorityTaskScheduler(cores);
        } else {
            throw new RuntimeException("Unsupported task scheduler type: " + mode);
        }

        /** Initialize HIW/LIW and HTQ/LTQ for Pigeon master */
        appSocketsHIWs = new HashMap<String, List<InetSocketAddress>>();
        appSocketsLIWs = new HashMap<String, List<InetSocketAddress>>();
        /** Initialize of book-keeping of swapped workers */
        occupiedWorkers = new HashMap<InetSocketAddress, PriorityType>();

        /** Initialize task scheduler & task launcher service */
        scheduler.initialize(conf);
        taskLauncherService = new TaskLauncherService();
        taskLauncherService.initialize(conf, scheduler);
    }

    public boolean registerBackend(String app, InetSocketAddress internalAddr, InetSocketAddress backendAddr, int type) {
        LOG.debug("Attempt to register worker: " + backendAddr + " at master:" + internalAddr + " for App: " + app);
        switch (type) {
            case 0:
                if (!appSocketsLIWs.containsKey(app))
                    appSocketsLIWs.put(app, Lists.newArrayList(backendAddr));
                else {
                    List<InetSocketAddress> LW = appSocketsLIWs.get(app);
                    if (!LW.contains(backendAddr))
                        LW.add(backendAddr);
                }
                break;
            case 1:
                if (!appSocketsHIWs.containsKey(app))
                    appSocketsHIWs.put(app, Lists.newArrayList(backendAddr));
                else {
                    List<InetSocketAddress> HW = appSocketsHIWs.get(app);
                    if (!HW.contains(backendAddr)) {
                        HW.add(backendAddr);
                    }
                }
                break;
            default:
                LOG.error("Invalid backend types!");
                break;
        }

        //TODO: verify the backend matches with the configured information
        return state.registerBackend(app, internalAddr, backendAddr, type);
    }

    //TODO: In future, this rpc() can return false to indicate unexpected request was assigned here
    public boolean launchTasksRequest(TLaunchTasksRequest request) throws TException{
        LOG.info("Received launch task request from " + ipAddress + " for request " + request.requestID);

        //TODO: sendFrontendMessage method should be accessed from recuirsive services, change the Thrif interface and uncomment the following statement
//        InetSocketAddress schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(), request.getSchedulerAddress().getPort());
        InetSocketAddress schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(), 20503);
        requestSchedulers.put(request.getRequestID(), schedulerAddress);

        synchronized (state) {
            List<InetSocketAddress> HIW = appSocketsHIWs.get(request.appID);
            List<InetSocketAddress> LIW = appSocketsLIWs.get(request.appID);

            //short-circuit to check if app backends (workers) have been started, o.w. throw out exceptions
            if(!state.masterNodeUp()) {
                if (HIW.isEmpty() || LIW.isEmpty()) {
                    throw new ServerNotReadyException("Master node must have more than one high/low priority worker available at start by default.");
                }
            }

            requestNumberOfTasks.put(request.requestID, request.tasksToBeLaunched.size());

            //Pigeon decides whether to send the task launch request or not based on the available resources
            for (TTaskLaunchSpec task : request.tasksToBeLaunched) {
                LOG.debug("Pigeon master handling submitted task: " + task.taskId + " for request: " + request.requestID + " expected execDurationMillis: " + task.message.getLong());
                InetSocketAddress worker = null;
                if (task.isHT) {//For high priority tasks
                    if (!LIW.isEmpty()) {
                        //If low priority idle queue is not empty, pick up one and send the request to the nm
                        worker = LIW.remove(0);
                        occupiedWorkers.put(worker, PriorityType.LOW);
                    } else if (!HIW.isEmpty()) {
                        //O.w. if  high priority idle queue is not empty, pick up one and send the request to that nm
                        worker = HIW.remove(0);
                        occupiedWorkers.put(worker, PriorityType.HIGH);
                    } else {//else enqueue the task in HTQ
                        scheduler.enqueue(
                                new TLaunchTasksRequest(request.appID, request.user, request.requestID, request.schedulerAddress, Lists.newArrayList(task)));
                    }
                } else {//For low priority tasks
                    if (!LIW.isEmpty()) {
                        //If low priority idle queue is not empty, pick up one and send the request to the nm
                        worker = LIW.remove(0);
                        occupiedWorkers.put(worker, PriorityType.LOW);
                    } else {
                        scheduler.enqueue(
                                new TLaunchTasksRequest(request.appID, request.user, request.requestID, request.schedulerAddress, Lists.newArrayList(task)));
                    }
                }

                //TODO: Assign more than 1 task to the worker based on its processing capability
                if(worker != null) {
                    TLaunchTasksRequest launchTasksRequest = new TLaunchTasksRequest(request.appID, request.user, request.requestID, request.schedulerAddress, Lists.newArrayList(task));
                    scheduler.submitLaunchTaskRequest(launchTasksRequest, worker);
                }
            }
        }
        return true;
    }

    public void taskFinished(List<TFullTaskId> task, THostPort worker) {
        InetSocketAddress idleWorker = Network.thriftToSocketAddress(worker);

        String app = task.get(0).appId;
        String requestId = task.get(0).requestId;

        //Instrument Pigeon to simulating send taskComplete() back to scheduler
        TFullTaskId t = task.get(0);
//        sendFrontendMessage(app, t, 0, null);

        synchronized (state) {
            if (!occupiedWorkers.containsKey(idleWorker))
                throw new RuntimeException("Unknown worker address, please verify the cluster configurations");

            //Handle the idle worker based on the task scheduler's logic
            //TODO: Handle more than one tasks finished from the same worker in future release
            boolean isIdle = scheduler.tasksFinished(task, idleWorker,
                    occupiedWorkers.get(idleWorker));

            if (isIdle) {
                LOG.debug("Worker: " + worker + " is now idle, putting it to the idle worker list");
                restoreWorker(app, idleWorker);
            } else
                LOG.debug("New task has been assigned to worker: " + worker);

            //Check if all tasks belong to the same request have been completed
            countTaskReservations(app, requestId);
        }
    }

    //restore the worker to idle worker list based on its {@Link PriorityType}
    private void restoreWorker(String app, InetSocketAddress worker) {
        switch (occupiedWorkers.get(worker)) {
            case HIGH:
                appSocketsHIWs.get(app).add(worker);
                break;
            case LOW:
                appSocketsLIWs.get(app).add(worker);
                break;
        }

        occupiedWorkers.remove(worker);
    }

    //Count the number of tasks finished for particular request; if so, handle the situation
    private void countTaskReservations(String appId, String requestId) {
        int counter = requestNumberOfTasks.get(requestId);
        counter--;
        if (counter == 0) {
            requestNumberOfTasks.remove(requestId);
            //If all tasks from the same request finished, inform the Pigeon scheduler
            scheduler.noTaskForReservation(appId, requestId, requestSchedulers.get(requestId), getMasterInternalSocket());
            requestSchedulers.remove(requestId);
        } else
            requestNumberOfTasks.put(requestId, counter);
    }

    private THostPort getMasterInternalSocket() {
        InetSocketAddress socket = Serialization.strToSocket(ipAddress + ":" + String.valueOf(masterInternalPort)).get();
        return Network.socketAddressToThrift(socket);
    }

    private class sendFrontendMessageCallback implements
            AsyncMethodCallback<SchedulerService.AsyncClient.sendFrontendMessage_call> {
        private InetSocketAddress frontendSocket;
        private SchedulerService.AsyncClient client;

        public sendFrontendMessageCallback(InetSocketAddress socket, SchedulerService.AsyncClient client) {
            frontendSocket = socket;
            this.client = client;
        }

        public void onComplete(SchedulerService.AsyncClient.sendFrontendMessage_call response) {
            try {
                schedulerClientPool.returnClient(frontendSocket, client);
            } catch (Exception e) {
                LOG.error(e);
            }
        }

        public void onError(Exception exception) {
            try {
                schedulerClientPool.returnClient(frontendSocket, client);
            } catch (Exception e) {
                LOG.error(e);
            }
            LOG.error(exception);
        }
    }

    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) {
        //        LOG.debug(Logging.functionCall(app, taskId, message));
        InetSocketAddress scheduler = requestSchedulers.get(taskId.requestId);
        if (scheduler == null) {
            LOG.error("Did not find any scheduler info for request: " + taskId);
            return;
        }

        try {
            SchedulerService.AsyncClient client = schedulerClientPool.borrowClient(scheduler);
            client.sendFrontendMessage(app, taskId, status, message,
                    new sendFrontendMessageCallback(scheduler, client));
            LOG.debug("finished sending message to frontend!");
        } catch (IOException e) {
            LOG.error(e);
        } catch (TException e) {
            LOG.error(e);
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}

