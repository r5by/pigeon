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

package edu.utarlington.pigeon.daemon.nodemonitor;

import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.Resources;
import edu.utarlington.pigeon.thrift.TFullTaskId;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A Node Monitor which is responsible for communicating with application
 * backends. This class is wrapped by multiple thrift servers, so it may
 * be concurrently accessed when handling multiple function calls
 * simultaneously.
 */
public class NodeMonitor {
    private final static Logger LOG = Logger.getLogger(NodeMonitor.class);
    //TODO: Add pigeon specified logging
//    private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
    private static NodeMonitorState state;

    //=======================================
    // Fileds
    //=======================================
    private String ipAddress;
    private TaskScheduler scheduler;
    private TaskLauncherService taskLauncherService;

    private HashMap<String, InetSocketAddress> appSockets =
            new HashMap<String, InetSocketAddress>();
    private HashMap<String, List<TFullTaskId>> appTasks =
            new HashMap<String, List<TFullTaskId>>();

    public void initialize(Configuration conf, int internalPort) throws RuntimeException {
        //STEP 1: Initialize deployment (registering backend addresses)
        //TODO: Add different mode support for pigeon
        String mode = conf.getString(PigeonConf.DEPLYOMENT_MODE, "unspecified");
        if (mode.equals("standalone")) {
//            state = new StandaloneNodeMonitorState();
        } else if (mode.equals("configbased")) {
            state = new ConfigNodeMonitorState();
        } else {
            throw new RuntimeException("Unsupported deployment mode: " + mode);
        }
        try {
            state.initialize(conf);
        } catch (IOException e) {
            LOG.fatal("Error initializing node monitor state.", e);
        }

        //STEP 2: Initialize resources
        int mem = Resources.getSystemMemoryMb(conf);
        LOG.info("Using memory allocation: " + mem);

        ipAddress = Network.getIPAddress(conf);

        int cores = Resources.getSystemCPUCount(conf);
        LOG.info("Using core allocation: " + cores);

        //STEP 3: Initialize connection with scheduler
        //TODO: Adding other types schedulers here
        String task_scheduler_type = conf.getString(PigeonConf.NM_TASK_SCHEDULER_TYPE, "fifo");
        if (task_scheduler_type.equals("round_robin")) {
//            scheduler = new RoundRobinTaskScheduler(cores);
        } else if (task_scheduler_type.equals("fifo")) {
            scheduler = new FifoTaskScheduler(cores);
        } else if (task_scheduler_type.equals("priority")) {
//            scheduler = new PriorityTaskScheduler(cores);
        } else {
            throw new RuntimeException("Unsupported task scheduler type: " + mode);
        }
        scheduler.initialize(conf, internalPort);
        new TaskLauncherService().initialize(conf, scheduler, internalPort);
    }

    /**
     * Registers the backend with assumed 0 load, and returns true if successful.
     * Returns false if the backend was already registered.
     */
    public boolean registerBackend(String app, InetSocketAddress internalAddr, InetSocketAddress backendAddr) {
//        LOG.debug(Logging.functionCall(appId, internalAddr, backendAddr));
        if (appSockets.containsKey(app)) {
            LOG.warn("Attempt to re-register app " + app);
            return false;
        }
        appSockets.put(app, backendAddr);
        appTasks.put(app, new ArrayList<TFullTaskId>());
        return state.registerBackend(app, internalAddr);
    }
}
