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

import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.TServers;
import edu.utarlington.pigeon.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class extends the thrift sparrow scheduler interface. It wraps the
 * {@link Scheduler} class and delegates most calls to that class.
 */
public class SchedulerThrift implements SchedulerService.Iface, RecursiveService.Iface {
    // Defaults if not specified by configuration
    public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
    private final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 8;
    public final static int DEFAULT_RECURSIVE_SERVICE_PORT = 20507;

    private Scheduler scheduler = new Scheduler();

    //=======================================
    // Scheduler Services (frontend)
    //=======================================
    @Override
    public boolean registerFrontend(String app, String socketAddress) throws TException {
        return scheduler.registerFrontend(app, socketAddress);
    }

    @Override
    public void submitJob(TSchedulingRequest req) throws TException {
        scheduler.submitJob(req);
    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {
        scheduler.sendFrontendMessage(app, taskId, status, message);
    }

    //=======================================
    // GetTask Services
    //=======================================


    @Override
    public void tasksFinished(String requestID, THostPort master) throws TException {
        scheduler.tasksFinished(requestID, master);
    }

    //=======================================
    // Daemon Services
    //=======================================
    /**
     * Initialize this thrift service.
     *
     * This spawns a multi-threaded thrift server and listens for Pigeon
     * scheduler requests.
     */
    public void initialize(Configuration conf) throws IOException {
        SchedulerService.Processor<SchedulerService.Iface> processor =
                new SchedulerService.Processor<SchedulerService.Iface>(this);

        int schedulerServicePort = conf.getInt(PigeonConf.SCHEDULER_THRIFT_PORT,
                DEFAULT_SCHEDULER_THRIFT_PORT);
        int recursivePort = conf.getInt(PigeonConf.RECURSIVE_SERVICE_PORT,
                DEFAULT_RECURSIVE_SERVICE_PORT);
        int threads = conf.getInt(PigeonConf.SCHEDULER_THRIFT_THREADS,
                DEFAULT_SCHEDULER_THRIFT_THREADS);

        String hostname = Network.getHostName(conf);
        //Initialize the address passed to launchTaskRequest, later be used for master to communicate with this scheduler
        InetSocketAddress addr = new InetSocketAddress(hostname, recursivePort);
        scheduler.initialize(conf, addr);

        TServers.launchThreadedThriftServer(schedulerServicePort, threads, processor);
        RecursiveService.Processor<RecursiveService.Iface> recursiveServiceProcessor =
                new RecursiveService.Processor<RecursiveService.Iface>(this);
        TServers.launchThreadedThriftServer(recursivePort, threads, recursiveServiceProcessor);

    }
}
