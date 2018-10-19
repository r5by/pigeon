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
public class SchedulerThrift implements SchedulerService.Iface, GetTaskService.Iface {
    // Defaults if not specified by configuration
    public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
    private final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 8;
    public final static int DEFAULT_GET_TASK_PORT = 20507;

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
    public TLaunchTaskRequest getTask(String requestID, THostPort nodeMonitorAddress) throws TException {
        return scheduler.getTask(requestID, nodeMonitorAddress);
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

        int port = conf.getInt(PigeonConf.SCHEDULER_THRIFT_PORT,
                DEFAULT_SCHEDULER_THRIFT_PORT);
        int threads = conf.getInt(PigeonConf.SCHEDULER_THRIFT_THREADS,
                DEFAULT_SCHEDULER_THRIFT_THREADS);
        String hostname = Network.getHostName(conf);
        InetSocketAddress addr = new InetSocketAddress(hostname, port);
        scheduler.initialize(conf, addr);
        TServers.launchThreadedThriftServer(port, threads, processor);

        int getTaskPort = conf.getInt(PigeonConf.GET_TASK_PORT,
                DEFAULT_GET_TASK_PORT);
        GetTaskService.Processor<GetTaskService.Iface> getTaskprocessor =
                new GetTaskService.Processor<GetTaskService.Iface>(this);
        TServers.launchSingleThreadThriftServer(getTaskPort, getTaskprocessor);
    }
}
