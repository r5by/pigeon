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

package edu.utarlington.pigeon.api;

import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.TClients;
import edu.utarlington.pigeon.daemon.util.TServers;
import edu.utarlington.pigeon.examples.SimpleFrontend;
import edu.utarlington.pigeon.thrift.*;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Java client to Pigeon scheduling service. Once a client is initialize()'d it
 * can be used safely from multiple threads.
 */
public class PigeonFrontendClient {
    private final static Logger LOG = Logger.getLogger(PigeonFrontendClient.class);
//    private final static int NUM_CLIENTS = 8; // Number of concurrent requests we support
    private final static int NUM_CLIENTS = 2; // Number of concurrent requests we support
    private final static int DEFAULT_FRONTEND_THREADS = 2;
    private final static int DEFAULT_LISTEN_PORT = 50201;

    BlockingQueue<SchedulerService.Client> clients =
            new LinkedBlockingQueue<SchedulerService.Client>();

    public static boolean launchedServerAlready = false;

    public boolean submitJob(String applicationId, double avgTasksD, List<TTaskSpec> tasks, TUserGroupInfo user) throws TException {
        TSchedulingRequest request = new TSchedulingRequest(applicationId, tasks, user);
        request.setAvgTasksExecDuration(avgTasksD);
        return submitRequest(request);
    }

    //For Spark
    public boolean submitJob(String applicationId, List<TTaskSpec> tasks, TUserGroupInfo user, String description) throws TException {
        TSchedulingRequest request = new TSchedulingRequest(applicationId, tasks, user);
        request.setDescription(description);
        return submitRequest(request);
    }

    private boolean submitRequest(TSchedulingRequest request) {
        try {
            SchedulerService.Client client = clients.take();
            client.submitJob(request);
            clients.put(client);
        } catch (InterruptedException e) {
            LOG.fatal(e);
        } catch (TException e) {
            LOG.error("Thrift exception when submitting job: " + e.getMessage());
            return false;
        }

        return true;
    }

    /**
     * Initialize a connection to a pigeon scheduler.
     * 1) Prepare FrontendService servers
     * 2) Prepare SchedulerService clients
     * @param schedularAddr The socket address of the Pigeon scheduler.
     * @param applicationId The application id. Note that this must be consistent across frontends
     *    *             and backends.
     * @param frontendServer A class which implements the frontend server interface (for
     *    *                        communication from Pigeon).
     * @throws TException
     * @throws IOException
     */
    public void initialize(InetSocketAddress schedularAddr, String applicationId, FrontendService.Iface frontendServer)
            throws TException, IOException {
        initialize(schedularAddr, applicationId, frontendServer, DEFAULT_LISTEN_PORT);
    }

    private void initialize(InetSocketAddress schedularAddr, String applicationId, FrontendService.Iface frontendServer, int listenPort)
            throws TException, IOException {
        FrontendService.Processor<FrontendService.Iface> processor =
                new FrontendService.Processor<FrontendService.Iface>(frontendServer);

        if (!launchedServerAlready) {
            try {
                TServers.launchThreadedThriftServer(listenPort, DEFAULT_FRONTEND_THREADS, processor);
            } catch (IOException e) {
                LOG.fatal("Couldn't launch server side of frontend", e);
            }
            launchedServerAlready = true;
        }

        for (int i = 0; i < NUM_CLIENTS; i++) {
            SchedulerService.Client client = TClients.createBlockingSchedulerClient(
                    schedularAddr.getAddress().getHostAddress(), schedularAddr.getPort(),
                    600000);
            clients.add(client);
        }
        clients.peek().registerFrontend(applicationId, Network.getIPAddress(new PropertiesConfiguration())
                + ":" + listenPort);
    }

    public void close() {
        for (int i = 0; i < NUM_CLIENTS; i++) {
            clients.poll().getOutputProtocol().getTransport().close();
        }
    }
}
