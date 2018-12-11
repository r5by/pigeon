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
