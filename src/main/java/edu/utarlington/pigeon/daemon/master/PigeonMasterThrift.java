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

package edu.utarlington.pigeon.daemon.master;

import com.google.common.base.Optional;
import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.Serialization;
import edu.utarlington.pigeon.daemon.util.TServers;
import edu.utarlington.pigeon.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class extends the thrift Pigeon master interface. It wraps the
 * {@link PigeonMaster} class and delegates most calls to that class.
 */
public class PigeonMasterThrift implements MasterService.Iface, InternalService.Iface {
    // Defaults if not specified by configuration
    public final static int DEFAULT_MASTER_THRIFT_PORT = 20501;
    public final static int DEFAULT_MASTER_THRIFT_THREADS = 8;
    public final static int DEFAULT_INTERNAL_THRIFT_PORT = 20502;
    public final static int DEFAULT_INTERNAL_THRIFT_THREADS = 8;

    private PigeonMaster pigeonMaster = new PigeonMaster();
    // The socket addr (ip:port) where we listen for requests from other Pigeon daemons.
    // Used when registering backends with the state store.
    private InetSocketAddress internalAddr;

    /**
     * Initialize this thrift service.
     *
     * This spawns 2 multi-threaded thrift servers, one exposing the app-facing
     * agent service and the other exposing the internal-facing agent service,
     * and listens for requests to both servers. We require explicit specification of the
     * ports for these respective interfaces, since they cannot always be determined from
     * within this class under certain configurations (e.g. a config file specifies
     * multiple NodeMonitors).
     */
    public void initialize(Configuration conf, int masterPort, int internalPort)
            throws IOException {
        pigeonMaster.initialize(conf, internalPort);

        // Setup application-facing agent service.
        MasterService.Processor<MasterService.Iface> processor =
                new MasterService.Processor<MasterService.Iface>(this);

        int threads = conf.getInt(PigeonConf.MASTER_THRIFT_THREADS,
                DEFAULT_MASTER_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(masterPort, threads, processor);

        // Setup internal-facing agent service.
        InternalService.Processor<InternalService.Iface> internalProcessor =
                new InternalService.Processor<InternalService.Iface>(this);
        int internalThreads = conf.getInt(
                PigeonConf.INTERNAL_THRIFT_THREADS,
                DEFAULT_INTERNAL_THRIFT_THREADS);
        TServers.launchThreadedThriftServer(internalPort, internalThreads, internalProcessor);

        internalAddr = new InetSocketAddress(Network.getHostName(conf), internalPort);
    }

    @Override
    public boolean registerBackend(String app, String backendSocket, int type) throws TException {
        Optional<InetSocketAddress> backendAddr = Serialization.strToSocket(backendSocket);
        if (!backendAddr.isPresent()) {
            return false;
        }
        return pigeonMaster.registerBackend(app, internalAddr, backendAddr.get(), type);
    }

    @Override
    public void taskFinished(List<TFullTaskId> task, THostPort worker) throws TException {
        pigeonMaster.taskFinished(task, worker);
    }

    @Override
    public boolean launchTasksRequest(TLaunchTasksRequest launchTasksRequest) throws TException {
        return pigeonMaster.launchTasksRequest(launchTasksRequest);
    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {
        pigeonMaster.sendFrontendMessage(app, taskId, status, message);
    }
}
