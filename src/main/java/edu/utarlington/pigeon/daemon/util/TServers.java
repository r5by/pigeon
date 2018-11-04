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

package edu.utarlington.pigeon.daemon.util;

import edu.utarlington.pigeon.thrift.BackendService;
import edu.utarlington.pigeon.thrift.MasterService;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * A wrapper class for launching various types of thrift servers
 *
 * @author ruby_
 * @create 2018-10-02-4:49 PM
 */

public class TServers {
    private final static Logger LOG = Logger.getLogger(TServers.class);
    private final static int SELECTOR_THREADS = 4;

    /**
     * BackendService server wrapper
     * Launch a single threaded nonblocking IO server. All requests to this server will be
     * handled in a single thread, so its requests should not contain blocking functions.
     */
    public static void launchSingleThreadThriftServer(int port, TProcessor processor)
        throws IOException {
        LOG.info("Staring async thrift server of type: " + processor.getClass().toString()
                + " on port " + port);
        TNonblockingServerTransport serverTransport;
        try {
            serverTransport = new TNonblockingServerSocket(port);
        } catch (TTransportException e) {
            throw new IOException(e);
        }
        TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor);
        TServer server = new TNonblockingServer(serverArgs);
        new Thread(new TServerRunnable(server)).start();
    }

    /**
     * NodeMonitorService server wrapper
     * Launch a multi-threaded Thrift server with the given {@code processor}. Note that
     * internally this creates an expanding thread pool of at most {@code threads} threads,
     * and requests are queued whenever that thread pool is saturated.
     */
    public static void launchThreadedThriftServer(int port, int threads, TProcessor processor)
    throws IOException{
        LOG.info("Staring async thrift server of type: " + processor.getClass().toString()
                + " on port " + port);
        TNonblockingServerTransport serverTransport;
        try {
            serverTransport = new TNonblockingServerSocket(port);
        } catch (TTransportException e) {
            throw new IOException(e);
        }
        TThreadedSelectorServer.Args serverArgs = new TThreadedSelectorServer.Args(serverTransport);
        serverArgs.transportFactory(new TFramedTransport.Factory());
        serverArgs.protocolFactory(new TBinaryProtocol.Factory());
        serverArgs.processor(processor);
        serverArgs.selectorThreads(SELECTOR_THREADS);
        serverArgs.workerThreads(threads);
        TServer server = new TThreadedSelectorServer(serverArgs);
        new Thread(new TServerRunnable(server)).start();
    }

    /**
     * Runnable class to wrap thrift servers in their own thread.
     */
    private static class TServerRunnable implements Runnable {
        private TServer server;

        public TServerRunnable(TServer server) {
            this.server = server;
        }

        public void run() {
            this.server.serve();
        }
    }


}
