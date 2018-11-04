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

import edu.utarlington.pigeon.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Wrapper class for launching various thrift services clients for Pigeon
 *
 */

public class TClients {
    private final static Logger LOG = Logger.getLogger(TClients.class);

    //=======================================
    // Master services clients
    //=======================================
    public static MasterService.Client createBlockingMasterClient(
            InetSocketAddress socket) throws IOException {
        return createBlockingMasterClient(socket.getAddress().getHostAddress(), socket.getPort());
    }

    public static MasterService.Client createBlockingMasterClient(String host, int port)
            throws IOException {
        return createBlockingMasterClient(host, port, 0);
    }

    public static MasterService.Client createBlockingMasterClient(String host, int port,
                                                                   int timeout)
            throws IOException {
        TTransport tr = new TFramedTransport(new TSocket(host, port, timeout));
        try {
            tr.open();
        } catch (TTransportException e) {
            LOG.warn("Error creating node monitor client to " + host + ":" + port);
            throw new IOException(e);
        }
        TProtocol proto = new TBinaryProtocol(tr);
        MasterService.Client client = new MasterService.Client(proto);
        return client;
    }

    //=======================================
    // Recursive services clients
    //=======================================
    public static RecursiveService.Client createBlockingRecursiveClient(
            String host, int port) throws IOException {
        return createBlockingRecursiveClient(host, port, 0);
    }

    public static RecursiveService.Client createBlockingRecursiveClient(
            String host, int port, int timeout) throws IOException {
        TTransport tr = new TFramedTransport(new TSocket(host, port, timeout));
        try {
            tr.open();
        } catch (TTransportException e) {
            LOG.warn("Error creating scheduler client to " + host + ":" + port);
            throw new IOException(e);
        }
        TProtocol proto = new TBinaryProtocol(tr);
        RecursiveService.Client client = new RecursiveService.Client(proto);
        return client;
    }

    //=======================================
    // Backend services clients
    //=======================================
    public static BackendService.Client createBlockingBackendClient(
            InetSocketAddress socket) throws IOException {
        return createBlockingBackendClient(socket.getAddress().getHostAddress(), socket.getPort());
    }

    public static BackendService.Client createBlockingBackendClient(
            String host, int port) throws IOException {
        TTransport tr = new TFramedTransport(new TSocket(host, port));
        try {
            tr.open();
        } catch (TTransportException e) {
            LOG.warn("Error creating backend client to " + host + ":" + port);
            throw new IOException(e);
        }
        TProtocol proto = new TBinaryProtocol(tr);
        BackendService.Client client = new BackendService.Client(proto);
        return client;
    }

    //=======================================
    // Frontend services clients
    //=======================================
    public static SchedulerService.Client createBlockingSchedulerClient(
            String host, int port) throws IOException {
        return createBlockingSchedulerClient(host, port, 0);
    }

    public static SchedulerService.Client createBlockingSchedulerClient(String hostAddress, int port, int timeout)
            throws IOException {
        TTransport tr = new TFramedTransport(new TSocket(hostAddress, port, timeout));
        try {
            tr.open();
        } catch (TTransportException e) {
            LOG.warn("Error creating scheduler client to " + hostAddress + ":" + port);
            throw new IOException(e);
        }
        TProtocol proto = new TBinaryProtocol(tr);
        SchedulerService.Client client = new SchedulerService.Client(proto);
        return client;
    }
}
