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

import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.thrift.THostPort;
import org.apache.commons.configuration.Configuration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class Network {

    public static InetSocketAddress thriftToSocketAddress(THostPort address) {
        return new InetSocketAddress(address.getHost(), address.getPort());
    }

    public static THostPort socketAddressToThrift(InetSocketAddress address) {
        return new THostPort(address.getAddress().getHostAddress(), address.getPort());
    }

    /** Return the hostname of this machine, based on configured value, or system
     * Interrogation. */
    public static String getHostName(Configuration conf) {
        String defaultHostname = null;
        try {
            defaultHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            defaultHostname = "localhost";
        }
        return conf.getString(PigeonConf.HOSTNAME, defaultHostname);
    }

    /**
     * Return the IP address of this machine, as determined from the hostname
     * specified in configuration or from querying the machine.
     */
    public static String getIPAddress(Configuration conf) {
        String hostname = getHostName(conf);
        try {
            return InetAddress.getByName(hostname).getHostAddress();
        } catch (UnknownHostException e) {
            return "IP UNKNOWN";
        }
    }

    /**
     * Return a new socket address by the ip address of {@param addr} with the port number of {@param port}
     */
    public static InetSocketAddress constructSocket(InetSocketAddress addr, int port) {
        return new InetSocketAddress(addr.getAddress(), port);
    }
}
