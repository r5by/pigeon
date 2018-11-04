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

import edu.utarlington.pigeon.thrift.ServerNotReadyException;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface PigeonMasterState {
    /**
     * Initialize state storage. This should open connections to any external
     * services if required.
     * @throws IOException
     */
    public void initialize(Configuration conf) throws IOException;

    /**
     * Register the backend identified by {@code appId} which can be reached via
     * the given master running at the given address. The node is assumed to have
     * resources given by {@code capacity}.
     */
    public boolean registerBackend(String appId, InetSocketAddress masterAddr, InetSocketAddress backendAddr, int backendType);

    /**
     *  Used to check the master node's state, at start-up one Pigeon master should have at least one HW/LW
     *  After the first request from Pigeon scheduler verifies this master node, the master will be considered ready for all the following requests.
     *  Simply flap-over the flag
     */
    public boolean masterNodeUp();
}
