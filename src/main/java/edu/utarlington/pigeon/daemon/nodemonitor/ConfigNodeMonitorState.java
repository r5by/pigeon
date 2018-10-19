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
import edu.utarlington.pigeon.daemon.util.ConfigUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

/***
 * A {@link NodeMonitorState} implementation based on a static config file.
 */

public class ConfigNodeMonitorState implements NodeMonitorState{
    private static final Logger LOG = Logger.getLogger(ConfigNodeMonitorState.class);

    private Set<InetSocketAddress> nodeMonitorsHW;
    private Set<InetSocketAddress> nodeMonitorsLW;
    private String staticAppId;

    @Override
    public void initialize(Configuration conf) throws IOException {
        //Pigeon initializes node monitors for high/low priority task separately
        nodeMonitorsHW = ConfigUtil.parseBackends(conf, PigeonConf.STATIC_NODE_MONITORS_HW);
        nodeMonitorsLW = ConfigUtil.parseBackends(conf, PigeonConf.STATIC_NODE_MONITORS_LW);
        staticAppId = conf.getString(PigeonConf.STATIC_APP_NAME);
    }

    @Override
    public boolean registerBackend(String appId, InetSocketAddress nodeMonitor) {
        // Verify that the given backend information matches the static configuration.
        if (!appId.equals(staticAppId)) {
            LOG.error("Requested to register backend for app " + appId +
                    " but was expecting app " + staticAppId);
        } else if (!(nodeMonitorsHW.contains(nodeMonitor)
                        || nodeMonitorsLW.contains(nodeMonitor))) {
            StringBuilder errorMessage = new StringBuilder();

            for (InetSocketAddress nodeMonitorAddress : nodeMonitorsHW) {
                errorMessage.append(nodeMonitorAddress.toString());
            }
            for (InetSocketAddress nodeMonitorAddress : nodeMonitorsLW) {
                errorMessage.append(nodeMonitorAddress.toString());
            }
            throw new RuntimeException("Address " + nodeMonitor.toString() +
                    " not found among statically configured addreses for app " + appId + " (statically " +
                    "configured addresses include: " + errorMessage.toString());
        }

        return true;
    }
}
