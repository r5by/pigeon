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
import edu.utarlington.pigeon.daemon.util.ConfigUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState{
    private static final Logger LOG = Logger.getLogger(ConfigSchedulerState.class);

    /* Pigeon scheduler save backends for high/low priority tasks separately */
    Set<InetSocketAddress> backendsHW;
    Set<InetSocketAddress> backendsLW;
    private Configuration conf;

    @Override
    public void initialize(Configuration conf) throws IOException {
        backendsHW = ConfigUtil.parseBackends(conf, PigeonConf.STATIC_NODE_MONITORS_HW);
        backendsLW = ConfigUtil.parseBackends(conf, PigeonConf.STATIC_NODE_MONITORS_LW);
        this.conf = conf;
    }

    @Override
    public boolean isHW(InetSocketAddress backendAddr) {
        return backendsHW.contains(backendAddr);
    }


    @Override
    public boolean watchApplication(String appId) {
        if (!appId.equals(conf.getString(PigeonConf.STATIC_APP_NAME))) {
            LOG.warn("Requested watch for app " + appId +
                    " but was expecting app " + conf.getString(PigeonConf.STATIC_APP_NAME));
        }
        return true;
    }

    @Override
    public Set<InetSocketAddress> getBackends(String appId, boolean isHW) {
        if (!appId.equals(conf.getString(PigeonConf.STATIC_APP_NAME))) {
            LOG.warn("Requested backends for app " + appId +
                    " but was expecting app " + conf.getString(PigeonConf.STATIC_APP_NAME));
        }

        return isHW ? backendsHW : backendsLW;
    }

    @Override
    public Set<InetSocketAddress> getBackends(boolean isHW) {
        if(isHW)
            LOG.debug("Preparing backends for hight priority workloads");
        else
            LOG.debug("Preparing backends for low priority workloads");

        return isHW ? backendsHW : backendsLW;
    }
}
