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

import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.master.PigeonMasterState;
import edu.utarlington.pigeon.daemon.util.ConfigUtil;
import edu.utarlington.pigeon.thrift.ServerNotReadyException;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

/***
 * A {@link PigeonMasterState} implementation based on a static config file.
 */

public class ConfigMasterState implements PigeonMasterState{
    private static final Logger LOG = Logger.getLogger(ConfigMasterState.class);

    private String staticAppId;
    private boolean upflag;

    @Override
    public void initialize(Configuration conf) throws IOException {
        staticAppId = conf.getString(PigeonConf.STATIC_APP_NAME);
        upflag = false;
    }

    //TODO: verify the backend matches with the configured information here
    @Override
    public boolean registerBackend(String appId, InetSocketAddress master, InetSocketAddress backend, int type) {
        // todo: Verify that the given backend information matches the static configuration.
        if (!appId.equals(staticAppId)) {
            LOG.error("Requested to register backend for app " + appId +
                    " but was expecting app " + staticAppId);
        }
        return true;
    }

    @Override
    public boolean masterNodeUp() {
        boolean flag = upflag;
        if(!upflag) {
            upflag = true;
        }
        return flag;
    }
}
