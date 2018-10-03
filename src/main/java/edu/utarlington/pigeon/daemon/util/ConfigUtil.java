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

import com.google.common.base.Optional;
import edu.utarlington.pigeon.daemon.PigeonConf;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import edu.utarlington.pigeon.thrift.TResourceVector;


/**
 * Utilities to aid the configuration file-based scheduler and node monitor.
 */
public class ConfigUtil {
    private final static Logger LOG = Logger.getLogger(ConfigUtil.class);

    /**
     * Parses the list of backends from a {@link Configuration}.
     *
     * Returns a map of address of backends to a {@link TResourceVector} describing the
     * total resource capacity for that backend.
     */
    public static Set<InetSocketAddress> parseBackends(
            Configuration conf) {
        if (!conf.containsKey(PigeonConf.STATIC_NODE_MONITORS)) {
            throw new RuntimeException("Missing configuration node monitor list");
        }

        Set<InetSocketAddress> backends = new HashSet<InetSocketAddress>();

        for (String node: conf.getStringArray(PigeonConf.STATIC_NODE_MONITORS)) {
            Optional<InetSocketAddress> addr = Serialization.strToSocket(node);
            if (!addr.isPresent()) {
                LOG.warn("Bad backend address: " + node);
                continue;
            }
            backends.add(addr.get());
        }

        return backends;
    }
}
