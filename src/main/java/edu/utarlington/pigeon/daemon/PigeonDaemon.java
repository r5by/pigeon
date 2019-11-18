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

package edu.utarlington.pigeon.daemon;

import edu.utarlington.pigeon.daemon.master.PigeonMasterThrift;
import edu.utarlington.pigeon.daemon.scheduler.SchedulerThrift;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A Pigeon Daemon includes both a scheduler and master, it needs to be launched on every scheduler/master node.
 */
public class PigeonDaemon {
    // Eventually, we'll want to change this to something higher than debug.
    public final static Level DEFAULT_LOG_LEVEL = Level.DEBUG;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("c", "configuration file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("help", "print help statement");
        OptionSet options = parser.parse(args);

        if (options.has("help") || !options.has("c")) {
            parser.printHelpOn(System.out);
            System.exit(-1);
        }

        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();

        //TODO: Add pigeon logging support

        String configFile = (String) options.valueOf("c");
        Configuration conf = new PropertiesConfiguration(configFile);
        PigeonDaemon daemon = new PigeonDaemon();
        daemon.initialize(conf);
    }

    private void initialize(Configuration conf) throws Exception{
        Level logLevel = Level.toLevel(conf.getString(PigeonConf.LOG_LEVEL, ""),
                DEFAULT_LOG_LEVEL);
        Logger.getRootLogger().setLevel(logLevel);

        // Start as many node monitors as specified in config
        String[] nmPorts = conf.getStringArray(PigeonConf.NM_THRIFT_PORTS);
        String[] inPorts = conf.getStringArray(PigeonConf.INTERNAL_THRIFT_PORTS);

        if (nmPorts.length != inPorts.length) {
            throw new ConfigurationException(PigeonConf.NM_THRIFT_PORTS + " and " +
                    PigeonConf.INTERNAL_THRIFT_PORTS + " not of equal length");
        }
        if (nmPorts.length > 1 &&
                (!conf.getString(PigeonConf.DEPLYOMENT_MODE, "").equals("standalone"))) {
            throw new ConfigurationException("Mutliple NodeMonitors only allowed " +
                    "in standalone deployment");
        }
        //Setup master & internal services
        if (nmPorts.length == 0) {
            (new PigeonMasterThrift()).initialize(conf,
                    PigeonMasterThrift.DEFAULT_MASTER_THRIFT_PORT,
                    PigeonMasterThrift.DEFAULT_INTERNAL_THRIFT_PORT);
        }


        // Setup scheduler & recursive services
        SchedulerThrift scheduler = new SchedulerThrift();
        scheduler.initialize(conf);
    }
}
