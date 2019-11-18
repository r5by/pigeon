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

/**
 * Configuration parameters for pigeon
 *
 */

public class PigeonConf {
    // Values: "debug", "info", "warn", "error", "fatal"
    public final static String LOG_LEVEL = "log_level";

    // Listen port for the state store --> scheduler interface
    public final static String SCHEDULER_THRIFT_PORT = "scheduler.thrift.port";
    public final static String SCHEDULER_THRIFT_THREADS =
            "scheduler.thrift.threads";

    /* List of ports corresponding to node monitors (backend interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String NM_THRIFT_PORTS = "agent.thrift.ports";

    /* List of ports corresponding to node monitors (internal interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String INTERNAL_THRIFT_PORTS = "internal_agent.thrift.ports";

    public final static String MASTER_THRIFT_THREADS = "agent.thrift.threads";
    public final static String INTERNAL_THRIFT_THREADS =
            "internal_agent.thrift.threads";
    /** Type of task scheduler to use on node monitor. Values: "fifo," "round_robin, " "priority." */
    public final static String NM_TASK_SCHEDULER_TYPE = "node_monitor.task_scheduler";

    public final static String SYSTEM_MEMORY = "system.memory";
    public final static int DEFAULT_SYSTEM_MEMORY = 1024;

    public final static String SYSTEM_CPUS = "system.cpus";
    public final static int DEFAULT_SYSTEM_CPUS = 4;

    // Values: "standalone", "configbased." Only "configbased" works currently.
    public final static String DEPLYOMENT_MODE = "deployment.mode";
    public final static String DEFAULT_DEPLOYMENT_MODE = "production";


    /** The hostname of this machine. */
    public final static String HOSTNAME = "hostname";

    // Parameters for static operation.
    /** Expects a comma-separated list of host:port pairs describing the address of the
     * internal interface of the node monitors. */
    public final static String STATIC_MASTERS = "static.masters";

    public final static String STATIC_APP_NAME = "static.app.name";

    public static final String RECURSIVE_SERVICE_PORT = "recursive.port";


    public static final String TR_CUTOFF = "tr_cutoff";
    public static final double TR_CUTOFF_DEFAULT = 13581.11;
}
