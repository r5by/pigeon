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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.scheduler.SchedulerThrift;
import edu.utarlington.pigeon.daemon.master.TaskScheduler.TaskSpec;
import edu.utarlington.pigeon.daemon.util.Network;
import edu.utarlington.pigeon.daemon.util.Resources;
import edu.utarlington.pigeon.daemon.util.TClients;
import edu.utarlington.pigeon.daemon.util.ThriftClientPool;
import edu.utarlington.pigeon.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * TaskLauncher service consumes TaskReservations produced by
 * For each TaskReservation, the TaskLauncherService attempts to fetch the task specification from
 * the scheduler that sent the reservation using the {@code getTask} RPC; if it successfully
 * fetches a task, it launches the task on the appropriate backend.
 *
 * TaskLauncherService uses multiple threads to launch tasks. Each thread keeps a client for
 * each scheduler, and a client for each application backend, and the TaskLauncherService uses
 * a number of threads equal to the number of slots available for running tasks on the machine.
 */
//TODO: Logging
public class TaskLauncherService {
    private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);

    /* The number of threads used by the service. */
    private int numThreads;

    /** The master node who's loading the task launcher service*/
    private THostPort master;

    private TaskScheduler scheduler;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {

        /** Client to use to communicate with each scheduler (indexed by scheduler hostname). */
        private HashMap<String, RecursiveService.AsyncClient> schedulerClients = Maps.newHashMap();

        /** Client to use to communicate with each application backend (indexed by backend address). */
        private HashMap<InetSocketAddress, BackendService.Client> backendClients = Maps.newHashMap();

        @Override
        public void run() {
            while (true) {
                TaskSpec task = scheduler.getNextTask(); // blocks until task is ready
                if (task.taskSpec == null) {
                    LOG.error("Unexpected empty task obtained!");
                }

                //Launch Task
                executeLaunchTaskRpc(task);
            }

        }

        /** Uses a tasksFinished() RPC to inform the appropriate scheduler tasks finished, triggered by a dummy TaskSpec created by TaskScheduler
         * upon receiving handleNoTasksReservations() request from master node */
        private void executeRecursiveCall(TaskSpec task) {
        }

        /**
         * Used for Pigeon task launch request wrapper
         */

        /** Executes an RPC to launch a task on an application backend. */
        private void executeLaunchTaskRpc(TaskSpec task) {
            if (!backendClients.containsKey(task.appBackendAddress)) {
                try {
                    backendClients.put(task.appBackendAddress,
                            TClients.createBlockingBackendClient(task.appBackendAddress));
                } catch (IOException e) {
                    LOG.error("Error creating thrift client: " + e.getMessage());
                    return;
                }
            }
            BackendService.Client backendClient = backendClients.get(task.appBackendAddress);
            THostPort schedulerHostPort = Network.socketAddressToThrift(task.schedulerAddress);
            //The isH property is irrelevant at the backend(worker) for Pigeon, so simply pass the default value here.
            TFullTaskId taskId = new TFullTaskId(task.taskSpec.getTaskId(), task.requestId,
                    task.appId, schedulerHostPort, false);
            try {
                backendClient.launchTask(task.taskSpec.bufferForMessage(), taskId, task.user);
            } catch (TException e) {
                LOG.error("Unable to launch task on backend " + task.appBackendAddress + ":" + e);
            }
        }
    }

    public void initialize(Configuration conf, TaskScheduler scheduler) {
        this.scheduler = scheduler;
        numThreads = scheduler.getMaxActiveTasks();
        if (numThreads <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            numThreads = Resources.getSystemCPUCount(conf);
        }

        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            service.submit(new TaskLaunchRunnable());
        }
    }
}
