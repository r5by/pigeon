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

import edu.utarlington.pigeon.thrift.*;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class extends the thrift sparrow scheduler interface. It wraps the
 * {@link Scheduler} class and delegates most calls to that class.
 */
//TODO: Complete this class
public class SchedulerThrift implements SchedulerService.Iface, GetTaskService.Iface {
    // Defaults if not specified by configuration
    public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
    private final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 8;
    public final static int DEFAULT_GET_TASK_PORT = 20507;

    //=======================================
    // Scheduler Services
    //=======================================
    @Override
    public boolean registerFrontend(String app, String socketAddress) throws TException {
        return false;
    }

    @Override
    public void submitJob(TSchedulingRequest req) throws IncompleteRequestException, TException {

    }

    @Override
    public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) throws TException {

    }

    //=======================================
    // GetTask Services
    //=======================================
    @Override
    public List<TTaskLaunchSpec> getTask(String requestId, THostPort nodeMonitorAddress) throws TException {
        return null;
    }
}
