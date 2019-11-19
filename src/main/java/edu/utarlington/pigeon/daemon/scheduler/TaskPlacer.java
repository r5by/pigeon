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

import edu.utarlington.pigeon.thrift.THostPort;
import edu.utarlington.pigeon.thrift.TSchedulingRequest;
import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/***
 * Task Placer for the Pigeon {@link Scheduler} Records task's information of one request
 */
public interface TaskPlacer {
    /** Returns true if all of the job's tasks have been placed. */
    public boolean allTasksPlaced();

    /** Returns the node monitors with outstanding reservations for this request.
     *
     * After this method is called once, the TaskPlacer assumes all those node monitors were sent
     * cancellation messages, so it will not return any node monitors in the future. */

    /**
     * Enqueue task request */
    void processRequest(TSchedulingRequest request, THostPort address) throws Exception;

    /** Records of completed/total tasks of the request */
    public int completedTasks();
    public int totalTasks();
    public int count(THostPort addr);

    /* Get the execution duration for the request from its taskPlacer*/
    public long getExecDurationMillis(long finishTime);

    /* Get the priority of this placer */
    public boolean getPriority();
}
