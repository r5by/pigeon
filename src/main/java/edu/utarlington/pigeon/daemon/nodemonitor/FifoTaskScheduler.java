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

import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and uses a FIFO queue to determine the order to launch
 * tasks whenever outstanding tasks exceed this amount.
 */
public class FifoTaskScheduler extends TaskScheduler{
    private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);

    public int maxActiveTasks;
    public Integer activeTasks;
    public LinkedBlockingQueue<TaskSpec> taskReservations =
            new LinkedBlockingQueue<TaskSpec>();

    public FifoTaskScheduler(int max) {
        maxActiveTasks = max;
        activeTasks = 0;
    }

    //=======================================
    // Overrides
    //=======================================
    @Override
    int getMaxActiveTasks() {
        return maxActiveTasks;
    }

    @Override
    protected void handleNoTaskForReservation(TaskSpec taskSpec) {
        attemptTaskLaunch(taskSpec.previousRequestId, taskSpec.previousTaskId);
    }

    /**
     * Attempts to launch a new task.
     *
     * The parameters {@code lastExecutedRequestId} and {@code lastExecutedTaskId} are used purely
     * for logging purposes, to determine how long the node monitor spends trying to find a new
     * task to execute. This method needs to be synchronized to prevent a race condition with
     * {@link handleSubmitTaskReservation}.
     */
    private synchronized void attemptTaskLaunch(
            String lastExecutedRequestId, String lastExecutedTaskId) {
        TaskSpec reservation = taskReservations.poll();
        if (reservation != null) {
            reservation.previousRequestId = lastExecutedRequestId;
            reservation.previousTaskId = lastExecutedTaskId;
            makeTaskRunnable(reservation);
        } else {
            activeTasks -= 1;
        }
    }
}
