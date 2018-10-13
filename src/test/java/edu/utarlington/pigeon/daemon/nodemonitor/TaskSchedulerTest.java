package edu.utarlington.pigeon.daemon.nodemonitor;

import com.google.common.collect.Lists;
import edu.utarlington.pigeon.thrift.TEnqueueTaskReservationsRequest;
import edu.utarlington.pigeon.thrift.TFullTaskId;
import edu.utarlington.pigeon.thrift.THostPort;
import edu.utarlington.pigeon.thrift.TUserGroupInfo;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import edu.utarlington.pigeon.daemon.nodemonitor.TaskScheduler.TaskSpec;

import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.*;

public class TaskSchedulerTest {
    int requestId;

    private TEnqueueTaskReservationsRequest createTaskReservationRequest(
            int numTasks, TaskScheduler scheduler, String userId) {
        return createTaskReservationRequest(numTasks, scheduler, userId, 0);
    }

    private TEnqueueTaskReservationsRequest createTaskReservationRequest(
            int numTasks, TaskScheduler scheduler, String userId, int priority) {
        String idStr = Integer.toString(requestId++);
        TUserGroupInfo user = new TUserGroupInfo(userId, "group", priority);
        THostPort schedulerAddress = new THostPort("1.2.3.4", 52);
        return new TEnqueueTaskReservationsRequest(
                "appId", user, idStr, schedulerAddress, numTasks);
    }
    @Before
    public void setUp() throws Exception {
        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();
        requestId = 1;
    }

    /**
     * Tests the fifo task scheduler.
     */
    @Test
    public void testFifo() {
        TaskScheduler scheduler = new FifoTaskScheduler(4);
        scheduler.initialize(new PropertiesConfiguration(), 12345);

        final String testApp = "test app";
        final InetSocketAddress backendAddress = new InetSocketAddress("123.4.5.6", 2);

        // Make sure that tasks are launched right away, if resources are available.
        scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, testApp),
                backendAddress);
        assertEquals(1, scheduler.runnableTasks());
        TaskSpec task = scheduler.getNextTask();
        assertEquals("1", task.requestId);
        assertEquals(0, scheduler.runnableTasks());

        scheduler.submitTaskReservations(createTaskReservationRequest(2, scheduler, testApp),
                backendAddress);
        assertEquals(2, scheduler.runnableTasks());

        // Make sure the request to schedule 3 tasks is appropriately split, with one task running
        // now and others started later.
        scheduler.submitTaskReservations(createTaskReservationRequest(3, scheduler, testApp),
                backendAddress);
        /* 4 tasks have been launched but one was already removed from the runnable queue using
         * getTask(), leaving 3 runnable tasks. */
        assertEquals(3, scheduler.runnableTasks());
        task = scheduler.getNextTask();
        assertEquals("2", task.requestId);
        task = scheduler.getNextTask();
        assertEquals("2", task.requestId);
        /* Make a list of task ids to use in every call to tasksFinished, and just update the request
         * id for each call. */
        TFullTaskId fullTaskId = new TFullTaskId();
        fullTaskId.taskId = "";
        List<TFullTaskId> completedTasks = Lists.newArrayList();
        completedTasks.add(fullTaskId);

        // Have a few tasks complete before the last runnable task is removed from the queue.
        fullTaskId.requestId = "2";
        scheduler.tasksFinished(completedTasks);
        scheduler.tasksFinished(completedTasks);
        fullTaskId.requestId = "1";
        scheduler.tasksFinished(completedTasks);

        task = scheduler.getNextTask();
        assertEquals("3", task.requestId);
        task = scheduler.getNextTask();
        assertEquals("3", task.requestId);
        task = scheduler.getNextTask();
        assertEquals("3", task.requestId);
        assertEquals(0, scheduler.runnableTasks());
    }
}