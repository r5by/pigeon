# PIGEON
# Copyright 2018 Univeristy of Texas at Arlington
#
# Modified from Sparrow - University of California, Berkeley
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include 'types.thrift'

namespace java edu.utarlington.pigeon.thrift

# A service used by application backends to coordinate with Pigeon.
//service NodeMonitorService {
//  # Register this machine as a backend for the given application.
//  bool registerBackend(1: string app, 2: string listenSocket);
//
//  # Inform the NodeMonitor that a particular task has finished
//  void tasksFinished(1: list<types.TFullTaskId> tasks);
//
//  # See SchedulerService.sendFrontendMessage
//  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId,
//                           3: i32 status, 4: binary message);
//}

# A service that frontends are expected to extend. Handles communication from
# a Scheduler.
service FrontendService {
  # See SchedulerService.sendFrontendMessage
  void frontendMessage(1: types.TFullTaskId taskId, 2: i32 status,
                       3: binary message);
}

service SchedulerStateStoreService {
  # Message from the state store giving the scheduler new information
  void updateNodeState(1: map<string, types.TNodeState> snapshot);
}

service StateStoreService {
  # Register a scheduler with the given socket address (IP: Port)
  void registerScheduler(1: string schedulerAddress);

  # Register a node monitor with the given socket address (IP: Port)
  void registerNodeMonitor(1: string nodeMonitorAddress);
}

# Service to use for debugging network latencies.
service PongService {
  string ping(1: string data);
}

#####################################################################
#                   Pigeon Services                                 #
#####################################################################

# SchedulerService is used by application frontends to communicate with Pigeon
# and place jobs. (Service port by default: 20503)
service SchedulerService {
  # Register a frontend for the given application.
  bool registerFrontend(1: string app, 2: string socketAddress);

  # Submit a job composed of a list of individual tasks.
  void submitJob(1: types.TSchedulingRequest req) throws (1: types.IncompleteRequestException e);

  # Send a message to be delivered to the frontend for {app} pertaining
  # to the task {taskId}. The {status} field allows for application-specific
  # status enumerations. Right now this is used only for Spark, which relies on
  # the scheduler to send task completion messages to frontends.
  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId,
                           3: i32 status, 4: binary message);
}

# The InternalService is used by Pigeon scheduler to communicate with master node (port by default: 20502)
service InternalService {
  # Pigeon don't need reservations, only need to send task launch request to a randomly selected master
  bool launchTasksRequest(1: types.TLaunchTasksRequest launchTaskRequest) throws (1: types.ServerNotReadyException e);
}

# A service used by application backends to coordinate with its master node (service port by default: 20501)
service MasterService {
  # Register this machine as a backend for the given application.
  # type indicates whether it's used as a high priority (1) or low priority (0) worker
  # TODO: Register the node with master node based on its locality (e.g. same rack with the traget master node)
  bool registerBackend(1: string app, 2: string listenSocket, 3: i32 type);

  # Inform the Master node that a particular task has finished
  void taskFinished(1: list<types.TFullTaskId> tasks, 2:types.THostPort worker);

  # See SchedulerService.sendFrontendMessage
  # todo: No use for now, maybe we should consider to remove this part for Pigeon?
  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId,
                           3: i32 status, 4: binary message);
}

# The recursive way of master node notifying task completed meg to scheduler (service port by default: 20507)
service RecursiveService {

  # The delegated master node will hold untill all tasks related to the same request completed to notify the scheduler
   void tasksFinished(1: string requestID, 2:types.THostPort master);
}

# A service that backends are expected to extend. Handles communication from a master.
service BackendService {
  void launchTask(1: binary message, 2: types.TFullTaskId taskId,
                  3: types.TUserGroupInfo user);
}