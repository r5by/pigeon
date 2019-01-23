# Pigeon Scheduler (BETA)

[Pigeon]() is a distributed, hierarchical job scheduler for datacenters, aiming at addressing the above issues. In Pigeon, workers are divided into groups. Each group has a master which manages all the tasks coming from distributed job schedulers and then distributes them to the workers in the group. Pigeon abandons the probe-and-late-task-binding process, underlying most existing distributed and hybrid schedulers. Upon receiving a job, a distributed scheduler in Pigeon immediately and evenly distributes all the tasks of the job to the masters, making it possible for Pigeon to deal with tiny jobs. In Pigeon, the job heterogeneity is well taken care of by masters through priority queuing and worker reservation for tasks belonging to short jobs at the task level, oblivious of job context and without having to introduce a centralized scheduler to handle long jobs, as is the case for the existing hybrid schedulers.

## Installation

Pigeon requires the following software packages for development:
* JDK 6+
* [Maven](https://maven.apache.org/)

## Building Pigeon

Build Pigeon with the following commands:
```sh
$ mvn compile
$ mvn package -Dmaven.test.skip=true
```

## Quick Start

After building, copy and paste `pigeon-1.0-SNAPSHOT.jar` under your porject `./target` path to all of your Pigeon scheduler and master hosts. Prepare your pigeon configuration file on each host machine according to the following example:
```diff
## Pigeon Configuraiton
# Deployment mode (configuration-based is currently supported)
deployment.mode = configbased

# pigeon masters, comma seperated
static.masters = <hostname_of_the_pigeon_master>:20502

# system cores
system.memory = 512
system.cpus = 1

# Name of the application using Sparrow. Need to be consistent with the scheduling requests submitted.
static.app.name = sleepApp
    
# hostname of this machine
hostname = <hostname_of_this_machine>
    
# Cut-off (in millisecond); required by the proto example
tr_cutoff = 10000.00
```

Launch Pigeon daemon on each scheduler or master node by:
```sh
java -XX:+UseConcMarkSweepGC -cp <path_to>/pigeon-1.0-SNAPSHOT.jar edu.utarlington.pigeon.daemon.PigeonDaemon -c <path_to_configuration_file>
```

A Pigeon worker should always be started after its master, and has its own configurations:

```diff
# its master socket
master.socket = <hostname_of_its_master>:20501

# worker type (integer): currently supported are 0 ==> low priority worker || 1 ==> high priority worker
worker.type = 0

# hostname of this machine
hostname = <hostname_of_this_machine>
```

With the above configuration, a worker can be registered with its master:
```sh
java -XX:+UseConcMarkSweepGC -cp <path_to>/pigeon-1.0-SNAPSHOT.jar edu.utarlington.pigeon.examples.SimpleBackend -c <path_to_backend_configuration_file>
```

Refer to `SimpleFrontend.conf` or `ProtoFrontend.conf` for frontend examples then start the frontend process by:
```sh
java -XX:+UseConcMarkSweepGC -cp <path_to>/pigeon-1.0-SNAPSHOT.jar edu.utarlington.pigeon.examples.SimpleFrontend -c <path_to_frontend_configuration_file>
```

## Pigeon Spark Plugin

Please refer to [here](https://github.com/ruby-/spark-parent_2.11.git) for our Pigeon Spark Plugin. A simple example can be found to guide you through integrating your own use case with Pigeon over [Spark](https://spark.apache.org/) platform.

## Research

Pigeon is a research project within the [UT Arlington Systems Research Group](CSESYS@LISTSERV.UTA.EDU). The Pigeon team consists of Huiyang Li, Zhijun Wang, Hao Che and Zhongwei Li, please contact us for details about Pigeon Scheduler via the following email addresses.

## Contact
- Zhijun Wang <zhijun.wang@uta.edu>
- Huiyang Li <huiyang.li@mavs.uta.edu>
- Zhongwei Li <zhongwei.li@mavs.uta.edu>
- Hao Che <hche@uta.edu>