phi-cluster-core
----------------
under development...

##### notes:

- to add gearman into your local repository

mvn install:install-file -DgroupId=gearman -DartifactId=java-gearman-service -Dversion=0.6.5 -Dpackaging=jar -Dfile=lib/java-gearman-service-0.6.5.jar

----

Phi-Cluster: Fault-tolerant Task Execution Cluster
==================================================

The goal of phi-cluster is to provide a simple, extensible framework to implement fault-tolerant, decentralized task execution engines for unreliable clustered machines. It has three main elements: 

1. Paxos consensus based distributed task pool and state management.
2. Decentralized failure detector based on The Phi Accrual Failure Detector technique.
3. Simple, pluggable task definition, and task executor.

All or a subset of the nodes in phi-cluster run Zookeeper service that phi-cluster uses to keep task data/state and node heartbeats for failure detector, which is based on The Phi-Accrual Failure Detector where each node decides whether the nodes of interest failed based on a probabilistic measure accrued over time. In phi-cluster, it is currently a simple configurable heartbeat-miss threshold.

Phi-cluster admits tasks into the cluster and puts them into distributed task pool to make them available for cluster nodes to pick. Cluster nodes attempt to pick an available task in the task pool's queue, and only one of them succeeds. Synchronization is provided through Zookeeper service, which is a majority based consensus system based on Paxos algorithm. 

Part of taking a task from task pool, the node that is going execute the task designates a number of other nodes that would attempt to reclaim the task if the node failed before or during the execution of the task. This is a similar idea to replication but here task is not replicated, rather, its id is put into a secondary task queue of some other nodes.

![ScreenShot](http://denizdemir.files.wordpress.com/2013/01/phi-cluster-task-replication.png)

Each node monitors its secondary tasks queue, and uses accrual failure detector technique to make a failure decision for the nodes for which it has tasks in its secondary queue. If it reaches a failure decision for any such node, it goes over its secondary queue and tries to reclaim the tasks that are owned by the failed node. Note that it does not try to take the ownership of the tasks, rather, it tries to remove the existing ownership of the tasks to make them available in the distributed task pool so that they can be picked by any node of the cluster. This is why we call it "reclaiming" the tasks.

Whether the reclaimed tasks are re-executed or resumed on another node is up to the implementer of the tasks. Phi-cluster provides facility to keep task states across the cluster, allowing tasks to update its state machines so that they can be resumed from the last state. The extent a task can be reclaimed, re-executed or resumed is up to its implementation. Phi-cluster provides necessary primitives and runtime environment to make that possible.

Phi-cluster is a decentralized cluster, and there is no leader node. However, phi-cluster does not have split-brain issue since it uses a consensus service to keep the cluster state, and it is always the majority of nodes that will win in case of split-brain --nodes in the minority will not be able to function while the ones in the majority will consider them failed. Note that we mean phi-cluster does not explicitly have leader selection but Zookeeper that phi-cluster relies on does have leader node and leader selection. 

Phi-cluster has an extensible task definition and execution mechanism. It comes with a simple executor and task definition interface based on Java's executor service and Runnable interface. We also provide a Gearman based executor just to illustrate the ability to use a completely different executor, which runs outside of the JVM. However, we do not plan to focus on Gearman beyond proof of concept.

Phi-cluster focuses on fault-tolerance, and tries to provide it with a simple framework. Its use of cluster wide consensus service has implications on its scalability. We think Phi-cluster is more suited for applications with relatively long-running tasks. It is important to keep the state updates of tasks and inter-task dependencies at minimum.

 
