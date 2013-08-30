package net.johnewart.gearman.cluster;

import akka.actor.ActorSystem;


public class GearmanClusterServer {
        public static void main(String[] args) {
        // Override the configuration of the port
        // when specified as program argument
        if (args.length > 0)
            System.setProperty("akka.remote.netty.tcp.port", args[0]);

        // Create an Akka system
        ActorSystem system = ActorSystem.create("ClusterSystem");


        // Create an actor that handles cluster domain events
        /*ActorRef clusterListener = system.actorOf(
                Props.create(GearmanClusterListener.class, "id"),
                "clusterListener"
        );

        // Add subscription of cluster events
        Cluster.get(system).subscribe(clusterListener,
                ClusterDomainEvent.class);
        */
    }
}
