package org.gearman.cluster;

import akka.actor.UntypedActor;
import akka.cluster.ClusterEvent.ClusterDomainEvent;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.LeaderChanged;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.concurrent.atomic.AtomicInteger;

public class GearmanClusterListener extends UntypedActor {
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    final AtomicInteger availableNodeCount;

    public GearmanClusterListener(String identifier)
    {
        availableNodeCount = new AtomicInteger(0);
    }



    @Override
    public void onReceive(Object message) {
        if (message instanceof CurrentClusterState) {
            CurrentClusterState state = (CurrentClusterState) message;
            log.info("Current members: {}", state.members());

        } else if (message instanceof MemberUp) {
            MemberUp mUp = (MemberUp) message;
            log.info("Member is Up: {}", mUp.member());
            availableNodeCount.incrementAndGet();
        } else if (message instanceof UnreachableMember) {
            UnreachableMember mUnreachable = (UnreachableMember) message;
            log.info("Member detected as unreachable: {}", mUnreachable.member());
        } else if (message instanceof MemberRemoved) {
            MemberRemoved mRemoved = (MemberRemoved) message;
            log.info("Member is Removed: {}", mRemoved.member());
            availableNodeCount.decrementAndGet();
        } else if (message instanceof LeaderChanged) {
            LeaderChanged lChanged = (LeaderChanged) message;
            log.info("Leader changed to: {}", lChanged.leader());
        } else if (message instanceof ClusterDomainEvent) {
            // ignore
            //log.info("Message: {}", message.toString());
        } else {
            unhandled(message);
        }

    }


}
