package net.johnewart.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 9/25/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public interface JobQueueMonitor {
    ImmutableMap<String, JobQueueMetrics> getSnapshots();
    ImmutableList<SystemSnapshot> getSystemSnapshots();
}
