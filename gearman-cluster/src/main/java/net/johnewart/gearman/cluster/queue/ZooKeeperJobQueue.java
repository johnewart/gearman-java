package net.johnewart.gearman.cluster.queue;

import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.persistence.PersistenceEngine;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class ZooKeeperJobQueue implements JobQueue, Watcher {
    private final Logger LOG = LoggerFactory.getLogger(ZooKeeperJobQueue.class);

    private final PersistenceEngine persistenceEngine;
    private final String name;
    private final ZooKeeper zooKeeper;
    private final String QUEUE_ROOT = "/queues";
    private final String lockPath;
    private final String queuePath;
    private final String highPath;
    private final String midPath;
    private final String lowPath;
    private final String uniqueIdPath;

    private final ConcurrentHashMap<Job, String> jobPathMap;

    public ZooKeeperJobQueue(final String name,
                             final PersistenceEngine persistenceEngine) throws IOException, KeeperException, InterruptedException {
        this.name = name;
        this.persistenceEngine = persistenceEngine;
        this.zooKeeper = new ZooKeeper("localhost:2181", 3000, this);
        this.queuePath = format("%s/%s", QUEUE_ROOT, name);
        this.highPath = format("%s/high", queuePath);
        this.midPath = format("%s/mid", queuePath);
        this.lowPath = format("%s/low", queuePath);
        this.lockPath = format("%s/locks", queuePath);
        this.uniqueIdPath = format("%s/uids", queuePath);
        this.jobPathMap = new ConcurrentHashMap<>();

        this.setupPaths();
    }

    @Override
    public final boolean enqueue(final Job job) {
        LOG.debug("Enqueueing " + job.toString());

        if(persistenceEngine.write(job)) {
            try {
                byte[] uniqueIdBytes = Bytes.toBytes(job.getUniqueID());
                final String jobPath = pathForJob(job);
                final String createdPath =
                        zooKeeper.create(jobPath, uniqueIdBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                final String uniqueIdPath =
                        zooKeeper.create(uidPath(job.getUniqueID()), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOG.debug("Added job as " + createdPath);
                return true;
            } catch (InterruptedException e) {
                LOG.error("Interrupted while storing: ", e);
                return false;
            } catch (KeeperException e) {
                LOG.error("Keeper error: ", e);
                return false;
            }

        } else {
            // ! written to persistent store
            LOG.error("Unable to save job to persistent store");
            return false;
        }

    }

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low
     * Caveat: in the normal queue, we skip over any jobs whose timestamp has not
     * come yet (support for epoch jobs)
     *
     * Removes the job from the queue.
     *
     * @return Next Job in the queue, null if none
     */
    @Override
    public final Job poll() {

        long currentTime = new DateTime().toDate().getTime() / 1000;
        String[] searchPaths = { highPath, midPath, lowPath };

        try {
            for(String searchPath : searchPaths) {

                final List<JobKey> jobKeys = getJobKeys(searchPath);

                for (JobKey jobKey : jobKeys) {
                    LOG.debug("CHILD: " + jobKey.uniqueId);
                    if(jobKey.timeToRun <= currentTime && !isLocked(jobKey.uniqueId)) {
                        Job job = findJobByUniqueId(jobKey.uniqueId);
                        if (job != null) {
                            lock(jobKey.uniqueId);
                            LOG.debug("Job: "  + job.getJobHandle());
                            String fullPath = format("%s/%s", searchPath, jobKey.pathKey);
                            jobPathMap.put(job, fullPath);
                            return job;
                        }
                    } else {
                       LOG.debug("Job " + jobKey.uniqueId + " was locked.");
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted: ", e);
        } catch (KeeperException e) {
            LOG.error("ZooKeeper error: ", e);
        }

        return null;
    }

    private List<JobKey> getJobKeys(final String searchPath) {
        List<JobKey> jobKeys = new ArrayList<>();

        try {
            List<String> jobKeyStrings = zooKeeper.getChildren(searchPath, false);
            jobKeys = new ArrayList<>(jobKeyStrings.size());

            for (String jobKeyString : jobKeyStrings ) {
                try {
                    jobKeys.add(new JobKey(jobKeyString));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            Collections.sort(jobKeys, new ZooKeeperJobKeyComparator());
        }  catch (InterruptedException e) {
            LOG.error("Interrupted: ", e);
        } catch (KeeperException e) {
            LOG.error("ZooKeeper error: ", e);
        }

        return jobKeys;
    }


    /**
     * Returns the total number of jobs in this queue
     * @return
     * 		The total number of jobs in all priorities
     */
    @Override
    public final long size() {
        return 0L;
    }

    @Override
    public long size(final JobPriority jobPriority) {
        return 0L;
    }

    @Override
    public final boolean uniqueIdInUse(final String uniqueID) {
        try {
            return zooKeeper.exists(uidPath(uniqueID), null) != null;
        } catch (KeeperException e) {
            LOG.error("ZooKeeper exception: ", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted exception: ", e);
        }

        // If an error occurred, assume that it's in use to avoid collision
        // TODO: This might be a bad idea....
        return true;
    }

    @Override
    public final boolean isEmpty() {
        return false;
    }

    @Override
    public void setMaxSize(final int size) {
        // NOOP. This is irrelevant in this queue type.
    }

    @Override
    public final String getName() {
        return this.name;
    }

    @Override
    public boolean remove(final Job job) {
        if (job == null) {
            return false;
        }

        try {
            persistenceEngine.delete(job);
            unLock(job.getUniqueID());
            zooKeeper.delete(jobPathMap.get(job), -1);
            zooKeeper.delete(uidPath(job.getUniqueID()), -1);
            return true;
        } catch (InterruptedException e) {
            LOG.error("Interrupted: ", e);
        } catch (KeeperException e) {
            LOG.error("Keeper error: ", e);
        }

        return false;
    }

    @Override
    public String metricName() {
        return this.name.replaceAll(":", ".");
    }

    @Override
    public Collection<QueuedJob> getAllJobs() {
        return new HashSet<>();
    }

    @Override
    public Job findJobByUniqueId(String uniqueID) {
        return persistenceEngine.findJob(this.name, uniqueID);
    }

    @Override
    public ImmutableMap<Integer, Long> futureCounts() {
        return ImmutableMap.of();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
    }

    private String uidPath(final String uniqueId) {
        return format("%s/%s", uniqueIdPath, uniqueId);
    }

    private String lockPath(final String uniqueId) {
        return format("%s/%s", lockPath, uniqueId);
    }

    private boolean isLocked(final String uniqueId) throws KeeperException, InterruptedException {
        return zooKeeper.exists(lockPath(uniqueId), false) != null;
    }

    private void lock(final String uniqueId) throws KeeperException, InterruptedException {
        zooKeeper.create(lockPath(uniqueId), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private void unLock(final String uniqueId) throws KeeperException, InterruptedException {
        zooKeeper.delete(lockPath(uniqueId), -1);
    }

    private void setupPaths() throws KeeperException, InterruptedException {
        // create paths
        createPathIfDoesNotExist(QUEUE_ROOT);
        createPathIfDoesNotExist(queuePath);
        createPathIfDoesNotExist(highPath);
        createPathIfDoesNotExist(midPath);
        createPathIfDoesNotExist(lowPath);
        createPathIfDoesNotExist(lockPath);
        createPathIfDoesNotExist(uniqueIdPath);
    }

    private void createPathIfDoesNotExist(final String path) throws KeeperException, InterruptedException {
        if(zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private String pathForJob(final Job job) {
        final String parentPath;

        switch(job.getPriority()) {
            case LOW:
                parentPath = lowPath;
                break;
            case NORMAL:
                parentPath = midPath;
                break;
            case HIGH:
                parentPath = highPath;
                break;
            default:
                parentPath = null;

        }

        if(parentPath != null) {
            return format("%s/%s::%d::", parentPath, job.getUniqueID(), job.getTimeToRun());
        } else {
            return null;
        }
    }

    static class JobKey {
        final public String uniqueId;
        final public long timeToRun;
        final public long queueOrder;
        final public String pathKey;

        public JobKey(final String jobKey) throws Exception {
            pathKey = jobKey;
            String[] parts = jobKey.split("::");
            if(parts.length == 3) {
                uniqueId = parts[0];
                timeToRun = Long.parseLong(parts[1]);
                queueOrder = Long.parseLong(parts[2]);
            } else {
                throw new Exception("Too few arguments");
            }
        }
    }

    static class ZooKeeperJobKeyComparator implements Comparator<JobKey> {
        @Override
        public int compare(JobKey left, JobKey right) {

            return left.queueOrder < right.queueOrder ? -1
                    : left.queueOrder > right.queueOrder ? 1
                    : 0;
        }
    }
}