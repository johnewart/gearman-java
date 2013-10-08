package net.johnewart.gearman.engine.queue.persistence;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.engine.core.QueuedJob;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class RedisPersistenceEngine implements PersistenceEngine {

    private final ObjectMapper mapper;
    private final JedisPool jedisPool;
    private final ConcurrentHashMap<String, QueuedJob> jobHandleToQueuedJobMap;
    private final String hostname;
    private final int port;

    private static Logger LOG = LoggerFactory.getLogger(RedisPersistenceEngine.class);

    public RedisPersistenceEngine(String hostname, int port)
    {
        this.hostname = hostname;
        this.port = port;
        mapper = new ObjectMapper();
        jobHandleToQueuedJobMap = new ConcurrentHashMap<>();
        LOG.debug(String.format("RedisQueue connecting on %s:%s", hostname, port));
        jedisPool = new JedisPool(new JedisPoolConfig(), this.hostname, this.port);
    }

    @Override
    public String getIdentifier() {
        return String.format("Redis - %s:%d", this.hostname, this.port);
    }

    public boolean write(Job job)
    {
        Jedis redisClient = jedisPool.getResource();
        try {
            String json = mapper.writeValueAsString(job);
            String bucket = "gm:" + job.getFunctionName();
            String key = job.getUniqueID();
            QueuedJob queuedJob = new QueuedJob(job);
            jobHandleToQueuedJobMap.put(job.getJobHandle(), queuedJob);
            redisClient.hset(bucket, key, json);
            LOG.debug("Storing in redis " + bucket + "-" + key + ": " + job.getUniqueID() + "/" + job.getJobHandle());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            jedisPool.returnResource(redisClient);
        }
    }

    public void delete(Job job)
    {
        delete(job.getFunctionName(), job.getUniqueID());
    }

    @Override
    public void delete(String functionName, String uniqueID)
    {
        Jedis redisClient = jedisPool.getResource();
        String bucket = "gm:" + functionName;

        //jobHandleToQueuedJobMap.remove(job.getJobHandle());
        LOG.debug("Removing from redis " + bucket + ": " + uniqueID);
        redisClient.hdel(bucket, uniqueID);
        jedisPool.returnResource(redisClient);
    }

    public void deleteAll()
    {
        Jedis redisClient = jedisPool.getResource();
        for(String key : redisClient.keys("gm:*"))
        {
            redisClient.del(key);
        }
        jobHandleToQueuedJobMap.clear();
        jedisPool.returnResource(redisClient);
    }

    @Override
    public Job findJob(String functionName, String uniqueID)
    {
        Jedis redisClient = jedisPool.getResource();
        Job job = null;
        try {
            String bucket = "gm:" + functionName;
            String jobJSON = redisClient.hgetAll(bucket).get(uniqueID);
            job = mapper.readValue(jobJSON, Job.class);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            jedisPool.returnResource(redisClient);
        }

        return job;
    }

    public Collection<QueuedJob> readAll()
    {
        Jedis redisClient = jedisPool.getResource();
        ArrayList<QueuedJob> jobs = new ArrayList<>();
        Job currentJob;
        for(String functionQueue : redisClient.keys("gm:*"))
        {
            Map<String, String> redisJobs = redisClient.hgetAll(functionQueue);
            for(Map.Entry<String, String> entry : redisJobs.entrySet())
            {
                final String uniqueID = entry.getKey();
                final String jobJSON = entry.getValue();

                try {
                    LOG.debug("Loading " + uniqueID + " from " + functionQueue);
                    LOG.debug("JSON: " + jobJSON);
                    currentJob = mapper.readValue(jobJSON, Job.class);
                    QueuedJob queuedJob = new QueuedJob(currentJob);
                    jobHandleToQueuedJobMap.put(currentJob.getJobHandle(), queuedJob);
                    jobs.add(queuedJob);
                } catch (IOException e) {
                    LOG.debug("Error deserializing job: " + e.toString());
                }
            }
        }
        LOG.debug("Loaded " + jobs.size() + " jobs from Redis!");
        jedisPool.returnResource(redisClient);

        return jobs;
    }

    @Override
    public Collection<QueuedJob> getAllForFunction(String functionName) {
        Jedis redisClient = jedisPool.getResource();
        ArrayList<QueuedJob> jobs = new ArrayList<>();
        Map<String, String> redisJobs = redisClient.hgetAll("gm:" + functionName);
        Job currentJob;

        for(String uniqueID : redisJobs.keySet())
        {
            try {
                String jobJSON = redisJobs.get(uniqueID);
                currentJob = mapper.readValue(jobJSON, Job.class);

                jobs.add(new QueuedJob(currentJob));
            } catch (IOException e) {
                LOG.debug("Error deserializing job: " + e.toString());
            }
        }

        jedisPool.returnResource(redisClient);

        return jobs;
    }

    public Job findJobByHandle(String jobHandle) {
        QueuedJob runnableJob = jobHandleToQueuedJobMap.get(jobHandle);
        return findJob(runnableJob.getFunctionName(), runnableJob.getUniqueID());
    }
}
