package org.gearman.server.persistence;

import org.codehaus.jackson.map.ObjectMapper;
import org.gearman.server.Job;
import org.gearman.server.core.RunnableJob;
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


public class RedisQueue implements PersistenceEngine {

    private final ObjectMapper mapper;
    private final JedisPool jedisPool;
    private final ConcurrentHashMap<String, RunnableJob> jobHandleToUniqueIdMap;
    private final String hostname;
    private final int port;

    private static Logger LOG = LoggerFactory.getLogger(RedisQueue.class);

    public RedisQueue(String hostname, int port)
    {
        this.hostname = hostname;
        this.port = port;
        mapper = new ObjectMapper();
        jobHandleToUniqueIdMap = new ConcurrentHashMap<>();
        LOG.debug(String.format("RedisQueue connecting on %s:%s", hostname, port));
        jedisPool = new JedisPool(new JedisPoolConfig(), this.hostname, this.port);
    }

    public void write(Job job)
    {
        Jedis redisClient = jedisPool.getResource();
        try {
            String json = mapper.writeValueAsString(job);
            String bucket = "gm:" + job.getFunctionName();
            String key = job.getUniqueID();
            jobHandleToUniqueIdMap.put(job.getJobHandle(), job.getRunnableJob());
            redisClient.hset(bucket, key, json);
            LOG.debug("Storing in redis " + bucket + "-" + key + ": " + job.getUniqueID() + "/" + job.getJobHandle());
        } catch (IOException e) {
            e.printStackTrace();
        }
        jedisPool.returnResource(redisClient);
    }

    public void delete(Job job)
    {
        Jedis redisClient = jedisPool.getResource();
        String bucket = "gm:" + job.getFunctionName();
        String key = job.getUniqueID();
        jobHandleToUniqueIdMap.remove(job.getJobHandle());
        LOG.debug("Removing from redis " + bucket + ": " + key);
        redisClient.hdel(bucket, key);
        jedisPool.returnResource(redisClient);
    }

    public void deleteAll()
    {
        Jedis redisClient = jedisPool.getResource();
        for(String key : redisClient.keys("gm:*"))
        {
            redisClient.del(key);
        }
        jobHandleToUniqueIdMap.clear();
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
        }
        jedisPool.returnResource(redisClient);

        return job;
    }

    public Collection<RunnableJob> readAll()
    {
        Jedis redisClient = jedisPool.getResource();
        ArrayList<RunnableJob> jobs = new ArrayList<>();
        Job currentJob;
        for(String functionQueue : redisClient.keys("gm:*"))
        {
            Map<String, String> redisJobs = redisClient.hgetAll(functionQueue);
            for(String uniqueID : redisJobs.keySet())
            {
                try {
                    LOG.debug("Loading " + uniqueID + " from " + functionQueue);
                    String jobJSON = redisJobs.get(uniqueID);
                    LOG.debug("JSON: " + jobJSON);
                    currentJob = mapper.readValue(jobJSON, Job.class);
                    jobs.add(currentJob.getRunnableJob());
                    jobHandleToUniqueIdMap.put(currentJob.getJobHandle(), currentJob.getRunnableJob());
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
    public Collection<RunnableJob> getAllForFunction(String functionName) {
        Jedis redisClient = jedisPool.getResource();
        ArrayList<RunnableJob> jobs = new ArrayList<>();
        Map<String, String> redisJobs = redisClient.hgetAll("gm:" + functionName);
        Job currentJob;

        for(String uniqueID : redisJobs.keySet())
        {
            try {
                String jobJSON = redisJobs.get(uniqueID);
                currentJob = mapper.readValue(jobJSON, Job.class);

                jobs.add(currentJob.getRunnableJob());
            } catch (IOException e) {
                LOG.debug("Error deserializing job: " + e.toString());
            }
        }

        jedisPool.returnResource(redisClient);

        return jobs;
    }

    @Override
    public Job findJobByHandle(String jobHandle) {
        RunnableJob runnableJob = jobHandleToUniqueIdMap.get(jobHandle);
        return findJob(runnableJob.getFunctionName(), runnableJob.getUniqueID());
    }
}
