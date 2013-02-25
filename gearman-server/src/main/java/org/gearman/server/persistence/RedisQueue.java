package org.gearman.server.persistence;

import org.codehaus.jackson.map.ObjectMapper;
import org.gearman.server.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/13/12
 * Time: 12:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class RedisQueue implements PersistenceEngine {

    private final ObjectMapper mapper;
    private final JedisPool jedisPool;
    private static Logger LOG = LoggerFactory.getLogger(RedisQueue.class);

    public RedisQueue()
    {
        LOG.debug("RedisQueue connecting on localhost:6379");
        jedisPool = new JedisPool(new JedisPoolConfig(), "localhost");
        mapper = new ObjectMapper();
    }

    public void write(Job job)
    {
        Jedis redisClient = jedisPool.getResource();
        String json = null;
        try {
            json = mapper.writeValueAsString(job);
            String bucket = "gm:" + job.getFunctionName();
            String key = job.getUniqueID().toString();
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
        String key = job.getUniqueID().toString();
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
        jedisPool.returnResource(redisClient);
    }

    @Override
    public Job findJob(String functionName, String uniqueID)
    {
        Jedis redisClient = jedisPool.getResource();
        Job job = null;
        try {
            String bucket = "gm:" + functionName;
            String key = uniqueID;
            String jobJSON = redisClient.hgetAll(bucket).get(key);
            job = mapper.readValue(jobJSON, Job.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        jedisPool.returnResource(redisClient);

        return job;
    }

    public Collection<Job> readAll()
    {
        Jedis redisClient = jedisPool.getResource();
        ArrayList<Job> jobs = new ArrayList<Job>();
        for(String functionQueue : redisClient.keys("gm:*"))
        {
            Map<String, String> redisJobs = redisClient.hgetAll(functionQueue);
            for(String uniqueID : redisJobs.keySet())
            {
                try {
                    LOG.debug("Loading " + uniqueID + " from " + functionQueue);
                    String jobJSON = redisJobs.get(uniqueID);
                    LOG.debug("JSON: " + jobJSON);
                    Job job = mapper.readValue(jobJSON, Job.class);
                    jobs.add(job);
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
    public Collection<Job> getAllForFunction(String functionName) {
        Jedis redisClient = jedisPool.getResource();
        ArrayList<Job> jobs = new ArrayList<Job>();
        Map<String, String> redisJobs = redisClient.hgetAll("gm:" + functionName);

        for(String uniqueID : redisJobs.keySet())
        {
            try {
                String jobJSON = redisJobs.get(uniqueID);
                Job job = mapper.readValue(jobJSON, Job.class);
                jobs.add(job);
            } catch (IOException e) {
                LOG.debug("Error deserializing job: " + e.toString());
            }
        }

        jedisPool.returnResource(redisClient);

        return jobs;
    }
}
