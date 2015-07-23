package net.johnewart.gearman.engine.queue.persistence;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.engine.core.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;


public class RedisPersistenceEngine implements PersistenceEngine {

    private final ObjectMapper mapper;
    private final JedisPool jedisPool;
    private final String hostname;
    private final int port;
    private final Counter writtenCounter, failedCounter, deletedCounter;
    private final Timer writeTimer, readTimer;

    private static Logger LOG = LoggerFactory.getLogger(RedisPersistenceEngine.class);

    public RedisPersistenceEngine(String hostname, int port, MetricRegistry metricRegistry)
    {
        this.hostname = hostname;
        this.port = port;
        this.deletedCounter = metricRegistry.counter(name("redis", "deleted"));
        this.writtenCounter = metricRegistry.counter(name("redis", "jobs", "written"));
        this.failedCounter = metricRegistry.counter(name("redis", "jobs", "failed"));
        this.writeTimer = metricRegistry.timer(name("redis", "write"));
        this.readTimer = metricRegistry.timer(name("redis", "read"));

        mapper = new ObjectMapper();
        LOG.debug(String.format("RedisQueue connecting on %s:%s", hostname, port));
        jedisPool = new JedisPool(new JedisPoolConfig(), this.hostname, this.port);
    }

    @Override
    public String getIdentifier() {
        return String.format("Redis - %s:%d", this.hostname, this.port);
    }

    public boolean write(Job job)
    {
        Timer.Context context = writeTimer.time();
        Jedis redisClient = jedisPool.getResource();
        try {
            String json = mapper.writeValueAsString(job);
            String bucket = "gm:" + job.getFunctionName();
            String key = job.getUniqueID();
            redisClient.hset(bucket, key, json);
            LOG.debug("Storing in redis " + bucket + "-" + key + ": " + job.getUniqueID() + "/" + job.getJobHandle());
            writtenCounter.inc();
            return true;
        } catch (IOException e) {
            failedCounter.inc();
            e.printStackTrace();
            return false;
        } finally {
            jedisPool.returnResource(redisClient);
            context.stop();
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
        LOG.debug("Removing from redis " + bucket + ": " + uniqueID);
        redisClient.hdel(bucket, uniqueID);
        jedisPool.returnResource(redisClient);
        deletedCounter.inc();
    }

    public void deleteAll()
    {
        Jedis redisClient = jedisPool.getResource();
        redisClient.keys("gm:*").forEach(redisClient::del);
        jedisPool.returnResource(redisClient);
    }

    @Override
    public Job findJob(String functionName, String uniqueID)
    {
        Timer.Context timer = readTimer.time();
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
            timer.stop();
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

}
