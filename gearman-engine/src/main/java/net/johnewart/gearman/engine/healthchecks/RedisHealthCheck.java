package net.johnewart.gearman.engine.healthchecks;

import com.codahale.metrics.health.HealthCheck;
import redis.clients.jedis.Jedis;

public class RedisHealthCheck extends HealthCheck
{
    private final Jedis redisClient;

    public RedisHealthCheck(Jedis redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public Result check() throws Exception {
        if (redisClient.isConnected()) {
            return Result.healthy();
        } else {
            return Result.unhealthy("Cannot connect to " + redisClient.toString());
        }
    }
}
