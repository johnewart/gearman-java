package org.gearman.server.healthchecks;

import com.yammer.metrics.core.HealthCheck;
import redis.clients.jedis.Jedis;

public class RedisHealthCheck extends HealthCheck {
    private final Jedis redisClient;

    public RedisHealthCheck(Jedis redisClient) {
        super("redis");
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
