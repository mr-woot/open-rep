package conn;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * Contributed By: Tushar Mudgal
 * On: 13/6/19 | 12:17 PM
 */
public class Redis {
    private static StatefulRedisConnection<String, String> redisConnection;

    /**
     * Get instance of the redis connection.
     * @return StatefulRedisConnection
     */
    public static StatefulRedisConnection<String, String> getInstance(){
        if (redisConnection == null) {
            try {
                RedisURI URI = RedisURI.Builder.redis("localhost", 6379).withDatabase(0).build();
                RedisClient redisClient = RedisClient
                        .create(URI);
                redisConnection = redisClient.connect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return redisConnection;
    }
}