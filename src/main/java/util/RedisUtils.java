package util;

import conn.Redis;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * Contributed By: Tushar Mudgal
 * On: 13/6/19 | 3:53 PM
 */
public class RedisUtils {

    public static Long getBinlogPosition() {
        StatefulRedisConnection<String, String> conn = Redis.getInstance();
        String binlogPosition = conn.sync().get("BINLOG_POSITION");
        return Long.valueOf(binlogPosition);
    }

    public static void setBinlogPosition(Long position) {
        StatefulRedisConnection<String, String> conn = Redis.getInstance();
        conn.sync().set("BINLOG_POSITION", String.valueOf(position));
    }

    public static String getBinlogFileName() {
        StatefulRedisConnection<String, String> conn = Redis.getInstance();
        return conn.sync().get("BINLOG_FILENAME");
    }

    public static void setBinlogFileName(String filename) {
        StatefulRedisConnection<String, String> conn = Redis.getInstance();
        conn.sync().set("BINLOG_FILENAME", String.valueOf(filename));
    }
}
