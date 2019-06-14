import com.google.code.or.OpenParser;
import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.gson.Gson;
import conn.Kafka;
import constants.DatabaseMappings;
import util.RedisUtils;
import util.SchemaMap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * Contributed By: Tushar Mudgal
 * On: 27/5/19 | 3:30 PM
 */
public class SpyWorkApplication {

    private static void startRealtimeBinlog() {
        Long binlogPosition = RedisUtils.getBinlogPosition();
        String binlogFileName = RedisUtils.getBinlogFileName();

        // Fill database mappings
        DatabaseMappings.fillDatabaseMappings();

        /*
         * Initialize Open Replicator with configs
         */
        final OpenReplicator or = new OpenReplicator();
        or.setUser("tushar");
        or.setPassword("tushar@123");
        or.setHost("localhost");
        or.setPort(3306);
        or.setServerId(1);
        or.setBinlogPosition(binlogPosition);
        or.setBinlogFileName(binlogFileName);

        /*
         * Fill table schema into in-memory
         */
        SchemaMap map = new SchemaMap();
        map.fillTableSchema();
        System.out.println(new Gson().toJson(SchemaMap.schemaTableMap));

        or.setBinlogEventListener(CheckEvent::getEvent);

        System.out.println("press 'q' to stop");
        final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            or.start();
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                if (line.equals("q")) {
                    or.stop(1000, null);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startStockBinlog() throws Exception {
        Long binlogPosition = 4L;
        String binlogFileName = "/home/tushar/Documents/poc";

        // Fill database mappings
//        DatabaseMappings.fillDatabaseMappings();

        final OpenParser op = new OpenParser();
        op.setStartPosition(4);
        op.setBinlogFileName("binlog.000001");
        op.setBinlogFilePath(binlogFileName);
        op.setBinlogEventListener(CheckEvent::getEvent);

        /*
         * Set Kafka connection
         */
        Kafka.createConnection();

        /*
         * Fill table schema into in-memory
         */
        SchemaMap map = new SchemaMap();
        map.fillTableSchema();
        System.out.println(new Gson().toJson(SchemaMap.schemaTableMap));

        op.start();

        System.out.println("press 'q' to stop");
        final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        for(String line = br.readLine(); line != null; line = br.readLine()) {
            if(line.equals("q")) {
                op.stop(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                break;
            }
        }
    }

    public static void main(String[] args) {
        try {
            startStockBinlog();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
