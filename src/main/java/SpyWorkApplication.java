import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.gson.Gson;
import constants.DatabaseMappings;
import util.SchemaMap;
import util.RedisUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Contributed By: Tushar Mudgal
 * On: 27/5/19 | 3:30 PM
 */
public class SpyWorkApplication {
    public static void main(String[] args) {
        // get binlogPosition and binlogFilename from redis and put it
        // into open replicator constructor.
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

        or.setBinlogEventListener(new BinlogEventListener() {
            public void onEvents(BinlogEventV4 event) {
                CheckEvent.getEvent(event);
            }
        });

        System.out.println("press 'q' to stop");
        final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            or.start();
            for(String line = br.readLine(); line != null; line = br.readLine()) {
                if(line.equals("q")) {
                    or.stop(1000, null);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
