import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.gson.Gson;
import conn.DataSource;
import constants.DatabaseMappings;
import util.ConfigBundle;
import util.SchemaMap;

import javax.xml.crypto.Data;
import java.awt.image.DataBuffer;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Contributed By: Tushar Mudgal
 * On: 27/5/19 | 3:30 PM
 */
public class SpyWorkApplication {
    public static void main(String[] args) {
        // Fill database mappings
        DatabaseMappings.fillDatabaseMappings();

        final OpenReplicator or = new OpenReplicator();
        or.setUser("tushar");
        or.setPassword("tushar@123");
        or.setHost("localhost");
        or.setPort(3306);
        or.setServerId(1);
        or.setBinlogPosition(4);
        or.setBinlogFileName("bin.000014");

        /*
         * Fill table schema into in-memory
         */
        SchemaMap map = new SchemaMap();
        map.fillTableSchema();
        System.out.println(new Gson().toJson(SchemaMap.schemaTableDataTypeMap));

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
