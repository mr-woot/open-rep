package constants;

import java.util.HashMap;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 3:23 PM
 */
public class TableMappings {
    public static HashMap<Long, String> kafkaTopicsMap = new HashMap<Long, String>();

    public static HashMap<Long, String> tableMap = new HashMap<Long, String>();

    public static String getTableName(Long id) {
        return tableMap.get(id);
    }

    public static boolean ifTableIdExistsInTableMap(Long tableId) {
        return tableMap.containsKey(tableId);
    }

    public static HashMap<Long, String> getTableMap() {
        return tableMap;
    }

    public static HashMap<Long, String> getKafkaTopicsMap() {
        return kafkaTopicsMap;
    }

    public static String getKafkaTopic(Long tableId) {
        return kafkaTopicsMap.get(tableId);
    }
}
