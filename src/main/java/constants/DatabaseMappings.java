package constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 4:56 PM
 */
public class DatabaseMappings {
    public static HashMap<String, String> databaseNamesList = new HashMap<String, String>();

    public static boolean checkIfDatabaseExists(String dbName) {
        return databaseNamesList.containsKey(dbName);
    }

    public static void fillDatabaseMappings() {
        databaseNamesList.put("test", "test");
    }
}
