package model;

import com.google.code.or.binlog.impl.event.QueryEvent;
import org.json.JSONObject;
import util.SchemaMap;

import java.util.HashMap;

/**
 * Contributed By: Tushar Mudgal
 * On: 18/6/19 | 12:07 PM
 */
public class AlterCreateRenameQuery {
    public static String buildResponse(QueryEvent event, BaseQuery base) {
        String sql = event.getSql().toString().trim().toLowerCase();
        HashMap<String, Object> map = new HashMap<>();
        map = SchemaMap.setDefaultValues(map, base);
        map.put("sql", sql);
        return new JSONObject(map).toString();
    }
}
