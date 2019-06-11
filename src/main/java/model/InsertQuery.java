package model;

import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import org.json.JSONArray;
import util.SchemaMap;

import java.util.Iterator;
import java.util.*;

/**
 * Contributed By: Tushar Mudgal
 * On: 11/6/19 | 3:59 PM
 */
public class InsertQuery {
    public static void buildResponse(WriteRowsEventV2 event, BaseQuery base, boolean isAsync) {
        Iterator<Row> rows = event.getRows().listIterator();
        List<Column> column = null;
        HashMap<String, Object> map = null;

        while (rows.hasNext()) {
            column = rows.next().getColumns();
            map = new HashMap<String, Object>();
            /*
             * Default keys for all operations
             */
            map.put("tableName", base.getTableName());
            map.put("databaseName", base.getDatabaseName());
            map.put("eventType", base.getEventType());

            JSONArray jsonArr = new JSONArray(column);

            System.out.println(jsonArr);

        }
    }
}
