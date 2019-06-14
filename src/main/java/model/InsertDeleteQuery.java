package model;

import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import org.json.JSONObject;
import util.SchemaMap;

import java.util.Iterator;
import java.util.*;

/**
 * Contributed By: Tushar Mudgal
 * On: 11/6/19 | 3:59 PM
 */
public class InsertDeleteQuery {
    private String build(Iterator<Row> rows, BaseQuery base) throws NullPointerException {
        List<Column> column = null;
        HashMap<String, Object> map = null;

        try {
            ArrayList tableSchema = SchemaMap.schemaTableMap.get(base.getTableName());

            while (rows.hasNext()) {
                map = new HashMap<String, Object>();
                column = rows.next().getColumns();
                /*
                 * Default keys for all operations
                 */
                map = SchemaMap.setDefaultValues(map, base);

                map.put("data", SchemaMap.getColumnsMap(tableSchema, column));
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return new JSONObject(map).toString();
    }
    public String buildResponse(WriteRowsEventV2 event, BaseQuery base) {
        return this.build(event.getRows().listIterator(), base);
    }
    public String buildResponse(DeleteRowsEventV2 event, BaseQuery base) {
        return this.build(event.getRows().listIterator(), base);
    }

}
