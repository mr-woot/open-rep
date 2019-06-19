package model;

import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import org.json.JSONException;
import org.json.JSONObject;
import util.SchemaMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Contributed By: Tushar Mudgal
 * On: 12/6/19 | 2:25 PM
 */
public class UpdateQuery {
    public static String buildResponse(UpdateRowsEventV2 event, BaseQuery base) {
        Iterator<Pair<Row>> rows1 = event.getRows().listIterator();
        Iterator<Pair<Row>> rows2 = event.getRows().listIterator();
        HashMap<String, Object> map = null;
        List<Column> before = null;
        List<Column> after = null;

        try {
            ArrayList tableSchema = SchemaMap.schemaTableMap.get(base.getTableName());

            while (rows1.hasNext() && rows2.hasNext()) {
                before = rows1.next().getBefore().getColumns();
                after = rows2.next().getAfter().getColumns();
                map = new HashMap<String, Object>();
                /*
                 * Default keys for all operations
                 */
                map = SchemaMap.setDefaultValues(map, base);

                JSONObject dataObject = new JSONObject();
                JSONObject beforeMap = new JSONObject();
                JSONObject afterMap = new JSONObject();

                for (int i = 0; i < before.size(); i++) {
                    SchemaMapBean dataMap = (SchemaMapBean) tableSchema.get(i);
                    beforeMap.put(dataMap.columnName, before.get(i));
                    afterMap.put(dataMap.columnName, after.get(i));
                }

                dataObject.put("before", beforeMap);
                dataObject.put("after", afterMap);
                map.put("data", dataObject);
            }
        } catch (JSONException e) {
//            e.printStackTrace();
        }
        return new JSONObject(map).toString();
    }
}
