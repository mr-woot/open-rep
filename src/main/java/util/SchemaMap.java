package util;

import com.google.code.or.common.glossary.Column;
import conn.DataSource;
import model.BaseQuery;
import model.SchemaMapBean;
import org.json.JSONObject;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 5:05 PM
 */
public class SchemaMap {
    public static HashMap<String, ArrayList> schemaTableMap = new HashMap<String, ArrayList>();

    public SchemaMap() { }

    public void fillTableSchema() {
        this.__getTables();
    }

    public static HashMap<String, Object> setDefaultValues(HashMap<String, Object> map, BaseQuery base) {
        map.put("tableName", base.getTableName());
        map.put("databaseName", base.getDatabaseName());
        map.put("eventType", base.getEventType());
        map.put("timestamp", base.getTimestamp());
        return map;
    }

    public static JSONObject getColumnsMap(ArrayList tableSchema, List<Column> columns) {
        JSONObject dataObject = new JSONObject();
        for (int i = 0; i < columns.size(); i++) {
            SchemaMapBean data = (SchemaMapBean) tableSchema.get(i);
            Object value = columns.get(i);
            dataObject.put(data.columnName, value);
        }
        return dataObject;
    }

    public void fillTableWiseSchema(String tableSchema, String tableName) {
        Connection conn = null;
        ResultSet tables = null;
        try {
            conn= DataSource.getInstance().getConnection();
            DatabaseMetaData md = conn.getMetaData();
            System.out.println(tableSchema + tableName);
            tables = md.getTables(null, tableSchema, tableName, null);
            while (tables.next()) {
                fillSchemaMap(conn, tables);
            }
        } catch (SQLException | IOException | PropertyVetoException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (tables != null) {
                    tables.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void fillSchemaMap(Connection conn, ResultSet tables) throws SQLException {
        String table =  tables.getString(3);
        Statement st = conn.createStatement();
        ResultSet rstable = st.executeQuery("SELECT * FROM " + table + " limit 1");
        ResultSetMetaData rsMetaData = rstable.getMetaData();
        ArrayList<SchemaMapBean> list = new ArrayList<SchemaMapBean>();
        for(int i = 1;i <= rsMetaData.getColumnCount();i++) {
            String columnName = rsMetaData.getColumnName(i);
            String columnType = rsMetaData. getColumnTypeName(i);
            setOrderBean(list, i, columnName, columnType);
        }
        schemaTableMap.put(table, list);
    }

    private void setOrderBean(ArrayList<SchemaMapBean> list, int i, String columnName, String columnType) {
        SchemaMapBean orderBean = new SchemaMapBean();
        orderBean.setPosition(i);
        orderBean.setColumnName(columnName);
        orderBean.setDataType(columnType);
        list.add(orderBean);
    }

    private void __getTables() {
        Connection conn = null;
        ResultSet tables = null;
        try {
            conn= DataSource.getInstance().getConnection();
            DatabaseMetaData md = conn.getMetaData();
            tables = md.getTables(null, "%", "%", null);
            System.out.println("Loading");
            int i = 0;
            while (tables.next()) {
                i++;
                if (i%100 == 0) {
                    System.out.println(".");
                } else {
                    System.out.print(".");
                }
                fillSchemaMap(conn, tables);
            }
            System.out.println("Loaded......");
        } catch (SQLException | IOException | PropertyVetoException e) {
            e.printStackTrace();
        }
    }

}
