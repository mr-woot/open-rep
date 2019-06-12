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
        this.__getTableSchema(tableSchema, tableName);
    }

    private void __getTableSchema(String tableSchema, String tableName) {
        Connection connection = null;
        ResultSet columnResultSet = null;
        ArrayList<SchemaMapBean> list = new ArrayList<SchemaMapBean>();
        try {
            connection = DataSource.getInstance().getConnection();
            PreparedStatement statement = connection.prepareStatement("select * from information_schema.columns" +
                    " where table_name = '" + tableName + "'" +
                    " and table_schema = '" + tableSchema + "'" +
                    " order by table_name,ordinal_position");
            columnResultSet = statement.executeQuery();
            while (columnResultSet.next()) {
                String columnType = columnResultSet.getString("DATA_TYPE");
                String columnName = columnResultSet.getString("COLUMN_NAME");
                Integer columnIndex = columnResultSet.getInt("ORDINAL_POSITION");
                SchemaMapBean order_bean = new SchemaMapBean();
                order_bean.setPosition(columnIndex);
                order_bean.setColumnName(columnName);
                order_bean.setDataType(columnType);
                list.add(order_bean);
            }
            // ## cache manager put
            schemaTableMap.put(tableName, list);
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (columnResultSet != null)
                    columnResultSet.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }

    }

    private void __getTables() {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = DataSource.getInstance().getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM information_schema.columns ORDER BY table_name");
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String tableSchema = resultSet.getString("TABLE_SCHEMA");
                this.__getTableSchema(tableSchema, tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
