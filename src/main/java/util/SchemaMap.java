package util;

import conn.DataSource;
import model.SchemaMapBean;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 5:05 PM
 */
public class SchemaMap {
    public static HashMap<String, ArrayList> schemaTableDataTypeMap = new HashMap<String, ArrayList>();

    public SchemaMap() { }

    public void fillTableSchema() {
        this.__getTables();
    }

    public void fillTableWiseSchema(String tableName) {
        this.__getTableSchema(tableName);
    }

    public void __getTableSchema(String tableName) {
        Connection connection = null;
        ResultSet columnResultSet = null;
        ArrayList<SchemaMapBean> list = new ArrayList<SchemaMapBean>();
        try {
            connection = DataSource.getInstance().getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            columnResultSet = metaData.getColumns(null, null, tableName, null);
            while (columnResultSet.next()) {
                String columnType = columnResultSet.getString("TYPE_NAME");
                String columnName = columnResultSet.getString("COLUMN_NAME");
                Integer columnIndex = columnResultSet.getInt("ORDINAL_POSITION");
                // System.out.println(tableName+" # "+columnName+" # "+columnType +" # "+columnIndex);
                SchemaMapBean order_bean = new SchemaMapBean();
                order_bean.setPosition(columnIndex);
                order_bean.setColumnName(columnName);
                order_bean.setDataType(columnType);
                list.add(order_bean);
            }
            // ## cache manager put
            schemaTableDataTypeMap.put(tableName, list);
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

    private void __getTables__() {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = DataSource.getInstance().getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM information_schema.columns ORDER BY table_name,ordinal_position");
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                this.__getTableSchema(tableName);
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

    private void __getTables() {
        Connection connection = null;
        ResultSet tableResultSet = null;

        try {
            connection = DataSource.getInstance().getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            tableResultSet = metaData.getTables(null, null, null, null);
            while (tableResultSet.next()) {
                String tableName = tableResultSet.getString("TABLE_NAME");
                this.__getTableSchema(tableName);
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                if (tableResultSet != null)
                    tableResultSet.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

//    protected static void __display(String tableName) {
//        ArrayList<SchemaMapBean> dataSet = (ArrayList<SchemaMapBean>) CacheManagerSingletone.getSchemaDataType().get(tableName);
//        for (int i = 0; i < dataSet.size(); i++) {
//            SchemaMapBean data = (SchemaMapBean) dataSet.get(i);
//            System.out.println(tableName + " " + data.columnName + " " + data.position + " " + data.dataType);
//        }
//    }

}
