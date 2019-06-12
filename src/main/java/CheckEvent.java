/**
 * Contributed By: Tushar Mudgal
 * On: 27/5/19 | 5:38 PM
 */

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;
import constants.DatabaseMappings;
import constants.TableMappings;
import model.BaseQuery;
import model.InsertDeleteQuery;
import model.SchemaMapBean;
import model.UpdateQuery;
import util.SchemaMap;

public class CheckEvent {
    static String databaseName;

    public CheckEvent() { }

    public static void getEvent(BinlogEventV4 event) {
        String binlogFileName = "";
        long binlogPosition = 0;
        boolean insertFlag = false,
                rotateEventFlag = false;

        try {
            if (event == null) {
                System.out.println("Event is null");
                return;
            }
            int eventType = event.getHeader().getEventType();
            switch (eventType) {
                /*
                 * Query event
                 */
                case MySQLConstants.QUERY_EVENT: {
                    QueryEvent queryEvent = (QueryEvent) event;

                    // get query statement
                    // check if its alter, create or rename
                    //
                    String sqlQuery = queryEvent.getSql().toString().trim().toLowerCase();

                    sqlQuery= sqlQuery.replaceAll("\\s\\s*", " ");
                    sqlQuery = sqlQuery.replace("(", " ");
                    sqlQuery = sqlQuery.replace("if not exists ", "");

                    // no need for removing ")"
                    // sqlQuery = sqlQuery.replace(")", " ");

                    sqlQuery= sqlQuery.replaceAll("\\s\\s*", " ");

                    SchemaMap schemaMap = new SchemaMap();

                    if (sqlQuery.startsWith("create")) {
                        schemaMap.fillTableWiseSchema(queryEvent.getDatabaseName().toString(), sqlQuery.split(" ")[2]);
//                        System.out.println(sqlQuery.split(" ")[2]);
                    } else if (sqlQuery.startsWith("alter")) {
                        schemaMap.fillTableWiseSchema(queryEvent.getDatabaseName().toString(), sqlQuery.split(" ")[2]);
//                        System.out.println(sqlQuery.split(" ")[2]);
                    } else if (sqlQuery.startsWith("rename")) {
                        schemaMap.fillTableWiseSchema(queryEvent.getDatabaseName().toString(), sqlQuery.split(" ")[2]);
//                        System.out.println(sqlQuery.split(" ")[2]);
                    } else {
//                        System.out.println("Other queries");
                    }

                    // ## Build response that needs to be send to kafka.
                    // ## Send queryEvent to kafka topic of type
                    break;
                }
                /*
                 * Table map event
                 */
                case MySQLConstants.TABLE_MAP_EVENT: {
                    TableMapEvent tableMapEvent = (TableMapEvent) event;
                    String tableName = tableMapEvent.getTableName().toString();
                    databaseName = tableMapEvent.getDatabaseName().toString();
                    TableMappings.tableMap.put(tableMapEvent.getTableId(), tableName);
                    break;
                }
                /*
                 * Create (Write) rows event
                 */
                case MySQLConstants.WRITE_ROWS_EVENT_V2: {
                    // check if database_name is in the required ones
                    // get binlogFileName
                    // get binlogPosition
                    // get tableName according to tableId
                    // get kafkaTopicMappings according to tableId
                    // if kafkaTopic exists, then produce to kafka the generated response.
                    // set insert flag true
                    if (DatabaseMappings.databaseNamesList.get(databaseName) != null) {
                        WriteRowsEventV2 writeRowsEventV2 = (WriteRowsEventV2) event;
                        binlogFileName = writeRowsEventV2.getBinlogFilename();
                        binlogPosition = writeRowsEventV2.getHeader().getNextPosition();
                        String tableName = TableMappings.getTableName(writeRowsEventV2.getTableId());
                        String kafkaTopic = TableMappings.getKafkaTopic(writeRowsEventV2.getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                            BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "insert", event.getHeader().getTimestamp());
                            new InsertDeleteQuery().buildResponse(writeRowsEventV2, baseQuery);
                            insertFlag = true;
                            // ## Send writeRowsEventV2 to kafka topic <?>
//                        }
                    }
                    break;
                }
                /*
                 * Update rows event
                 */
                case MySQLConstants.UPDATE_ROWS_EVENT_V2: {
                    if (DatabaseMappings.databaseNamesList.get(databaseName) != null) {
                        UpdateRowsEventV2 updateRowsEventV2 = (UpdateRowsEventV2) event;
                        binlogFileName = updateRowsEventV2.getBinlogFilename();
                        binlogPosition = updateRowsEventV2.getHeader().getNextPosition();
                        String tableName = TableMappings.getTableName(updateRowsEventV2.getTableId());
                        String kafkaTopic = TableMappings.getKafkaTopic(updateRowsEventV2.getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                        BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "update", updateRowsEventV2.getHeader().getTimestamp());
                        UpdateQuery.buildResponse(updateRowsEventV2, baseQuery);
                        insertFlag = true;
                        // ## Send writeRowsEventV2 to kafka topic <?>
//                        }
                    }

                    // ## Send writeRowsEventV2 to kafka topic <?>
                    break;
                }
                /*
                 * Delete row event
                 */
                case MySQLConstants.DELETE_ROWS_EVENT_V2: {
                    if (DatabaseMappings.databaseNamesList.get(databaseName) != null) {
                        DeleteRowsEventV2 deleteRowsEvent = (DeleteRowsEventV2) event;
                        binlogFileName = deleteRowsEvent.getBinlogFilename();
                        binlogPosition = deleteRowsEvent.getHeader().getNextPosition();
                        String tableName = TableMappings.getTableName(deleteRowsEvent.getTableId());
                        String kafkaTopic = TableMappings.getKafkaTopic(deleteRowsEvent.getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                        BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "delete", event.getHeader().getTimestamp());
                        new InsertDeleteQuery().buildResponse(deleteRowsEvent, baseQuery);
                        insertFlag = true;
                        // ## Send writeRowsEventV2 to kafka topic <?>
//                        }
                    }
                    // ## Send deleteRowsEvent to kafka topic <?>
                    break;
                }
                case MySQLConstants.ROTATE_EVENT: {
                    RotateEvent rotateEvent = (RotateEvent) event;
//                    System.out.println("re--" + rotateEvent);
                    // ## Insert into redis the last binLogFile position and filename
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Error Ocurred in getEvent method switch case block " + e);
        }
    }
}