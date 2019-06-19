/**
 * Contributed By: Tushar Mudgal
 * On: 27/5/19 | 5:38 PM
 */

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;
import constants.TableMappings;
import lombok.extern.log4j.Log4j;
import model.AlterCreateRenameQuery;
import model.BaseQuery;
import model.InsertDeleteQuery;
import model.UpdateQuery;
import util.KafkaUtils;
import util.RedisUtils;
import util.SchemaMap;

@Log4j
public class CheckEvent {
    static String databaseName;

    public CheckEvent() {
    }

    public static void getEvent(BinlogEventV4 event) {
        String binlogFileName = "";
        long binlogPosition = 0;
        boolean insertFlag = false,
                rotateEventFlag = false;

        try {
            if (event == null) {
                log.error("Event is empty");
                return;
            }
            int eventType = event.getHeader().getEventType();
            switch (eventType) {
                /*
                 * Query event
                 */
                case MySQLConstants.QUERY_EVENT: {
                    QueryEvent queryEvent = (QueryEvent) event;
                    databaseName = queryEvent.getDatabaseName().toString();
                    // get query statement
                    // check if its alter, create or rename
                    String sqlQuery = queryEvent.getSql().toString().trim().toLowerCase();
                    sqlQuery = sqlQuery.replaceAll("\\s\\s*", " ");
                    sqlQuery = sqlQuery.replace("(", " ");
                    sqlQuery = sqlQuery.replace("if not exists ", "");
                    // no need for removing ")"
                    // sqlQuery = sqlQuery.replace(")", " ");
                    sqlQuery = sqlQuery.replaceAll("\\s\\s*", " ");
                    SchemaMap schemaMap = new SchemaMap();
                    if (sqlQuery.startsWith("create")) {
                        sendDdlToKafka(event, queryEvent, sqlQuery, schemaMap, "create");
                    } else if (sqlQuery.startsWith("alter")) {
                        sendDdlToKafka(event, queryEvent, sqlQuery, schemaMap, "alter");
                    } else if (sqlQuery.startsWith("rename")) {
                        sendDdlToKafka(event, queryEvent, sqlQuery, schemaMap, "rename");
                    }
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
//                    if (DatabaseMappings.databaseNamesList.get(databaseName) != null) {
                    WriteRowsEventV2 writeRowsEventV2 = (WriteRowsEventV2) event;
                    binlogFileName = writeRowsEventV2.getBinlogFilename();
                    binlogPosition = writeRowsEventV2.getHeader().getNextPosition();
                    String tableName = TableMappings.getTableName(writeRowsEventV2.getTableId());
                    String kafkaTopic = TableMappings.getKafkaTopic(writeRowsEventV2.getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                    BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "insert", event.getHeader().getTimestamp());
                    String response = new InsertDeleteQuery().buildResponse(writeRowsEventV2, baseQuery);
                    KafkaUtils.sendMessage("com.paisabzaar.core.open-replicator-test", response);
                    insertFlag = true;
//                        }
//                    }
                    break;
                }
                /*
                 * Update rows event
                 */
                case MySQLConstants.UPDATE_ROWS_EVENT_V2: {
//                    if (DatabaseMappings.databaseNamesList.get(databaseName) != null) {
                    UpdateRowsEventV2 updateRowsEventV2 = (UpdateRowsEventV2) event;
                    binlogFileName = updateRowsEventV2.getBinlogFilename();
                    binlogPosition = updateRowsEventV2.getHeader().getNextPosition();
                    String tableName = TableMappings.getTableName(updateRowsEventV2.getTableId());
                    String kafkaTopic = TableMappings.getKafkaTopic(updateRowsEventV2.getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                    BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "update", updateRowsEventV2.getHeader().getTimestamp());
                    String response = UpdateQuery.buildResponse(updateRowsEventV2, baseQuery);
                    KafkaUtils.sendMessage("com.paisabzaar.core.open-replicator-test", response);
                    insertFlag = true;
//                        }
//                    }
                    break;
                }
                /*
                 * Delete row event
                 */
                case MySQLConstants.DELETE_ROWS_EVENT_V2: {
//                    if (DatabaseMappings.databaseNamesList.get(databaseName) != null) {
                    DeleteRowsEventV2 deleteRowsEvent = (DeleteRowsEventV2) event;
                    binlogFileName = deleteRowsEvent.getBinlogFilename();
                    binlogPosition = deleteRowsEvent.getHeader().getNextPosition();
                    String tableName = TableMappings.getTableName(deleteRowsEvent.getTableId());
                    String kafkaTopic = TableMappings.getKafkaTopic(deleteRowsEvent.getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                    BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "delete", event.getHeader().getTimestamp());
                    String response = new InsertDeleteQuery().buildResponse(deleteRowsEvent, baseQuery);
                    KafkaUtils.sendMessage("com.paisabzaar.core.open-replicator-test", response);
                    insertFlag = true;
//                        }
//                    }
                    break;
                }
                case MySQLConstants.ROTATE_EVENT: {
                    RotateEvent rotateEvent = (RotateEvent) event;
                    binlogFileName = rotateEvent.getBinlogFileName().toString();
                    binlogPosition = rotateEvent.getBinlogPosition();
                    rotateEventFlag = true;
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Error Ocurred in getEvent method switch case block " + e);
        } finally {
            if (insertFlag || rotateEventFlag) {
//                RedisUtils.setBinlogFileName(binlogFileName);
//                RedisUtils.setBinlogPosition(binlogPosition);
            }
        }
    }

    private static void sendDdlToKafka(BinlogEventV4 event, QueryEvent queryEvent, String sqlQuery, SchemaMap schemaMap, String create) {
        BaseQuery baseQuery;
        String tableName = sqlQuery.split(" ")[2];
        schemaMap.fillTableWiseSchema(queryEvent.getDatabaseName().toString(), tableName);
        baseQuery = new BaseQuery(tableName, databaseName, create, event.getHeader().getTimestamp());
        String response = AlterCreateRenameQuery.buildResponse(queryEvent, baseQuery);
        KafkaUtils.sendMessage("com.paisabzaar.core.open-replicator-test-ddl", response);
    }
}