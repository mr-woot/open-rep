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
import model.InsertQuery;
import util.SchemaMap;

import static util.SchemaMap.schemaTableDataTypeMap;

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
//                    String query = queryEvent.getSql().toString().trim().toLowerCase();
//                    System.out.println(query);
                    // ## Build response that needs to be send to kafka.

                    /*
                     * ## Send queryEvent to kafka topic of type
                     * {
                     *      type: <insert | update | delete | {alter, rename, create}>
                     *
                     * }
                     */
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
                        binlogFileName = ((WriteRowsEventV2) event).getBinlogFilename();
                        binlogPosition = event.getHeader().getNextPosition();
                        String tableName = TableMappings.getTableName(((WriteRowsEventV2) event).getTableId());
//                        String kafkaTopic = TableMappings.getKafkaTopic(((WriteRowsEventV2) event).getTableId());
//                        if (kafkaTopic != null) {
//                            System.out.println("Produced to kafka with topic: " + kafkaTopic);
                            BaseQuery baseQuery = new BaseQuery(tableName, databaseName, "insert", event.getHeader().getTimestamp());
                            InsertQuery.buildResponse(writeRowsEventV2, baseQuery, false);
                            // ## Send writeRowsEventV2 to kafka topic <?>
//                        }
                    }
                    break;
                }
                /*
                 * Update rows event
                 */
                case MySQLConstants.UPDATE_ROWS_EVENT_V2: {
                    UpdateRowsEventV2 updateRowsEventV2 = (UpdateRowsEventV2) event;
//                    System.out.println("ure--" + updateRowsEventV2);
                    // ## Send writeRowsEventV2 to kafka topic <?>
                    break;
                }
                /*
                 * Delete row event
                 */
                case MySQLConstants.DELETE_ROWS_EVENT_V2: {
                    DeleteRowsEventV2 deleteRowsEvent = (DeleteRowsEventV2) event;
//                    System.out.println("dre--" + deleteRowsEvent);
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