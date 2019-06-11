package util;

import com.google.code.or.binlog.BinlogEventV4;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 1:26 PM
 */
public interface ResponseBuilderIFace {
    String buildInsertQueryResponse(BinlogEventV4 event, String tableName, String databaseName);
}
