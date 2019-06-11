package util.impl;

import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.gson.Gson;
import model.BaseQuery;
import util.ResponseBuilderIFace;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 1:28 PM
 */
public class ResponseBuilder implements ResponseBuilderIFace {
    @Override
    public String buildInsertQueryResponse(BinlogEventV4 event, String tableName, String databaseName) {
        BinlogEventV4Header header = event.getHeader();
        return new Gson().toJson(new BaseQuery(tableName, databaseName, "insert", header.getTimestamp()));
    }
}
