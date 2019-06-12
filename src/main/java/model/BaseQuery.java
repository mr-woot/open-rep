package model;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 2:08 PM
 */
public class BaseQuery {

    /*
     * Header attributes
     */
    private String tableName;
    private String databaseName;
    private Long timestamp;
    private String eventType;

    public BaseQuery(String tableName, String databaseName, String eventType, Long timestamp) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BaseQuery{");
        sb.append("tableName='").append(tableName).append('\'');
        sb.append(", databaseName='").append(databaseName).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", eventType='").append(eventType).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
