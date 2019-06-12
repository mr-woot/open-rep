package model;

import java.io.Serializable;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 5:06 PM
 */
public class SchemaMapBean implements Serializable {
    private static final long serialVersionUID = 6035974668097879675L;
    public Integer position;
    public String columnName;
    public String dataType;


    public SchemaMapBean() { }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SchemaMapBean{");
        sb.append("position=").append(position);
        sb.append(", columnName='").append(columnName).append('\'');
        sb.append(", dataType='").append(dataType).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
