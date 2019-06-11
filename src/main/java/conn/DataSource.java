package conn;

import org.apache.commons.dbcp2.BasicDataSource;
import util.ConfigBundle;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Contributed By: Tushar Mudgal
 * On: 10/6/19 | 5:09 PM
 */
public class DataSource {

    private static DataSource datasource;
    private BasicDataSource ds;

    private DataSource() throws IOException, SQLException, PropertyVetoException {
        ds = new BasicDataSource();
        ds.setDriverClassName(ConfigBundle.getValue("jdbc_driver"));
        ds.setUsername(ConfigBundle.getValue("user"));
        ds.setPassword(ConfigBundle.getValue("password"));
        ds.setUrl(ConfigBundle.getValue("db_url"));

        // the settings below are optional -- dbcp can work with defaults
        ds.setMinIdle(5);
        ds.setMaxIdle(10);
        ds.setMaxTotal(15);
        ds.setMaxOpenPreparedStatements(180);
    }

    public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException {
        if (datasource == null) {
            datasource = new DataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }


}
