package dspa_project.database.init;

import dspa_project.config.ConfigLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @source mysqltutorial.org
 */
public class MySQLJDBCUtil {

    /**
     * Get database connection
     *
     * @return a Connection object
     */
    public static Connection getConnection(){
        // assign db parameters
        String url = ConfigLoader.getSql_url();
        String user = ConfigLoader.getSql_user();
        String password = ConfigLoader.getSql_password();

        // create a connection to the database
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }
}
