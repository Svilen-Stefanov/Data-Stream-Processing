package dspa_project.database.queries;

import dspa_project.DataLoader;
import dspa_project.database.init.MySQLJDBCUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLQuery {
    public static ArrayList<Long> getSubclassesOfTagclass(long value){
        Connection conn = null;
        Statement st = null;
        ArrayList<Long> subclasses = new ArrayList<Long>();
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            // QUERY
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "tagclass_isSubclassOf_tagclass", null);

            if (rs.next()) {
                String query = "SELECT TAG_CLASS_ID" +
                                    " FROM tagclass_isSubclassOf_tagclass" +
                                    " WHERE PARENT_TAG_CLASS_ID = " + value + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    subclasses.add(result.getLong("TAG_CLASS_ID"));
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(DataLoader.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch(Exception e){
            e.printStackTrace();
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(DataLoader.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return subclasses;
    }
}
