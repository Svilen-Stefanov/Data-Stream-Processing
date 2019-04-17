package dspa_project.database.queries;

import dspa_project.DataLoader;
import dspa_project.database.init.MySQLJDBCUtil;
import dspa_project.model.Person;

import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLQuery {
    //TODO make a single method where you pass the query as the argument.
    //change the getInt to getLong and also the name of the column we get
    public static int getNumberOfPeople(){
        int res = -1;
        Connection conn = null;
        Statement st = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person", null);

            if (rs.next()) {
                String query = "SELECT Count(*) AS `count`" +
                        " FROM `person`" + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    res = result.getInt("count");
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return res;
    }

    public static long [] getUniversity(long personId){
        Connection conn = null;
        Statement st = null;
        long [] subclasses = new long[2];
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person", null);

            if (rs.next()) {
                String query = "SELECT `ORGANIZATION_ID`, `CLASS_YEAR`" +
                        " FROM `study_at`" +
                        " WHERE `PERSON_ID` = " + personId + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    subclasses[0] = result.getLong("ORGANIZATION_ID");
                    subclasses[1] = result.getLong("CLASS_YEAR");
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return subclasses;
    }

    public static ArrayList<Long> getWorkAt(long personId){
        Connection conn = null;
        Statement st = null;
        ArrayList<Long> subclasses = new ArrayList<Long>();
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person", null);

            if (rs.next()) {
                String query = "SELECT `ORGANIZATION_ID`" +
                        " FROM `study_at`" +
                        " WHERE `PERSON_ID` = " + personId + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    subclasses.add(result.getLong("ORGANIZATION_ID"));
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return subclasses;
    }

    public static ArrayList<Long> getPossibleFriends(long personId){
        Connection conn = null;
        Statement st = null;
        ArrayList<Long> subclasses = new ArrayList<Long>();
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person", null);

            if (rs.next()) {
                String query = "SELECT `id` AS `possible_friend_id`" +
                        " FROM `person`" +
                        " WHERE `id` NOT IN (" +
                            " SELECT `Person_id_B`" +
                            " FROM `person_knows_person`" +
                            " WHERE `Person_id_A` = " + personId + ")" +
                        " AND NOT `id` = "  + personId + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    subclasses.add(result.getLong("possible_friend_id"));
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return subclasses;
    }

    public static ArrayList<Long> getTagsOfInterest(long personId){
        Connection conn = null;
        Statement st = null;
        ArrayList<Long> subclasses = new ArrayList<Long>();
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person", null);

            if (rs.next()) {
                String query = "SELECT `tag_id`"+
                        " FROM `person_hasInterest_tag`" +
                        " WHERE `Person_id` = " + personId + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    subclasses.add(result.getLong("tag_id"));
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return subclasses;
    }

    public static long getTagClass(long tagId){
        Connection conn = null;
        Statement st = null;
       long res = -1;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person", null);

            if (rs.next()) {
                String query = "SELECT `tag_class_id`"+
                        " FROM `tag_hasType_tagclass`" +
                        " WHERE `Tag_id` = " + tagId + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    res = result.getLong("tag_class_id");
                }
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return res;
    }

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
            Logger lgr = Logger.getLogger(SQLQuery.class.getName());
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
                Logger lgr = Logger.getLogger(SQLQuery.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
        return subclasses;
    }
}
