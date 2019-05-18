package dspa_project.database.queries;

import dspa_project.config.DataLoader;
import dspa_project.database.init.MySQLJDBCUtil;
import dspa_project.tasks.task2.RecommenderSystem;

import java.io.BufferedReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLQuery {
    enum QUERY_RESULT_TYPES {
        BIGINT,
        INT,
        STRING
    }

    public static boolean updateEngladParentLocation(){
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
                String query = "UPDATE place_isPartOf_place" +
                                " SET PLACE_ID_B = 103" +
                                " WHERE PLACE_ID_A = 28 and PLACE_ID_B = 5172;";

                PreparedStatement stmt = conn.prepareStatement(query);

                int affectedRows = stmt.executeUpdate(query);
                if (affectedRows == 1)
                    return true;

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
        return false;
    }

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

    public static ArrayList<String> getLanguage(long value){
        Connection conn = null;
        Statement st = null;
        ArrayList<String> subclasses = new ArrayList<>();
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            // QUERY
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "speaks_language", null);

            if (rs.next()) {
                String query = "SELECT LANGUAGE" +
                    " FROM speaks_language" +
                    " WHERE person_id = " + value + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    subclasses.add(result.getString("LANGUAGE"));
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

    public static long getLocation(long value){
        Connection conn = null;
        Statement st = null;
        long res = 0;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            // QUERY
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "person_isLocatedIn_place", null);

            if (rs.next()) {
                String query = "SELECT PLACE_ID" +
                    " FROM person_isLocatedIn_place" +
                    " WHERE person_id = " + value + ";";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);
                while(result.next()) {
                    res = result.getLong("PLACE_ID");
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

    private static ResultSet getQueryResult(String query, String tableName){
        Connection conn = null;
        Statement st = null;
        ResultSet result = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, tableName, null);

            if (rs.next()) {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
            }
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
        return result;
    }

    public static long getRootLocation(long value){
        // TODO: check only for posts and comments (likes have no locationID)
        Connection conn = null;
        Statement st = null;
        long res = value;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            // QUERY
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "place_isPartOf_place", null);

            if (rs.next()) {
                boolean resultExists = true;
                ResultSet result = null;
                while(resultExists) {
                    String query = "SELECT PLACE_ID_B" +
                            " FROM place_isPartOf_place" +
                            " WHERE place_id_a = " + res + ";";

                    Statement stmt = conn.createStatement();

                    result = stmt.executeQuery(query);
                    if (result.next())
                    {
                        res = result.getLong("PLACE_ID_B");
                    }
                    else
                    {
                        resultExists = false;
                    }
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

    public static void createStaticSimilarityTable(float [][] staticSimilarity){
        Connection conn = null;
        try
        {
            String tableName = "static_similarity";
            String [] attributeNames = {"PERSON_ID_A", "PERSON_ID_B", "SIMILARITY"};
            String [] attributeTypes = {"BIGINT", "BIGINT", "FLOAT"};
            conn = MySQLJDBCUtil.getConnection();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, tableName, null);

            if (!rs.next()) {
                String sqlCreate = "CREATE TABLE IF NOT EXISTS " + tableName + "(";

                for (int i = 0; i < attributeNames.length; i++) {
                    sqlCreate += attributeNames[i] + " " + attributeTypes[i] + ",";
                }

                sqlCreate = sqlCreate.substring(0, sqlCreate.length()-1);
                sqlCreate += ")";

                Statement stmt = conn.createStatement();
                stmt.execute(sqlCreate);

                String values_str = "";
                ArrayList<String> values = new ArrayList<>();

                int curBatchIndex = 0;
                int batch_size = 1000;
                for (int i = 0; i < staticSimilarity.length; i++) {
                    for (int j = 0; j < staticSimilarity[0].length; j++) {
                        values_str += ",(";

                        String userA = String.valueOf(RecommenderSystem.SELECTED_USERS[i]);
                        values_str += "?,";
                        values.add(userA);

                        String userB = String.valueOf(j);
                        values_str += "?,";
                        values.add(userB);

                        String similarity = String.format("%.4f", staticSimilarity[i][j]);
                        values_str += "?";
                        values.add(similarity);

                        values_str += ")";

                        if (curBatchIndex % batch_size == batch_size - 1) {
                            try {
                                insertIntoMysqlTable(conn, attributeNames, values, values_str, tableName);
                            } catch (Exception e){
                                e.printStackTrace();
                            }
                            values_str = "";
                            values.clear();
                        }

                        curBatchIndex++;
                    }
                }

                if (values.size() != 0)
                    insertIntoMysqlTable(conn, attributeNames, values, values_str, tableName);
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(DataLoader.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch(Exception e){
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(DataLoader.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

    public static float [][] getStaticSimilarity(){
        Connection conn = null;
        Statement st = null;
        float [][] possibleFriendsMap = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            // QUERY
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "static_similarity", null);

            if (rs.next()) {
                int numberOfUsers = SQLQuery.getNumberOfPeople();
                possibleFriendsMap = new float[RecommenderSystem.SELECTED_USERS.length][numberOfUsers];
                String query = "SELECT *" +
                        " FROM static_similarity;";

                Statement stmt = conn.createStatement();

                ResultSet result = stmt.executeQuery(query);

                for (int i = 0; i < RecommenderSystem.SELECTED_USERS.length; i++) {
                    for (int j = 0; j < numberOfUsers; j++) {
                        result.next();
                        // selected user
                        int userA = RecommenderSystem.ID_TO_IDX.get((int)result.getLong("PERSON_ID_A"));
                        // one of all users
                        int userB = (int)result.getLong("PERSON_ID_B");
                        possibleFriendsMap[userA][userB] = result.getFloat("SIMILARITY");
                    }
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

        return possibleFriendsMap;
    }

    public static ArrayList<Long> getLocationTree(long value){
        // TODO: check only for posts and comments (likes have no locationID)
        Connection conn = null;
        Statement st = null;
        long res = value;
        ArrayList<Long> queryResult = new ArrayList<>();
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            // QUERY
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, "place_isPartOf_place", null);

            if (rs.next()) {
                boolean resultExists = true;
                ResultSet result = null;
                while(resultExists) {
                    String query = "SELECT PLACE_ID_B" +
                            " FROM place_isPartOf_place" +
                            " WHERE place_id_a = " + res + ";";

                    Statement stmt = conn.createStatement();

                    result = stmt.executeQuery(query);
                    if (result.next())
                    {
                        res = result.getLong("PLACE_ID_B");
                        queryResult.add(res);
                    }
                    else
                    {
                        resultExists = false;
                    }
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

        return queryResult;
    }

    private static void insertIntoMysqlTable( Connection conn, String [] attributeNames, ArrayList<String> values, String values_string, String tableName ) throws SQLException {
        String query = "INSERT INTO  `static_database`.`" + tableName + "` (";

        for (int i = 0; i < attributeNames.length; i++) {
            query += "`" + attributeNames[i] + "`" + " , ";
        }

        query = query.substring(0, query.length() - 3);
        query += ") ";
        values_string = values_string.substring(1);

        query += "VALUES " + values_string + ";";

        PreparedStatement ps = conn.prepareStatement(query);
        for (int i = 0; i < values.size(); i++) {
            ps.setString(i + 1, values.get(i));
        }
        ps.executeUpdate();
        ps.close();
    }

    public static void resetTables(){
        Connection conn = null;
        Statement st = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            String query = "drop database static_database;";
            String query2 ="create database static_database;";

            st.executeUpdate(query);
            st.executeUpdate(query2);
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
    }

    public static void createAndFillTable(String tableName, String [] attributeNames, String [] attributeTypes, BufferedReader br){
        Connection conn = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, tableName, null);

            if (!rs.next()) {
                String sqlCreate = "CREATE TABLE IF NOT EXISTS " + tableName + "(";

                for (int i = 0; i < attributeNames.length; i++) {
                    sqlCreate += attributeNames[i] + " " + attributeTypes[i] + ",";
                }

                sqlCreate = sqlCreate.substring(0, sqlCreate.length()-1);
                sqlCreate += ")";

                Statement stmt = conn.createStatement();
                stmt.execute(sqlCreate);

                String[] nextLine;
                String nextLineString;
                String values_str = "";
                ArrayList values = new ArrayList<String>();

                int curBatchIndex = 0;
                int batch_size = 1000;
                while ((nextLineString = br.readLine()) != null) {
                    nextLine = nextLineString.split("\\|");

                    values_str += ",(";

                    for (int i = 0; i < nextLine.length; i++) {
                        String data = nextLine[i];
                        if (attributeNames[i] == "CREATION_DATE"){
                            // remove T for time
                            data = data.replace("T"," ");
                            // remove Z at the end of string
                            data = data.substring(0, data.length() - 1);
                        }
                        values_str += "?,";
                        values.add(data);
                    }
                    values_str = values_str.substring(0, values_str.length()-1);
                    values_str += ")";

                    if (curBatchIndex % batch_size == batch_size - 1) {
                        try {
                            insertIntoMysqlTable(conn, attributeNames, values, values_str, tableName);
                        } catch (Exception e){
                            e.printStackTrace();
                        }
                        values_str = "";
                        values.clear();
                    }

                    curBatchIndex++;
                }

                if (values.size() != 0)
                    insertIntoMysqlTable(conn, attributeNames, values, values_str, tableName);
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(DataLoader.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch(Exception e){
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(DataLoader.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

}
