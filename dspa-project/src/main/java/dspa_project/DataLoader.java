package dspa_project;

import dspa_project.config.ConfigLoader;
import dspa_project.database.MySQLJDBCUtil;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataLoader {

    // Streaming buffers
    private BufferedReader commentBr;
    private BufferedReader likesBr;
    private BufferedReader postsBr;

    // Buffers for static data
    private BufferedReader personsInterestsBr;
    private BufferedReader tagBr;
    private BufferedReader tagClassBr;
    private BufferedReader tagTypeBr;
    private BufferedReader tagIsSubclassBr;
    private BufferedReader personKnowsPersonBr;

    private final String PERSON_INTEREST_TABLE = "person_hasInterest_tag";
    private final String TAG_TABLE = "tag";
    private final String TAG_CLASS_TABLE = "tagclass";
    private final String TAG_TYPE_TABLE = "tag_hasType_tagclass";
    private final String TAG_IS_SUBCLASS_TABLE = "tagclass_isSubclassOf_tagclass";
    private final String PERSON_KNOWS_PERSON_TABLE = "person_knows_person";

    public DataLoader() throws IOException {
        ConfigLoader.load();
        commentBr = new BufferedReader(new FileReader(ConfigLoader.getCommentEvent()));
        likesBr = new BufferedReader(new FileReader(ConfigLoader.getLikeEvent()));
        postsBr = new BufferedReader(new FileReader(ConfigLoader.getPostEvent()));

        personsInterestsBr = new BufferedReader(new FileReader(ConfigLoader.getPersonsInterestsPath()));
        tagBr = new BufferedReader(new FileReader(ConfigLoader.getTagPath()));
        tagClassBr = new BufferedReader(new FileReader(ConfigLoader.getTagClassPath()));
        tagTypeBr = new BufferedReader(new FileReader(ConfigLoader.getTagTypePath()));
        tagIsSubclassBr = new BufferedReader(new FileReader(ConfigLoader.getTagIsSubclassPath()));
        personKnowsPersonBr = new BufferedReader(new FileReader(ConfigLoader.getPersonKnowsPersonPath()));

        // skip headers
        commentBr.readLine();
        likesBr.readLine();
        postsBr.readLine();

        personsInterestsBr.readLine();
        tagBr.readLine();
        tagClassBr.readLine();
        tagTypeBr.readLine();
        tagIsSubclassBr.readLine();
        personKnowsPersonBr.readLine();
    }

    public CommentEvent parseComment() throws IOException {
        String line = commentBr.readLine();

        if(line == null)
            return null;

        String [] data = line.split("\\|");
        CommentEvent commentEvent = new CommentEvent(data);
        return commentEvent;
    }

    public LikeEvent parseLike() throws IOException {
        String line = likesBr.readLine();

        if(line == null)
            return null;

        String [] data = line.split("\\|");
        LikeEvent likeEvent = new LikeEvent(data);
        return likeEvent;
    }

    public PostEvent parsePost() throws IOException {
        String line = postsBr.readLine();

        if(line == null)
            return null;

        String [] data = line.split("\\|");
        PostEvent postEvent = new PostEvent(data);
        return postEvent;
    }

    public void createAndFillTable(String tableName, String [] attributeNames, String [] attributeTypes, BufferedReader br){
        Connection conn = null;
        Statement st = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

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

                String values = "(";
                for (int i = 0; i < attributeNames.length; i++) {
                    values += "'-1',";
                }
                values = values.substring(0, values.length()-1);
                values += ")";

                String query = "";
                while ((nextLineString = br.readLine()) != null) {
                    nextLine = nextLineString.split("\\|");

                    values += ",('";
                    for (int i = 0; i < nextLine.length; i++) {
                        values += nextLine[i] + "','";
                    }
                    values = values.substring(0, values.length()-3);
                    values += "')";

                    query = "INSERT INTO  `static_database`.`" + tableName + "` (";

                    for (int i = 0; i < attributeNames.length; i++) {
                        query += "`" + attributeNames[i] + "`" + " , ";
                    }

                    query = query.substring(0, query.length()-3);
                    query += ") ";
                    query += "VALUES " + values + ";";
                }
                st.executeUpdate(query);
                query = "DELETE FROM `" + tableName + "` WHERE `" + attributeNames[0] + "` = '-1'";
                st.executeUpdate(query);
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
    }

    /* STATIC DATA */
    public void parseStaticData(){
        parsePersonsInterests();
        parseTags();
        parseTagClasses();
        parseTagTypes();
        parseTagIsSubclasses();
        parsePersonKnowsPerson();
    }

    public void parsePersonsInterests() {
        String [] attributeNames = {"PERSON_ID", "TAG_ID"};
        String [] attributeTypes = {"INTEGER", "INTEGER"};
        createAndFillTable(PERSON_INTEREST_TABLE, attributeNames, attributeTypes, personsInterestsBr);
    }

    public void parseTags() {
        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"INTEGER", "VARCHAR(100)", "VARCHAR(100)"};
        createAndFillTable(TAG_TABLE, attributeNames, attributeTypes, tagBr);
    }

    public void parseTagClasses() {
        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"INTEGER", "VARCHAR(50)", "VARCHAR(50)"};
        createAndFillTable(TAG_CLASS_TABLE, attributeNames, attributeTypes, tagClassBr);
    }

    public void parseTagTypes() {
        String [] attributeNames = {"TAG_ID", "TAG_CLASS_ID"};
        String [] attributeTypes = {"INTEGER", "INTEGER"};
        createAndFillTable(TAG_TYPE_TABLE, attributeNames, attributeTypes, tagTypeBr);
    }

    public void parseTagIsSubclasses() {
        String [] attributeNames = {"TAG_CLASS_ID", "PARENT_TAG_CLASS_ID"};
        String [] attributeTypes = {"INTEGER", "INTEGER"};
        createAndFillTable(TAG_IS_SUBCLASS_TABLE, attributeNames, attributeTypes, tagIsSubclassBr);
    }

    public void parsePersonKnowsPerson() {
        String [] attributeNames = {"PERSON_ID_A", "PERSON_ID_B"};
        String [] attributeTypes = {"INTEGER", "INTEGER"};
        createAndFillTable(PERSON_KNOWS_PERSON_TABLE, attributeNames, attributeTypes, personKnowsPersonBr);
    }
}
