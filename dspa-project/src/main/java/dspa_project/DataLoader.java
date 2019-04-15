package dspa_project;

import dspa_project.config.ConfigLoader;
import dspa_project.database.init.MySQLJDBCUtil;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.Person;
import dspa_project.model.PostEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataLoader {

    // Streaming buffers
    private BufferedReader commentBr;
    private BufferedReader likesBr;
    private BufferedReader postsBr;

    // Buffers for static data
    private static BufferedReader personBr;

    private static BufferedReader personsInterestsBr;
    private static BufferedReader tagBr;
    private static BufferedReader tagClassBr;
    private static BufferedReader tagTypeBr;
    private static BufferedReader tagIsSubclassBr;
    private static BufferedReader personKnowsPersonBr;

    private static final String PERSON_TABLE = "person";
    private static final String PERSON_INTEREST_TABLE = "person_hasInterest_tag";
    private static final String TAG_TABLE = "tag";
    private static final String TAG_CLASS_TABLE = "tagclass";
    private static final String TAG_TYPE_TABLE = "tag_hasType_tagclass";
    private static final String TAG_IS_SUBCLASS_TABLE = "tagclass_isSubclassOf_tagclass";
    private static final String PERSON_KNOWS_PERSON_TABLE = "person_knows_person";

    public DataLoader() throws IOException {
        ConfigLoader.load();
        commentBr = new BufferedReader(new FileReader(ConfigLoader.getCommentEvent()));
        likesBr = new BufferedReader(new FileReader(ConfigLoader.getLikeEvent()));
        postsBr = new BufferedReader(new FileReader(ConfigLoader.getPostEvent()));

        personBr = new BufferedReader(new FileReader(ConfigLoader.getPersonPath()));

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

        personBr.readLine();

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

    public static void createAndFillTable(String tableName, String [] attributeNames, String [] attributeTypes, BufferedReader br){
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
                String values = "";

                String query = "";
                boolean counterSet = false;
                while ((nextLineString = br.readLine()) != null) {
                    nextLine = nextLineString.split("\\|");

                    values += ",('";
                    for (int i = 0; i < nextLine.length; i++) {
                        String data = nextLine[i];
                        if (attributeNames[i] == "CREATION_DATE"){
                            // remove T for time
                            data = data.replace("T"," ");
                            // remove Z at the end of string
                            data = data.substring(0, data.length() - 1);
                        }
                        values += data + "','";
                    }
                    values = values.substring(0, values.length()-3);
                    values += "')";

                    query = "INSERT INTO  `static_database`.`" + tableName + "` (";

                    for (int i = 0; i < attributeNames.length; i++) {
                        query += "`" + attributeNames[i] + "`" + " , ";
                    }

                    query = query.substring(0, query.length()-3);
                    query += ") ";
                    if (!counterSet) {
                        values = values.substring(1);
                        counterSet = true;
                    }
                    query += "VALUES " + values + ";";
                }
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
    public static void parseStaticData(){
        parsePersonsInterests();
        parseTags();
        parseTagClasses();
        parseTagTypes();
        parseTagIsSubclasses();
        parsePersonKnowsPerson();
        parsePeople();
    }

    //TODO: maybe don't parse all people and keep them in memory
    public static void parsePeople() {
        String [] attributeNames = {"ID", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATION_DATE", "LOCATION_IP", "BROWSER_USED"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(30)", "VARCHAR(30)", "VARCHAR(30)", "DATETIME", "DATETIME", "VARCHAR(30)", "VARCHAR(30)"};
        createAndFillTable(PERSON_TABLE, attributeNames, attributeTypes, personBr);
    }

    public static void parsePersonsInterests() {
        String [] attributeNames = {"PERSON_ID", "TAG_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(PERSON_INTEREST_TABLE, attributeNames, attributeTypes, personsInterestsBr);
    }

    public static void parseTags() {
        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(100)", "VARCHAR(100)"};
        createAndFillTable(TAG_TABLE, attributeNames, attributeTypes, tagBr);
    }

    public static void parseTagClasses() {
        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(50)", "VARCHAR(50)"};
        createAndFillTable(TAG_CLASS_TABLE, attributeNames, attributeTypes, tagClassBr);
    }

    public static void parseTagTypes() {
        String [] attributeNames = {"TAG_ID", "TAG_CLASS_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(TAG_TYPE_TABLE, attributeNames, attributeTypes, tagTypeBr);
    }

    public static void parseTagIsSubclasses() {
        String [] attributeNames = {"TAG_CLASS_ID", "PARENT_TAG_CLASS_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(TAG_IS_SUBCLASS_TABLE, attributeNames, attributeTypes, tagIsSubclassBr);
    }

    public static void parsePersonKnowsPerson() {
        String [] attributeNames = {"PERSON_ID_A", "PERSON_ID_B"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(PERSON_KNOWS_PERSON_TABLE, attributeNames, attributeTypes, personKnowsPersonBr);
    }
}
