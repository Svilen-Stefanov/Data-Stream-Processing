package dspa_project.model;

import java.util.Date;

public class Person {
    public enum Gender{
        FEMALE,
        MALE
    }

    public enum Browser{
        FIREFOX,
        CHROME,
        SAFARI,
        INTERNET_EXPLORER,
        OPERA
    }

    private long id;
    private String firstName;
    private String lastName;
    private String gender; // as Gender?
    private Date birthday;
    private Date creationDate;
    private String locationIP;
    private String browserUsed;

    public Person(long id, String firstName, String lastName, String gender, Date birthday, Date creationDate, String locationIP, String browserUsed) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.gender = gender;
        this.birthday = birthday;
        this.creationDate = creationDate;
        this.locationIP = locationIP;
        this.browserUsed = browserUsed;
    }

    public long getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getGender() {
        return gender;
    }

    public Date getBirthday() {
        return birthday;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public String getLocationIP() {
        return locationIP;
    }

    public String getBrowserUsed() {
        return browserUsed;
    }
}
