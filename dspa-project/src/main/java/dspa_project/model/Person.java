package dspa_project.model;

import dspa_project.database.queries.SQLQuery;

import java.util.ArrayList;

public class Person {
    private long id;
    private ArrayList<Long> tagsOfInterest;
    private ArrayList<Long> workAt;
    private ArrayList<Long> studyAt;
    private ArrayList<String> speaksLanguage;
    private long location;

    public Person(long id) {
        this.id = id;
        this.tagsOfInterest = SQLQuery.getTagsOfInterest(id);
        this.workAt = SQLQuery.getWorkAt(id);
        this.studyAt = SQLQuery.getUniversity(id);
        this.speaksLanguage = SQLQuery.getLanguage(id);
        this.location = SQLQuery.getLocation(id);
    }

    public long getId() {
        return id;
    }

    public ArrayList<Long> getTagsOfInterest() {
        return tagsOfInterest;
    }

    public ArrayList<Long> getWorkAt() {
        return workAt;
    }

    public ArrayList<Long> getStudyAt() {
        return studyAt;
    }

    public ArrayList<String> getSpeaksLanguage() {
        return speaksLanguage;
    }

    public long getLocation() {
        return location;
    }
}

