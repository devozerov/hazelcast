package com.hazelcast.projectx;

import java.io.Serializable;
import java.util.Objects;

public class SerializablePerson implements Serializable {
    private long id;
    private long departmentId;
    private String firstName;
    private String lastName;

    public SerializablePerson() {
        // No-op.
    }

    public SerializablePerson(long id, long departmentId, String firstName, String lastName) {
        this.id = id;
        this.departmentId = departmentId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public long getId() {
        return id;
    }

    public long getDepartmentId() {
        return departmentId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }
}
