package com.hazelcast.projectx;

import com.hazelcast.projectx.transportable.Transportable;

import java.util.Objects;

public class TransportablePerson implements Transportable {
    private long id;
    private long departmentId;
    private String firstName;
    private String lastName;

    public TransportablePerson() {
        // No-op.
    }

    public TransportablePerson(long id, long departmentId, String firstName, String lastName) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TransportablePerson that = (TransportablePerson) o;

        return id == that.id &&
            departmentId == that.departmentId &&
            Objects.equals(firstName, that.firstName) &&
            Objects.equals(lastName, that.lastName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, departmentId, firstName, lastName);
    }

    @Override
    public String toString() {
        return "TransportablePerson{id=" + id + ", departmentId=" + departmentId +
               ", firstName=" + firstName + ", lastName=" + lastName + '}';
    }
}
