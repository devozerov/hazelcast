package com.hazelcast.projectx;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortablePerson implements Portable {
    private long id;
    private long departmentId;
    private String firstName;
    private String lastName;

    public PortablePerson() {
        // No-op.
    }

    public PortablePerson(long id, long departmentId, String firstName, String lastName) {
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
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 2;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("id", id);
        writer.writeLong("departmentId", departmentId);
        writer.writeUTF("firstName", firstName);
        writer.writeUTF("lastName", lastName);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        id = reader.readLong("id");
        departmentId = reader.readLong("departmentId");
        firstName = reader.readUTF("firstName");
        lastName = reader.readUTF("lastName");
    }
}
