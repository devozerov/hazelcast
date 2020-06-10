package com.hazelcast.sql.projectx;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.schema.ExternalCatalog;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collection;

public class Runner {
    public static void main(String[] args) throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(new Config().setProperty("hazelcast.logging.type", "none"));

        System.out.println();
        print("Application ready (" + instance.getCluster().getMembers().size() + " member(s))");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            String command = reader.readLine();

            processCommand(instance, command);
        }
    }

    private static void processCommand(HazelcastInstance instance, String command) throws Exception {
        if (command.toLowerCase().equals("quit")) {
            System.exit(-1);
        } else if (command.toLowerCase().equals("show tables")) {
            processShowTables(instance);
        } else {
            processQuery(instance, command);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void processQuery(HazelcastInstance instance, String query) throws Exception {
        try (SqlCursor cursor = instance.getSqlService().query(query)) {
            for (SqlRow row : cursor) {
                // No-op.
            }

            print("Query executed successfully");
        } catch (Exception e) {
            print("Failed to execute the query: " + e.getMessage());
        }
    }

    private static void processShowTables(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = ((HazelcastInstanceProxy) instance).getOriginal().node.getNodeEngine();

        Collection<Table> tables = new ExternalCatalog(nodeEngine).getTables();

        print(tables.size() + " table(s) found");

        for (Table table : tables) {
            printTable(table);
        }
    }

    private static void printTable(Table table) {
        StringBuilder sb = new StringBuilder();

        sb.append(table.getName()).append(" [");

        for (int i = 0; i < table.getFieldCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }

            TableField field = table.getField(i);

            sb.append(field.getName()).append(" ").append(field.getType().getTypeFamily().name());
        }

        sb.append("]");

        print(sb.toString());
    }

    private static void print(String text) {
        System.out.println(">>> " + text);
        System.out.println();
    }
}
