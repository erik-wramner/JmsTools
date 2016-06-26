/*
 * Copyright 2016 Erik Wramner.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package name.wramner.jmstools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;

/**
 * Import log files from producers and consumers into a database for analysis.
 * 
 * @author Erik Wramner
 */
public class LogAnalyzer {

    public static void main(String[] args) {
        new LogAnalyzer().run(args);
    }

    public void run(String[] args) {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:jmstoolsdb", "SA", "");
            createSchema(conn);

            for (String arg : args) {
                File argumentFile = new File(arg);
                if (argumentFile.isDirectory()) {
                    importFilesInDirectory(conn, argumentFile);
                } else if (argumentFile.isFile()) {
                    importFile(conn, argumentFile);

                } else {
                    System.err.println("Unexpected argument '" + arg + "' - not a file or directory!");
                }
            }

            openCommandPrompt(conn);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private void importFilesInDirectory(Connection conn, File directory) throws IOException, SQLException {
        for (File file : directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".log") && (name.startsWith("enqueued_") || name.startsWith("dequeued_"));
            }
        })) {
            importFile(conn, file);
        }
    }

    private void importFile(Connection conn, File file) throws IOException, SQLException {
        if (file.getName().startsWith("enqueued_")) {
            importEnqueuedFile(conn, file);
        } else if (file.getName().startsWith("dequeued_")) {
            importDequeuedFile(conn, file);
        } else {
            throw new IOException("Unknown file type " + file.getName() + "!");
        }
    }

    private void importEnqueuedFile(Connection conn, File file) throws IOException, SQLException {
        try (PreparedStatement stat = conn.prepareStatement("insert into produced_messages"
                        + " (outcome, outcome_time, produced_time, application_id, payload_size, delay_seconds)"
                        + " values (?, ?, ?, ?, ?, ?)");
                        BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // Skip header line
            reader.readLine();
            // Insert lines
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String[] fields = line.split("\t");
                int pos = 1;
                int field = 0;
                stat.setString(pos++, fields[field++]);
                stat.setTimestamp(pos++, new Timestamp(Long.parseLong(fields[field++])));
                stat.setTimestamp(pos++, new Timestamp(Long.parseLong(fields[field++])));
                stat.setString(pos++, fields[field++]);
                stat.setInt(pos++, Integer.parseInt(fields[field++]));
                stat.setInt(pos++, Integer.parseInt(fields[field++]));
                stat.executeUpdate();
            }
            conn.commit();
        }
    }

    private void importDequeuedFile(Connection conn, File file) throws IOException, SQLException {
        try (PreparedStatement stat = conn.prepareStatement("insert into consumed_messages"
                        + " (outcome, outcome_time, consumed_time, jms_id, application_id, payload_size)"
                        + " values (?, ?, ?, ?, ?, ?)");
                        BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // Skip header line
            reader.readLine();
            // Insert lines
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String[] fields = line.split("\t");
                int pos = 1;
                int field = 0;
                stat.setString(pos++, fields[field++]);
                stat.setTimestamp(pos++, new Timestamp(Long.parseLong(fields[field++])));
                stat.setTimestamp(pos++, new Timestamp(Long.parseLong(fields[field++])));
                stat.setString(pos++, fields[field++]);
                stat.setString(pos++, fields[field++]);
                stat.setInt(pos++, Integer.parseInt(fields[field++]));
                stat.executeUpdate();
            }
            conn.commit();
        }
    }

    private void openCommandPrompt(Connection conn) throws IOException, SqlToolError, SQLException {
        SqlFile sqlFile = new SqlFile(null, true);
        sqlFile.setConnection(conn);
        sqlFile.execute();
    }

    private void createSchema(Connection conn) throws IOException, SqlToolError, SQLException {
        SqlFile sqlFile = new SqlFile(
                        new InputStreamReader(LogAnalyzer.class.getResourceAsStream("/create_schema.sql")),
                        "create schema", null, null, false, null);
        sqlFile.setConnection(conn);
        sqlFile.setAutoClose(true);
        sqlFile.execute();
    }
}
