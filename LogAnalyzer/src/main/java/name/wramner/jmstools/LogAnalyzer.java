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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Import log files from producers and consumers into a database for analysis, either with a generated HTML report or
 * interactively from a SQL command prompt.
 *
 * @author Erik Wramner
 */
public class LogAnalyzer {
    private static final int MAX_LOST_MESSAGES_IN_REPORT = 50;
    private static final int MAX_DUPLICATE_MESSAGES_IN_REPORT = 50;
    private static final int MAX_GHOST_MESSAGES_IN_REPORT = 50;
    private static final int MAX_ALIEN_MESSAGES_IN_REPORT = 50;
    private static final int MAX_UNDEAD_MESSAGES_IN_REPORT = 50;

    /**
     * Program entry point.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        new LogAnalyzer().run(args);
    }

    /**
     * Parse command line, optionally create database schema, import log files and generate a report or provide an
     * interactive SQL command line.
     *
     * @param args The command line arguments.
     */
    public void run(String[] args) {
        Configuration config = new Configuration();
        if (parseCommandLine(args, config)) {
            Connection conn = null;
            try {
                Class.forName("org.hsqldb.jdbc.JDBCDriver");
                conn = DriverManager.getConnection(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword());
                createSchema(conn);
                System.out.println("Importing files...");
                importLogFiles(conn, config.getRemainingArguments());
                if (config.isInteractive()) {
                    openCommandPrompt(conn);
                } else {
                    // Quick and dirty, but it works
                    System.out.println("Reading template...");
                    String report = readReportTemplate();
                    System.out.println("Generating lost messages...");
                    report = report.replace("{lostMessageReport}", generateLostMessageReport(conn));
                    System.out.println("Generating duplicate messages...");
                    report = report.replace("{duplicateMessageReport}", generateDuplicateMessageReport(conn));
                    System.out.println("Generating ghost messages...");
                    report = report.replace("{ghostMessageReport}", generateGhostMessageReport(conn));
                    System.out.println("Generating alien messages...");
                    report = report.replace("{alienMessageReport}", generateAlienMessageReport(conn));
                    System.out.println("Generating undead messages...");
                    report = report.replace("{undeadMessageReport}", generateUndeadMessageReport(conn));
                    System.out.println("Generating produced per minute table...");
                    report = report.replace("{producedPerMinuteTable}", generateProducedPerMinuteTable(conn));
                    System.out.println("Generating consumed per minute table...");
                    report = report.replace("{consumedPerMinuteTable}", generateConsumedPerMinuteTable(conn));
                    System.out.println("Generating flight time table...");
                    report = report.replace("{flightTimeTable}", generateFlightTimeTable(conn));
                    System.out.println("Writing report...");
                    writeReport(config.getReportFile(), report);
                    System.out.println("Done!");
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            } finally {
                closeSafely(conn);
            }
        }
    }

    private void writeReport(File reportFile, String report) throws IOException, FileNotFoundException {
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(reportFile))) {
            os.write(report.getBytes(Charset.defaultCharset()));
            os.flush();
        }
    }

    private String generateLostMessageReport(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select application_id from lost_messages")) {
            if (rs.next()) {
                List<String> ids = new ArrayList<>();
                do {
                    ids.add(rs.getString(1));
                } while (rs.next());
                StringBuilder sb = new StringBuilder();
                sb.append("<p>A total of <b>").append(ids.size()).append("</b> message(s) have been <b>lost</b>: ");
                for (int i = 0; i < ids.size() && i < MAX_LOST_MESSAGES_IN_REPORT; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(ids.get(i));
                }
                sb.append(".</p>");
                if (ids.size() > MAX_LOST_MESSAGES_IN_REPORT) {
                    sb.append("<p>Only some messages included, use interactive mode for details.</p>");
                }
                sb.append("<p>Check the dead letter queue!</p>");
                return sb.toString();
            } else {
                return "<p>No lost messages.</p>";
            }
        }
    }

    private String generateDuplicateMessageReport(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select application_id from duplicate_messages")) {
            if (rs.next()) {
                List<String> ids = new ArrayList<>();
                do {
                    ids.add(rs.getString(1));
                } while (rs.next());
                StringBuilder sb = new StringBuilder();
                sb.append("<p>A total of <b>").append(ids.size()).append("</b> message(s) are duplicates: ");
                for (int i = 0; i < ids.size() && i < MAX_DUPLICATE_MESSAGES_IN_REPORT; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(ids.get(i));
                }
                sb.append(".</p>");
                if (ids.size() > MAX_DUPLICATE_MESSAGES_IN_REPORT) {
                    sb.append("<p>Only some messages included, use interactive mode for details.</p>");
                }
                return sb.toString();
            } else {
                return "<p>No duplicate messages.</p>";
            }
        }
    }

    private String generateGhostMessageReport(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select * from ghost_messages")) {
            if (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("<p>The messages below were sent, but rolled back. They should not have been delivered. They are ghosts.</p>");
                sb.append("<table><thead><tr><th>JMSID</th><th>Application id</th><th>Consumed</th></thead><tbody>");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                int count = 0;
                do {
                    sb.append("<tr>");
                    sb.append("<td>").append(rs.getString("jms_id")).append("</td>");
                    sb.append("<td>").append(rs.getString("application_id")).append("</td>");
                    sb.append("<td>").append(df.format(rs.getTimestamp("consumed_time"))).append("</td>");
                    sb.append("</tr>");
                    if (++count > MAX_GHOST_MESSAGES_IN_REPORT) {
                        sb.append("<tr><td colspan=\"3\">...more data available, use interactive mode!</td></tr>");
                        break;
                    }
                } while (rs.next());
                sb.append("</tbody></table>");
                return sb.toString();
            } else {
                return "<p>No ghost messages.</p>";
            }
        }
    }

    private String generateUndeadMessageReport(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select * from undead_messages")) {
            if (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("<p>The messages below were never sent (in this test), yet here they are.</p>");
                sb.append("<table><thead><tr><th>JMSID</th><th>Application id</th><th>Consumed</th></thead><tbody>");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                int count = 0;
                do {
                    sb.append("<tr>");
                    sb.append("<td>").append(rs.getString("jms_id")).append("</td>");
                    sb.append("<td>").append(rs.getString("application_id")).append("</td>");
                    sb.append("<td>").append(df.format(rs.getTimestamp("consumed_time"))).append("</td>");
                    sb.append("</tr>");
                    if (++count > MAX_UNDEAD_MESSAGES_IN_REPORT) {
                        sb.append("<tr><td colspan=\"3\">...more data available, use interactive mode!</td></tr>");
                        break;
                    }
                } while (rs.next());
                sb.append("</tbody></table>");
                return sb.toString();
            } else {
                return "<p>No undead messages.</p>";
            }
        }
    }

    private String generateConsumedPerMinuteTable(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select time_period, total_count, total_bytes"
                             + " from consumed_per_minute order by time_period")) {
            if (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("<table><thead><tr><th>Period</th><th>Consumed count</th><th>Consumed bytes</th></thead><tbody>");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                do {
                    sb.append("<tr>");
                    sb.append("<td>").append(df.format(rs.getTimestamp(1))).append("</td>");
                    sb.append("<td align='right'>").append(rs.getInt(2)).append("</td>");
                    sb.append("<td align='right'>").append(rs.getInt(3)).append("</td>");
                    sb.append("</tr>");
                } while (rs.next());
                sb.append("</tbody></table>");
                return sb.toString();
            } else {
                return "<p>No consumed messages.</p>";
            }
        }
    }

    private String generateProducedPerMinuteTable(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select time_period, total_count, total_bytes"
                             + " from produced_per_minute order by time_period")) {
            if (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("<table><thead><tr><th>Period</th><th>Produced count</th><th>Produced bytes</th></thead><tbody>");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                do {
                    sb.append("<tr>");
                    sb.append("<td>").append(df.format(rs.getTimestamp(1))).append("</td>");
                    sb.append("<td align='right'>").append(rs.getInt(2)).append("</td>");
                    sb.append("<td align='right'>").append(rs.getInt(3)).append("</td>");
                    sb.append("</tr>");
                } while (rs.next());
                sb.append("</tbody></table>");
                return sb.toString();
            } else {
                return "<p>No produced messages.</p>";
            }
        }
    }

    private String generateFlightTimeTable(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select trunc(produced_time, 'mi'), flight_time_millis"
                             + " from message_flight_time order by 1")) {
            if (rs.next()) {
                StringBuilder sb = new StringBuilder();
                sb.append("<table><thead><tr><th>Time</th><th>Messages</th><th>Min</th><th>Max</th><th>95 percentile</th><th>98 percentile</th></thead><tbody>");
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                Timestamp lastTime = rs.getTimestamp(1);
                List<Integer> flightTimes = new ArrayList<Integer>();
                do {
                    Timestamp time = rs.getTimestamp(1);
                    int flightTimeMillis = rs.getInt(2);
                    if (!time.equals(lastTime)) {
                        appendFlightTimeSummaryForMinute(sb, df, lastTime, flightTimes);
                        lastTime = time;
                    } else {
                        flightTimes.add(Integer.valueOf(flightTimeMillis));
                    }
                } while (rs.next());
                appendFlightTimeSummaryForMinute(sb, df, lastTime, flightTimes);
                sb.append("</tbody></table>");
                return sb.toString();
            } else {
                return "<p>No message flight times recorded.</p>";
            }
        }
    }

    private void appendFlightTimeSummaryForMinute(StringBuilder sb, SimpleDateFormat df, Timestamp time,
                    List<Integer> flightTimes) {
        if (!flightTimes.isEmpty()) {
            Collections.sort(flightTimes);
            int numberOfMeasurements = flightTimes.size();
            int percentile95Index = (numberOfMeasurements * 95 / 100);
            int percentile98Index = (numberOfMeasurements * 98 / 100);
            sb.append("<tr>");
            sb.append("<td>").append(df.format(time)).append("</td>");
            sb.append("<td align='right'>").append(numberOfMeasurements).append("</td>");
            sb.append("<td align='right'>").append(flightTimes.get(0)).append("</td>");
            sb.append("<td align='right'>").append(flightTimes.get(numberOfMeasurements - 1)).append("</td>");
            sb.append("<td align='right'>").append(flightTimes.get(percentile95Index)).append("</td>");
            sb.append("<td align='right'>").append(flightTimes.get(percentile98Index)).append("</td>");
            sb.append("</tr>");
            flightTimes.clear();
        }
    }

    private String generateAlienMessageReport(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement();
             ResultSet rs = stat.executeQuery("select jms_id from alien_messages")) {
            if (rs.next()) {
                List<String> ids = new ArrayList<>();
                do {
                    ids.add(rs.getString(1));
                } while (rs.next());
                StringBuilder sb = new StringBuilder();
                sb.append("<p>A total of <b>").append(ids.size()).append("</b> message(s) are alien: ");
                for (int i = 0; i < ids.size() && i < MAX_ALIEN_MESSAGES_IN_REPORT; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(ids.get(i));
                }
                sb.append(".</p>");
                if (ids.size() > MAX_ALIEN_MESSAGES_IN_REPORT) {
                    sb.append("<p>Only some messages included, use interactive mode for details.</p>");
                }
                sb.append("<p>This may be normal depending on the test.</p>");
                return sb.toString();
            } else {
                return "<p>No alien messages.</p>";
            }
        }
    }

    private void closeSafely(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }
    }

    private void importLogFiles(Connection conn, List<String> fileAndDirectoryPaths) throws IOException, SQLException {
        for (String fileOrDirectoryPath : fileAndDirectoryPaths) {
            File fileOrDirectory = new File(fileOrDirectoryPath);
            if (fileOrDirectory.isDirectory()) {
                importFilesInDirectory(conn, fileOrDirectory);
            } else if (fileOrDirectory.isFile()) {
                importFile(conn, fileOrDirectory);
            } else {
                System.err.println("Unexpected argument '" + fileOrDirectoryPath + "' - not a file or directory!");
            }
        }
    }

    /**
     * Parse the command line into the specified configuration.
     *
     * @param args The command line.
     * @param config The configuration class.
     * @return true if successful, false on errors such as missing arguments.
     */
    private boolean parseCommandLine(String[] args, Configuration config) {
        CmdLineParser parser = new CmdLineParser(config);
        try {
            parser.parseArgument(args);
            if (config.getRemainingArguments() == null || config.getRemainingArguments().isEmpty()) {
                printUsage(parser);
                return false;
            } else {
                return true;
            }
        } catch (CmdLineException e) {
            printUsage(parser);
            System.out.println("Error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Print usage. All supported options are listed.
     *
     * @param parser The parser.
     */
    private void printUsage(CmdLineParser parser) {
        System.out.println("Usage: java " + getClass().getName() + " [options] [files/directories]");
        System.out.println();
        System.out.println("Where the options are:");
        parser.printUsage(System.out);
        System.out.println();
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
                        + " (outcome, outcome_time, produced_time, application_id, payload_size"
                        + ", delay_seconds, jms_id)"
                        + " values (?, ?, ?, ?, ?, ?, ?)");
             BufferedReader reader = new BufferedReader(
                             new InputStreamReader(new FileInputStream(file), Charset.defaultCharset()))) {
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
                String applicationId = fields[field++];
                if (applicationId != null && applicationId.length() > 0) {
                    stat.setString(pos++, applicationId);
                } else {
                    stat.setNull(pos++, Types.VARCHAR);
                }
                String payloadSizeString = fields[field++];
                if (payloadSizeString != null && payloadSizeString.length() > 0) {
                    stat.setInt(pos++, Integer.parseInt(payloadSizeString));
                } else {
                    stat.setNull(pos++, Types.INTEGER);
                }
                stat.setInt(pos++, Integer.parseInt(fields[field++]));
                stat.setString(pos++, fields[field++]);
                stat.executeUpdate();
            }
            conn.commit();
        }
    }

    private void importDequeuedFile(Connection conn, File file) throws IOException, SQLException {
        try (PreparedStatement stat = conn.prepareStatement("insert into consumed_messages"
                        + " (outcome, outcome_time, consumed_time, jms_id, application_id, payload_size)"
                        + " values (?, ?, ?, ?, ?, ?)");
             BufferedReader reader = new BufferedReader(
                             new InputStreamReader(new FileInputStream(file), Charset.defaultCharset()))) {
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
                String applicationId = fields[field++];
                if (applicationId != null && applicationId.length() > 0) {
                    stat.setString(pos++, applicationId);
                } else {
                    stat.setNull(pos++, Types.VARCHAR);
                }
                String payloadSizeString = fields[field++];
                if (payloadSizeString != null && payloadSizeString.length() > 0) {
                    stat.setInt(pos++, Integer.parseInt(payloadSizeString));
                } else {
                    stat.setNull(pos++, Types.INTEGER);
                }
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
        // Note that the character set should match the encoding in pom.xml
        Charset charset = Charset.forName("Cp1252");
        SqlFile sqlFile = new SqlFile(
                        new InputStreamReader(LogAnalyzer.class.getResourceAsStream("/create_schema.sql"), charset),
                        "create schema", null, null, false, null);
        sqlFile.setConnection(conn);
        sqlFile.setAutoClose(true);
        sqlFile.execute();
    }

    private String readReportTemplate() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/report_template.html")) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int c = is.read(); c != -1; c = is.read()) {
                bos.write(c);
            }
            return bos.toString();
        }
    }

    private static class Configuration {
        private static final String DEFAULT_JDBC_URL = "jdbc:hsqldb:mem:jmstoolsdb";
        private static final String DEFAULT_JDBC_USER = "sa";
        private static final String DEFAULT_JDBC_PASSWORD = "";
        private static final String DEFAULT_REPORT_FILE = "report.html";

        @Option(name = "-i", aliases = "--interactive", usage = "Open a SQL prompt for custom queries")
        private boolean _interactive;

        @Option(name = "-url", aliases = "--jdbc-url", usage = "JDBC URL for HSQLDB database")
        private String _jdbcUrl = DEFAULT_JDBC_URL;

        @Option(name = "-user", aliases = { "--jdbc-user" }, usage = "JDBC user for HSQLDB database connection")
        private String _jdbcUser = DEFAULT_JDBC_USER;

        @Option(name = "-pw", aliases = { "--jdbc-password" }, usage = "JDBC password for HSQLDB database connection")
        private String _jdbcPassword = DEFAULT_JDBC_PASSWORD;

        @Option(name = "-o", aliases = { "--output-file" }, usage = "Path and file name for report")
        private File _reportFile = new File(DEFAULT_REPORT_FILE);

        @Argument
        private List<String> _args = new ArrayList<String>();

        public boolean isInteractive() {
            return _interactive;
        }

        public String getJdbcUrl() {
            return _jdbcUrl;
        }

        public String getJdbcUser() {
            return _jdbcUser;
        }

        public String getJdbcPassword() {
            return _jdbcPassword;
        }

        public List<String> getRemainingArguments() {
            return _args;
        }

        public File getReportFile() {
            return _reportFile;
        }
    }
}
