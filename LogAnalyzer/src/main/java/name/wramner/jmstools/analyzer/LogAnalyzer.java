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
package name.wramner.jmstools.analyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templateresolver.AbstractConfigurableTemplateResolver;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.templateresolver.FileTemplateResolver;

/**
 * Import log files from producers and consumers into a database for analysis, either with a generated HTML report or
 * interactively from a SQL command prompt.
 *
 * @author Erik Wramner
 */
public class LogAnalyzer {

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
        printVersion();
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
                    System.out.println("Generating Thymeleaf report...");
                    generateThymeleafReport(config, conn);
                    System.out.println("Done!");
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
            } finally {
                closeSafely(conn);
            }
        }
    }

    /**
     * Generate report using the Thymeleaf template engine.
     *
     * @param config The configuration.
     * @param conn The database connection.
     * @throws IOException on read or write errors. @ on database errors.
     */
    private void generateThymeleafReport(Configuration config, Connection conn) throws IOException, SQLException {
        TemplateEngine engine = new TemplateEngine();
        File templateFile = config.getTemplateFile();
        AbstractConfigurableTemplateResolver resolver = templateFile != null ? new FileTemplateResolver()
                        : new ClassLoaderTemplateResolver();
        resolver.setTemplateMode("HTML");
        engine.setTemplateResolver(resolver);
        Context context = new Context();
        DataProvider dataProvider = new DataProvider(conn);
        context.setVariable("dataProvider", dataProvider);
        try (Writer writer = Files.newBufferedWriter(config.getReportFile().toPath(), Charset.forName("UTF-8"))) {
            engine.process(templateFile != null ? templateFile.getPath()
                            : (dataProvider.isCorrectnessTest() ? "correctness_test_report.html"
                                            : "perf_test_report.html"),
                            context, writer);
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
            if (config.getRemainingArguments() == null || config.getRemainingArguments().isEmpty()
                            || config.isHelpRequested()) {
                printUsage(parser);
                return false;
            } else {
                return true;
            }
        } catch (CmdLineException e) {
            printUsage(parser);
            if (!config.isHelpRequested()) {
                System.out.println("Error: " + e.getMessage());
            }
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

    /**
     * Print program name and version.
     */
    private void printVersion() {
        String version = getClass().getPackage().getImplementationVersion();
        System.out.println(getClass().getSimpleName() + " " + (version != null ? version : "(unknown version)"));
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
                        + ", delay_seconds, jms_id)" + " values (?, ?, ?, ?, ?, ?, ?)");
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
                        "create schema", null, null, false, (File) null);
        sqlFile.setConnection(conn);
        sqlFile.setAutoClose(true);
        sqlFile.execute();
    }

    private static class Configuration {
        // Replace mem with file to use a file-based database and perhaps reduce memory footprint
        private static final String DEFAULT_JDBC_URL = "jdbc:hsqldb:mem:jmstoolsdb";
        private static final String DEFAULT_JDBC_USER = "sa";
        private static final String DEFAULT_JDBC_PASSWORD = "";
        private static final String DEFAULT_REPORT_FILE = "report.html";

        @Option(name = "-?", aliases = { "--help", "--options" }, usage = "Print help text with options")
        private boolean _help;

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

        @Option(name = "-t", aliases = { "--template-file" }, usage = "Optional Thymeleaf template file")
        private File _templateFile;

        @Argument
        private List<String> _args = new ArrayList<String>();

        public boolean isHelpRequested() {
            return _help;
        }

        public File getTemplateFile() {
            return _templateFile;
        }

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
