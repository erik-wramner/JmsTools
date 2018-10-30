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

import java.awt.Color;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
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

    /**
     * This exception wraps a normal SQL exception but is unchecked.
     */
    public static class UncheckedSqlException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public UncheckedSqlException(SQLException cause) {
            super(cause);
        }

        public UncheckedSqlException(String message, SQLException cause) {
            super(message, cause);
        }
    }

    /**
     * Message that has been produced or consumed.
     */
    public static class Message {
        private final String _jmsId;
        private final String _applicationId;
        private final Integer _payloadSize;

        public Message(String jmsId, String applicationId, Integer payloadSize) {
            _jmsId = jmsId;
            _applicationId = applicationId;
            _payloadSize = payloadSize;
        }

        public String getJmsId() {
            return _jmsId;
        }

        public String getApplicationId() {
            return _applicationId;
        }

        public Integer getPayloadSize() {
            return _payloadSize;
        }
    }

    /**
     * Produced message.
     */
    public static class ProducedMessage extends Message {
        private final int _delaySeconds;
        private final Timestamp _publishedTimestamp;

        public ProducedMessage(String jmsId, String applicationId, Integer payloadSize, Timestamp publishedTimestamp,
                        int delaySeconds) {
            super(jmsId, applicationId, payloadSize);
            _delaySeconds = delaySeconds;
            _publishedTimestamp = publishedTimestamp;
        }

        public int getDelaySeconds() {
            return _delaySeconds;
        }

        public Timestamp getPublishedTimestamp() {
            return _publishedTimestamp;
        }
    }

    /**
     * Consumed message.
     */
    public static class ConsumedMessage extends Message {
        private final Timestamp _consumedTimestamp;

        public ConsumedMessage(String jmsId, String applicationId, Integer payloadSize, Timestamp consumedTimestamp) {
            super(jmsId, applicationId, payloadSize);
            _consumedTimestamp = consumedTimestamp;
        }

        public Timestamp getConsumedTimestamp() {
            return _consumedTimestamp;
        }
    }

    /**
     * Metrics for a period (a minute).
     */
    public static class PeriodMetrics {
        private final Timestamp periodStart;
        private final int _producedCount;
        private final int _consumedCount;
        private final int _producedBytes;
        private final int _consumedBytes;
        private final int _maxProducedSize;
        private final int _maxConsumedSize;
        private final int _medianProducedSize;
        private final int _medianConsumedSize;

        public PeriodMetrics(Timestamp periodStart, int producedCount, int consumedCount, int producedBytes,
                        int consumedBytes, int maxProducedSize, int maxConsumedSize, int medianProducedSize,
                        int medianConsumedSize) {
            this.periodStart = periodStart;
            _producedCount = producedCount;
            _consumedCount = consumedCount;
            _producedBytes = producedBytes;
            _consumedBytes = consumedBytes;
            _maxProducedSize = maxProducedSize;
            _maxConsumedSize = maxConsumedSize;
            _medianProducedSize = medianProducedSize;
            _medianConsumedSize = medianConsumedSize;
        }

        public Timestamp getPeriodStart() {
            return periodStart;
        }

        public int getTotal() {
            return getProduced() + getConsumed();
        }

        public int getProduced() {
            return _producedCount;
        }

        public int getConsumed() {
            return _consumedCount;
        }

        public int getTotalBytes() {
            return getProducedBytes() + getConsumedBytes();
        }

        public int getProducedBytes() {
            return _producedBytes;
        }

        public int getConsumedBytes() {
            return _consumedBytes;
        }

        public int getMaxProducedSize() {
            return _maxProducedSize;
        }

        public int getMaxConsumedSize() {
            return _maxConsumedSize;
        }

        public int getMedianProducedSize() {
            return _medianProducedSize;
        }

        public int getMedianConsumedSize() {
            return _medianConsumedSize;
        }
    }

    /**
     * Flight time metrics for a period (minute).
     */
    public static class FlightTimeMetrics {
        private final Timestamp _period;
        private final int _count;
        private final int _min;
        private final int _max;
        private final int _median;
        private final int _percentile95;

        public FlightTimeMetrics(Timestamp period, int count, int min, int max, int median, int percentile95) {
            _period = period;
            _count = count;
            _min = min;
            _max = max;
            _median = median;
            _percentile95 = percentile95;
        }

        public Timestamp getPeriod() {
            return _period;
        }

        public int getCount() {
            return _count;
        }

        public int getMin() {
            return _min;
        }

        public int getMax() {
            return _max;
        }

        public int getMedian() {
            return _median;
        }

        public int getPercentile95() {
            return _percentile95;
        }
    }

    /**
     * This class provides data for the Thymeleaf reports.
     */
    public static class DataProvider {
        private final Connection _conn;
        private final Timestamp _startTime;
        private final Timestamp _endTime;
        private final int _consumedMessageCount;
        private final int _producedMessageCount;
        private final int _lostMessageCount;
        private final int _duplicateMessageCount;
        private final int _ghostMessageCount;
        private final int _alienMessageCount;
        private final int _undeadMessageCount;
        private final int _delayedMessageCount;
        private List<FlightTimeMetrics> _flightTimeMetrics;

        /**
         * Constructor.
         *
         * @param conn The database connection.
         */
        public DataProvider(Connection conn) {
            _conn = conn;
            _startTime = findStartTime();
            _endTime = findEndTime();
            _producedMessageCount = findSimpleCount("produced_messages");
            _consumedMessageCount = findSimpleCount("consumed_messages");
            _lostMessageCount = findSimpleCount("lost_messages");
            _duplicateMessageCount = findSimpleCount("duplicate_messages");
            _ghostMessageCount = findSimpleCount("ghost_messages");
            _alienMessageCount = findSimpleCount("alien_messages");
            _undeadMessageCount = findSimpleCount("undead_messages");
            _delayedMessageCount = findWithIntResult("select count(*) from produced_messages where delay_seconds > 0");
        }

        public Timestamp getStartTime() {
            return _startTime;
        }

        public Timestamp getEndTime() {
            return _endTime;
        }

        public int getTotalMessageCount() {
            return getConsumedMessageCount() + getProducedMessageCount();
        }

        public int getConsumedMessageCount() {
            return _consumedMessageCount;
        }

        public int getProducedMessageCount() {
            return _producedMessageCount;
        }

        public int getLostMessageCount() {
            return _lostMessageCount;
        }

        public int getDuplicateMessageCount() {
            return _duplicateMessageCount;
        }

        public int getGhostMessageCount() {
            return _ghostMessageCount;
        }

        public int getAlienMessageCount() {
            return _alienMessageCount;
        }

        public int getUndeadMessageCount() {
            return _undeadMessageCount;
        }

        public int getDelayedMessageCount() {
            return _delayedMessageCount;
        }

        public double getDelayedMessagePercentage() {
            return getProducedMessageCount() > 0 ? (100.0 * getDelayedMessageCount()) / getProducedMessageCount() : 0;
        }

        public boolean isFlightTimeDataAvailable() {
            return getProducedMessageCount() > 0 && getConsumedMessageCount() > 0 && isCorrectnessTest();
        }

        public List<FlightTimeMetrics> getFlightTimeMetrics() {
            if (_flightTimeMetrics != null) {
                return _flightTimeMetrics;
            }
            List<FlightTimeMetrics> list = new ArrayList<>();
            try (Statement stat = _conn.createStatement();
                 ResultSet rs = stat.executeQuery("select trunc(produced_time, 'mi'), flight_time_millis"
                                 + " from message_flight_time order by 1")) {
                if (rs.next()) {
                    Timestamp lastTime = rs.getTimestamp(1);
                    MutableIntList flightTimes = IntLists.mutable.empty();
                    do {
                        Timestamp time = rs.getTimestamp(1);
                        int flightTimeMillis = rs.getInt(2);
                        if (!time.equals(lastTime)) {
                            list.add(computeFlightTimeMetrics(lastTime, flightTimes));
                            flightTimes.clear();
                            lastTime = time;
                        }
                        flightTimes.add(flightTimeMillis);
                    } while (rs.next());
                    if (!flightTimes.isEmpty()) {
                        list.add(computeFlightTimeMetrics(lastTime, flightTimes));
                    }
                }
                return (_flightTimeMetrics = list);
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }

        public String getBase64MessagesPerMinuteImage() {
            TimeSeries timeSeriesConsumed = new TimeSeries("Consumed");
            TimeSeries timeSeriesProduced = new TimeSeries("Produced");
            TimeSeries timeSeriesTotal = new TimeSeries("Total");
            for (PeriodMetrics m : getMessagesPerMinute()) {
                Minute minute = new Minute(m.getPeriodStart());
                timeSeriesConsumed.add(minute, m.getConsumed());
                timeSeriesProduced.add(minute, m.getProduced());
                timeSeriesTotal.add(minute, m.getConsumed() + m.getProduced());
            }
            TimeSeriesCollection timeSeriesCollection = new TimeSeriesCollection(timeSeriesConsumed);
            timeSeriesCollection.addSeries(timeSeriesProduced);
            timeSeriesCollection.addSeries(timeSeriesTotal);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                JFreeChart chart = ChartFactory.createTimeSeriesChart("Messages per minute", "Time", "Messages",
                                timeSeriesCollection);
                chart.getPlot().setBackgroundPaint(Color.WHITE);
                ChartUtilities.writeChartAsPNG(bos, chart, 1024, 500);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return "data:image/png;base64," + Base64.getEncoder().encodeToString(bos.toByteArray());
        }

        public String getBase64BytesPerMinuteImage() {
            TimeSeries timeSeriesConsumed = new TimeSeries("Consumed");
            TimeSeries timeSeriesProduced = new TimeSeries("Produced");
            TimeSeries timeSeriesTotal = new TimeSeries("Total");
            for (PeriodMetrics m : getMessagesPerMinute()) {
                Minute minute = new Minute(m.getPeriodStart());
                timeSeriesConsumed.add(minute, m.getConsumedBytes() / 1024);
                timeSeriesProduced.add(minute, m.getProducedBytes() / 1024);
                timeSeriesTotal.add(minute, m.getTotalBytes() / 1024);
            }
            TimeSeriesCollection timeSeriesCollection = new TimeSeriesCollection(timeSeriesConsumed);
            timeSeriesCollection.addSeries(timeSeriesProduced);
            timeSeriesCollection.addSeries(timeSeriesTotal);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                JFreeChart chart = ChartFactory.createTimeSeriesChart("Kilobytes per minute", "Time", "Bytes (k)",
                                timeSeriesCollection);
                chart.getPlot().setBackgroundPaint(Color.WHITE);
                ChartUtilities.writeChartAsPNG(bos, chart, 1024, 500);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return "data:image/png;base64," + Base64.getEncoder().encodeToString(bos.toByteArray());
        }

        public String getBase64EncodedFlightTimeMetricsImage() {
            TimeSeries timeSeries50p = new TimeSeries("Median");
            TimeSeries timeSeries95p = new TimeSeries("95 percentile");
            TimeSeries timeSeriesMax = new TimeSeries("Max");
            for (FlightTimeMetrics m : getFlightTimeMetrics()) {
                Minute minute = new Minute(m.getPeriod());
                timeSeries50p.add(minute, m.getMedian());
                timeSeries95p.add(minute, m.getPercentile95());
                timeSeriesMax.add(minute, m.getMax());
            }
            TimeSeriesCollection timeSeriesCollection = new TimeSeriesCollection(timeSeries50p);
            timeSeriesCollection.addSeries(timeSeries95p);
            timeSeriesCollection.addSeries(timeSeriesMax);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                JFreeChart chart = ChartFactory.createTimeSeriesChart("Flight time", "Time", "ms",
                                timeSeriesCollection);
                chart.getPlot().setBackgroundPaint(Color.WHITE);
                ChartUtilities.writeChartAsPNG(bos, chart, 1024, 500);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return "data:image/png;base64," + Base64.getEncoder().encodeToString(bos.toByteArray());
        }

        private FlightTimeMetrics computeFlightTimeMetrics(Timestamp time, MutableIntList flightTimes) {
            flightTimes.sortThis();
            int numberOfMeasurements = flightTimes.size();
            int medianIndex = (numberOfMeasurements * 50 / 100);
            int percentile95Index = (numberOfMeasurements * 95 / 100);
            return new FlightTimeMetrics(time, flightTimes.size(), flightTimes.get(0),
                            flightTimes.get(flightTimes.size() - 1), flightTimes.get(medianIndex),
                            flightTimes.get(percentile95Index));
        }

        public List<PeriodMetrics> getMessagesPerMinute() {
            // This is called multiple times but intentionally NOT cached as it takes too much memory
            try (Statement stat = _conn.createStatement();
                 ResultSet rs = stat.executeQuery("select time_period, produced_count, consumed_count,"
                                 + " produced_bytes, consumed_bytes," + " produced_max_size, consumed_max_size,"
                                 + " produced_median_size, consumed_median_size"
                                 + " from messages_per_minute order by time_period")) {
                List<PeriodMetrics> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new PeriodMetrics(rs.getTimestamp("time_period"), rs.getInt("produced_count"),
                                    rs.getInt("consumed_count"), rs.getInt("produced_bytes"),
                                    rs.getInt("consumed_bytes"), rs.getInt("produced_max_size"),
                                    rs.getInt("consumed_max_size"), rs.getInt("produced_median_size"),
                                    rs.getInt("consumed_median_size")));
                }
                return list;
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }

        /**
         * Check if this is (or may be) a correctness test where the id header is present. Without the id header it is
         * impossible to connect produced and consumed messages.
         *
         * @return true if id is present in database, false otherwise.
         */
        public boolean isCorrectnessTest() {
            return getProducedMessageCount() > 0 && getConsumedMessageCount() > 0 && findExists(
                            "select application_id from produced_messages where application_id is not null");
        }

        public List<ProducedMessage> getLostMessages() {
            return findProducedMessages("select jms_id, application_id, payload_size, produced_time, delay_seconds"
                            + " from lost_messages order by jms_id");
        }

        public List<ConsumedMessage> getAlienMessages() {
            return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                            + " from alien_messages order by jms_id");
        }

        public List<ConsumedMessage> getUndeadMessages() {
            return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                            + " from undead_messages order by jms_id");
        }

        public List<ConsumedMessage> getGhostMessages() {
            return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                            + " from ghost_messages order by jms_id");
        }

        public List<ConsumedMessage> getDuplicateMessages() {
            return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                            + " from consumed_messages c"
                            + " where exists (select * from duplicate_messages d where d.application_id = c.application_id)"
                            + " order by application_id, jms_id");
        }

        private List<ProducedMessage> findProducedMessages(String sql) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                List<ProducedMessage> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new ProducedMessage(rs.getString("jms_id"), rs.getString("application_id"),
                                    (Integer) rs.getObject("payload_size"), rs.getTimestamp("produced_time"),
                                    rs.getInt("delay_seconds")));
                }
                return list;
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }

        private List<ConsumedMessage> findConsumedMessages(String sql) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                List<ConsumedMessage> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(new ConsumedMessage(rs.getString("jms_id"), rs.getString("application_id"),
                                    rs.getInt("payload_size"), rs.getTimestamp("consumed_time")));
                }
                return list;
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }

        private int findSimpleCount(String table) {
            return findWithIntResult("select count(*) from " + table);
        }

        private Timestamp findStartTime() {
            return findWithTimestampResult("select min(ts) from (" //
                            + "select min(produced_time) ts from produced_messages" //
                            + " union all " //
                            + "select min(consumed_time) ts from consumed_messages)");
        }

        private Timestamp findEndTime() {
            return findWithTimestampResult("select max(ts) from (" //
                            + "select max(produced_time) ts from produced_messages" //
                            + " union all " //
                            + "select max(consumed_time) ts from consumed_messages)");
        }

        private boolean findExists(String sql) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                return rs.next();
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }

        private int findWithIntResult(String sql) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }

        private Timestamp findWithTimestampResult(String sql) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getTimestamp(1);
                }
                return null;
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
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
                        "create schema", null, null, false, null);
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
