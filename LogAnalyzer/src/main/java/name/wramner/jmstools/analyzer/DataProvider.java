/*
 * Copyright 2018 Erik Wramner.
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

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Minute;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

/**
 * This class provides data for the Thymeleaf reports. The public methods here can be called from the reports.
 */
public class DataProvider {
    private final Connection _conn;
    private List<FlightTimeMetrics> _flightTimeMetrics;
    private final Map<String, Object> _cache = new HashMap<>();

    /**
     * Constructor.
     *
     * @param conn The database connection.
     */
    public DataProvider(Connection conn) {
        _conn = conn;
    }

    /**
     * Get the number of alien messages, meaning messages without the id property set by the JmsTools producer for
     * correctness tests.
     *
     * @return total alien message count.
     */
    public int getAlienMessageCount() {
        return findSimpleCount("alien_messages");
    }

    /**
     * Get a list of alien messages, meaning messages without the id property set by the JmsTools producer for
     * correctness tests.
     *
     * @return list with alien messages.
     */
    public List<ConsumedMessage> getAlienMessages() {
        return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                        + " from alien_messages order by jms_id");
    }

    /**
     * Get the average size of the consumed messages in bytes.
     *
     * @return size.
     */
    public long getAverageConsumedMessageSize() {
        return findWithLongResult("select avg(payload_size) from consumed_messages");
    }

    /**
     * Get the average size of the produced messages in bytes.
     *
     * @return size.
     */
    public long getAverageProducedMessageSize() {
        return findWithLongResult("select avg(payload_size) from produced_messages");
    }

    /**
     * Get a base64-encoded image for inclusion in an img tag with a chart with kilobytes per minute produced and
     * consumed.
     *
     * @return chart as base64 string.
     */
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

    /**
     * Get a base64-encoded image for inclusion in an img tag with a chart with message flight times.
     *
     * @return chart as base64 string.
     */
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
            JFreeChart chart = ChartFactory.createTimeSeriesChart("Flight time", "Time", "ms", timeSeriesCollection);
            chart.getPlot().setBackgroundPaint(Color.WHITE);
            ChartUtilities.writeChartAsPNG(bos, chart, 1024, 500);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "data:image/png;base64," + Base64.getEncoder().encodeToString(bos.toByteArray());
    }

    /**
     * Get a base64-encoded image for inclusion in an img tag with a chart with number of produced and consumed messages
     * per minute.
     *
     * @return chart as base64 string.
     */
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

    /**
     * Get a base64-encoded image for inclusion in an img tag with a chart with number of produced and consumed messages
     * per second.
     *
     * @return chart as base64 string.
     */
    public String getBase64MessagesPerSecondImage() {
        TimeSeries timeSeriesConsumed = new TimeSeries("Consumed");
        TimeSeries timeSeriesProduced = new TimeSeries("Produced");
        TimeSeries timeSeriesTotal = new TimeSeries("Total");
        for (PeriodMetrics m : getMessagesPerSecond()) {
            Second second = new Second(m.getPeriodStart());
            timeSeriesConsumed.add(second, m.getConsumed());
            timeSeriesProduced.add(second, m.getProduced());
            timeSeriesTotal.add(second, m.getConsumed() + m.getProduced());
        }
        TimeSeriesCollection timeSeriesCollection = new TimeSeriesCollection(timeSeriesConsumed);
        timeSeriesCollection.addSeries(timeSeriesProduced);
        timeSeriesCollection.addSeries(timeSeriesTotal);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            JFreeChart chart = ChartFactory.createTimeSeriesChart("Messages per second (TPS)", "Time", "Messages",
                            timeSeriesCollection);
            chart.getPlot().setBackgroundPaint(Color.WHITE);
            ChartUtilities.writeChartAsPNG(bos, chart, 1024, 500);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "data:image/png;base64," + Base64.getEncoder().encodeToString(bos.toByteArray());
    }

    /**
     * Get the sum of the size in bytes of all committed consumed messages.
     *
     * @return sum of consumed message sizes.
     */
    public long getCommittedConsumedBytes() {
        return findWithLongResult("select sum(payload_size) from consumed_messages where outcome = 'C'");
    }

    /**
     * Get the total number of committed consumed messages.
     *
     * @return count.
     */
    public int getCommittedConsumedCount() {
        return getConsumedMessageCount() - getRolledBackConsumedCount() - getInDoubtConsumedCount();
    }

    /**
     * Get the sum of the size in bytes of all committed produced messages.
     *
     * @return sum of produced message sizes.
     */
    public long getCommittedProducedBytes() {
        return findWithLongResult("select sum(payload_size) from produced_messages where outcome = 'C'");
    }

    /**
     * Get the total number of committed produced messages.
     *
     * @return count.
     */
    public int getCommittedProducedCount() {
        return getProducedMessageCount() - getRolledBackProducedCount() - getInDoubtProducedCount();
    }

    /**
     * Get the total number of consumed messages including rolled back and in-doubt messages.
     *
     * @return count.
     */
    public int getConsumedMessageCount() {
        return findSimpleCount("consumed_messages");
    }

    /**
     * Get the total number of delayed messages, i.e. messages that should not be delivered immediately but later.
     *
     * @return count.
     */
    public int getDelayedMessageCount() {
        return findWithIntResult("select count(*) from produced_messages where delay_seconds > 0");
    }

    /**
     * Get the percentage of delayed messages, i.e. messages that were sent with delayed delivery.
     *
     * @return percentage.
     */
    public double getDelayedMessagePercentage() {
        return getProducedMessageCount() > 0 ? (100.0 * getDelayedMessageCount()) / getProducedMessageCount() : 0;
    }

    /**
     * Get the total number of duplicate messages identified.
     *
     * @return count.
     */
    public int getDuplicateMessageCount() {
        return findSimpleCount("duplicate_messages");
    }

    /**
     * Get list with all duplicate messages.
     *
     * @return duplicate messages.
     */
    public List<ConsumedMessage> getDuplicateMessages() {
        return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                        + " from consumed_messages c"
                        + " where exists (select * from duplicate_messages d where d.application_id = c.application_id)"
                        + " order by application_id, jms_id");
    }

    /**
     * Get time for last produced or consumed message.
     *
     * @return end time.
     */
    public Timestamp getEndTime() {
        return findWithTimestampResult("select max(ts) from (" //
                        + "select max(produced_time) ts from produced_messages" //
                        + " union all " //
                        + "select max(consumed_time) ts from consumed_messages)");
    }

    /**
     * Get the time for the first consumed message.
     *
     * @return time.
     */
    public Timestamp getFirstConsumedTime() {
        return findWithTimestampResult("select min(consumed_time) ts from consumed_messages");
    }

    /**
     * Get the time for the first produced message.
     *
     * @return time.
     */
    public Timestamp getFirstProducedTime() {
        return findWithTimestampResult("select min(produced_time) ts from produced_messages");
    }

    /**
     * Get list with flight time metrics per minute.
     *
     * @return list with flight time metrics.
     */
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

    /**
     * Get the number of ghost messages, i.e. the number of messages that were delivered even though the sender rolled
     * them back.
     *
     * @return count.
     */
    public int getGhostMessageCount() {
        return findSimpleCount("ghost_messages");
    }

    /**
     * Get list with all ghost messages, i.e. the ones that were delivered even though the sender rolled them back.
     *
     * @return list with messages.
     */
    public List<ConsumedMessage> getGhostMessages() {
        return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                        + " from ghost_messages order by jms_id");
    }

    /**
     * Get the total number of consumed messages that are in doubt, meaning that they may have been committed or rolled
     * back or possibly stuck in pending state for a two phase commit (not known).
     *
     * @return consumed messages in doubt.
     */
    public int getInDoubtConsumedCount() {
        return findWithIntResult("select count(*) from consumed_messages where outcome = '?'");
    }

    /**
     * Get list with all in doubt consumed messages.
     *
     * @return messages.
     */
    public List<ConsumedMessage> getInDoubtConsumedMessages() {
        return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                        + " from consumed_messages c where outcome = '?' order by consumed_time, jms_id");
    }

    /**
     * Get the total number of produced messages that are in doubt, meaning that they may have been committed or rolled
     * back or possibly stuck in pending state for a two phase commit (not known).
     *
     * @return produced messages in doubt.
     */
    public int getInDoubtProducedCount() {
        return findWithIntResult("select count(*) from produced_messages where outcome = '?'");
    }

    /**
     * Get a list with all in doubt produced messages.
     *
     * @return list with messages.
     */
    public List<ProducedMessage> getInDoubtProducedMessages() {
        return findProducedMessages("select jms_id, application_id, payload_size, produced_time, delay_seconds"
                        + " from produced_messages where outcome = '?' order by produced_time, jms_id");
    }

    /**
     * Get the time for the last consumed message.
     *
     * @return time.
     */
    public Timestamp getLastConsumedTime() {
        return findWithTimestampResult("select max(consumed_time) ts from consumed_messages");
    }

    /**
     * Get the time for the last produced message.
     *
     * @return time.
     */
    public Timestamp getLastProducedTime() {
        return findWithTimestampResult("select max(produced_time) ts from produced_messages");
    }

    /**
     * Get the total number of lost messages.
     *
     * @return count.
     */
    public int getLostMessageCount() {
        return findSimpleCount("lost_messages");
    }

    /**
     * Get a list with all lost messages.
     *
     * @return list with messages.
     */
    public List<ProducedMessage> getLostMessages() {
        return findProducedMessages("select jms_id, application_id, payload_size, produced_time, delay_seconds"
                        + " from lost_messages order by jms_id");
    }

    /**
     * Get the size of the largest consumed message in bytes.
     *
     * @return size.
     */
    public long getMaxConsumedMessageSize() {
        return findWithLongResult("select max(payload_size) from consumed_messages");
    }

    /**
     * Get the size of the largest produced message in bytes.
     *
     * @return size.
     */
    public long getMaxProducedMessageSize() {
        return findWithLongResult("select max(payload_size) from produced_messages");
    }

    /**
     * Get a list with period metrics per minute.
     *
     * @return list with metrics with minute resolution.
     */
    public List<PeriodMetrics> getMessagesPerMinute() {
        // This may be called multiple times but intentionally NOT cached as it takes too much memory
        return getMessagesPerInterval(TimeUnit.MINUTES);
    }

    /**
     * Get a list with period metrics per second.
     *
     * @return list with metrics with second resolution.
     */
    public List<PeriodMetrics> getMessagesPerSecond() {
        // This may be called multiple times but intentionally NOT cached as it takes too much memory
        return getMessagesPerInterval(TimeUnit.SECONDS);
    }

    /**
     * Get the size of the smallest consumed message in bytes.
     *
     * @return size.
     */
    public long getMinConsumedMessageSize() {
        return findWithLongResult("select min(payload_size) from consumed_messages");
    }

    /**
     * Get the size of the smallest produced message in bytes.
     *
     * @return size.
     */
    public long getMinProducedMessageSize() {
        return findWithLongResult("select min(payload_size) from produced_messages");
    }

    /**
     * Get the total number of produced messages including rolled back and in-doubt messages.
     *
     * @return count.
     */
    public int getProducedMessageCount() {
        return findSimpleCount("produced_messages");
    }

    /**
     * Get the number of consumed messages that were rolled back intentionally or due to errors.
     *
     * @return count.
     */
    public int getRolledBackConsumedCount() {
        return findWithIntResult("select count(*) from consumed_messages where outcome = 'R'");
    }

    /**
     * Get the number of produced messages that were rolled back intentionally or due to errors.
     *
     * @return count.
     */
    public int getRolledBackProducedCount() {
        return findWithIntResult("select count(*) from produced_messages where outcome = 'R'");
    }

    /**
     * Get time for first produced or consumed message.
     *
     * @return start time.
     */
    public Timestamp getStartTime() {
        return findWithTimestampResult("select min(ts) from (" //
                        + "select min(produced_time) ts from produced_messages" //
                        + " union all " //
                        + "select min(consumed_time) ts from consumed_messages)");
    }

    /**
     * Get test duration in minutes rounding up. A test that has been running for 1 minute an 59 seconds will be
     * reported as 2 minutes.
     *
     * @return duration in minutes.
     */
    public int getTestDurationMinutes() {
        return (getTestDurationSeconds() + 59) / 60;
    }

    /**
     * Get test duration in seconds rounded up to the closest second like ceil.
     *
     * @return duration in seconds.
     */
    public int getTestDurationSeconds() {
        return (int) ((getEndTime().getTime() - getStartTime().getTime() + 999L) / 1000L);
    }

    /**
     * Get the total number of consumed and produced messages.
     *
     * @return count.
     */
    public int getTotalMessageCount() {
        return getConsumedMessageCount() + getProducedMessageCount();
    }

    /**
     * Get the number of undead messages. An undead message is one that has the identifying headers used by JmsTools,
     * but that has not been recorded as sent. The most likely explanation is that a message from an other test was
     * received or that only consumer logs were imported. Another possibility is that a producer was killed before
     * managing to record the sent message.
     *
     * @return count.
     */
    public int getUndeadMessageCount() {
        return findSimpleCount("undead_messages");
    }

    /**
     * Get a list with all undead messages. An undead message is one that has the identifying headers used by JmsTools,
     * but that has not been recorded as sent. The most likely explanation is that a message from an other test was
     * received or that only consumer logs were imported. Another possibility is that a producer was killed before
     * managing to record the sent message.
     *
     * @return list with messages.
     */
    public List<ConsumedMessage> getUndeadMessages() {
        return findConsumedMessages("select jms_id, application_id, payload_size, consumed_time"
                        + " from undead_messages order by jms_id");
    }

    /**
     * Check if this is (or may be) a correctness test where the id header is present. Without the id header it is
     * impossible to connect produced and consumed messages.
     *
     * @return true if id is present in database, false otherwise.
     */
    public boolean isCorrectnessTest() {
        return getProducedMessageCount() > 0 && getConsumedMessageCount() > 0 && findExistsWithCache(
                        "select application_id from produced_messages where application_id is not null");
    }

    /**
     * Check if it makes sense to show flight times. Produced and consumed messages with the unique id header must be
     * present for that.
     *
     * @return true if flight times are available.
     */
    public boolean isFlightTimeDataAvailable() {
        return getProducedMessageCount() > 0 && getConsumedMessageCount() > 0 && isCorrectnessTest();
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

    private boolean findExistsWithCache(String sql) {
        Boolean b = (Boolean) _cache.get(sql);
        if (b == null) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                b = Boolean.valueOf(rs.next());
                _cache.put(sql, b);
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }
        return b.booleanValue();
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

    private int findSimpleCount(String table) {
        return findWithIntResult("select count(*) from " + table);
    }

    private int findWithIntResult(String sql) {
        Number n = findWithCachedScalarResult(sql, Number.class);
        return n != null ? n.intValue() : 0;
    }

    private long findWithLongResult(String sql) {
        Number n = findWithCachedScalarResult(sql, Number.class);
        return n != null ? n.longValue() : 0L;
    }

    private Timestamp findWithTimestampResult(String sql) {
        return findWithCachedScalarResult(sql, Timestamp.class);
    }

    private <T> T findWithCachedScalarResult(String sql, Class<T> cls) {
        Object cachedValue = _cache.get(sql);
        if (cachedValue == null) {
            try (Statement stat = _conn.createStatement(); ResultSet rs = stat.executeQuery(sql)) {
                if (rs.next()) {
                    cachedValue = rs.getObject(1);
                }
                _cache.put(sql, cachedValue);
            } catch (SQLException e) {
                throw new UncheckedSqlException(e);
            }
        }
        return cachedValue != null ? cls.cast(cachedValue) : null;
    }

    private List<PeriodMetrics> getMessagesPerInterval(TimeUnit timeUnit) {
        String view = translateTimeUnitToMessagesPerIntervalViewName(timeUnit);
        try (Statement stat = _conn.createStatement();
             ResultSet rs = stat.executeQuery("select time_period, produced_count, consumed_count,"
                             + " produced_bytes, consumed_bytes, produced_max_size, consumed_max_size,"
                             + " produced_median_size, consumed_median_size from " + view + " order by time_period")) {
            List<PeriodMetrics> list = new ArrayList<>();
            while (rs.next()) {
                list.add(new PeriodMetrics(rs.getTimestamp("time_period"), rs.getInt("produced_count"),
                                rs.getInt("consumed_count"), rs.getLong("produced_bytes"), rs.getLong("consumed_bytes"),
                                rs.getInt("produced_max_size"), rs.getInt("consumed_max_size"),
                                rs.getInt("produced_median_size"), rs.getInt("consumed_median_size")));
            }
            return list;
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        }
    }

    private String translateTimeUnitToMessagesPerIntervalViewName(TimeUnit timeUnit) {
        switch (timeUnit) {
        case MINUTES:
            return "messages_per_minute";
        case SECONDS:
            return "messages_per_second";
        default:
            throw new IllegalArgumentException("Time unit " + timeUnit.name() + " not supported");
        }
    }
}