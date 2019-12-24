package com.techwolf.poseidon.demo.flink.common.util;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author zhoupeijie
 * write to hdfs with /ds=yyyy-MM-dd
 */
public class DateTimeDsBucketer<T> implements Bucketer<T> {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";
    private final String formatString;
    private final ZoneId zoneId;
    private transient DateTimeFormatter dateTimeFormatter;
    private  String formatString2;
    public DateTimeDsBucketer() {
        this("yyyy-MM-dd--HH");
    }
    public DateTimeDsBucketer(String formatString) {
        this(formatString, ZoneId.systemDefault());
    }

    public DateTimeDsBucketer(ZoneId zoneId) {
        this("yyyy-MM-dd--HH", zoneId);
    }
    public DateTimeDsBucketer(String formatString, ZoneId zoneId) {
        this.formatString = (String) Preconditions.checkNotNull(formatString);
        this.zoneId = (ZoneId) Preconditions.checkNotNull(zoneId);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId);
    }

    public DateTimeDsBucketer(String formatString, String formatString2, ZoneId zoneId) {
        this.formatString = (String) Preconditions.checkNotNull(formatString);
        this.zoneId = (ZoneId) Preconditions.checkNotNull(zoneId);
        this.formatString2=(String) Preconditions.checkNotNull(formatString2);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(this.zoneId);
    }

    @Override
    public Path getBucketPath(Clock clock, Path basePath, T element) {
        String newDateTimeString = this.dateTimeFormatter.format(Instant.ofEpochMilli(clock.currentTimeMillis()));
        return new Path(basePath + "/ds=" + newDateTimeString);
    }

    @Override
    public String toString() {
        return "DateTimeBucketer{formatString='" + this.formatString + '\'' + ", zoneId=" + this.zoneId + '}';
    }
}
