package assigner;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public abstract class CustomTimeWindow extends Windows<TimeWindow> {

    // local zone id
    protected final ZoneId zoneId;
    // grace period milliseconds
    protected final long grace;

    /**
     * Default constructor
     * @param zoneId local zone id
     * @param grace grace period milliseconds
     */
    protected CustomTimeWindow(final ZoneId zoneId, final Duration grace) {
        this.zoneId = zoneId;
        this.grace = grace.toMillis();
    }

    /**
     * Converts the zoned time to milliseconds
     * @param zonedDateTime time with an associated time zone
     * @return timestamp in milliseconds
     */
    protected long toEpochMilli(final ZonedDateTime zonedDateTime) {
        return zonedDateTime.toInstant().toEpochMilli();
    }

    /**
     * Getter for the grace period
     * @return grace period in milliseconds
     */
    @Override
    public long gracePeriodMs() {
        return grace;
    }
}
