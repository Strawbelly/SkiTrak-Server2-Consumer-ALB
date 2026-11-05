package consumer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Records consumer lag: processing start minus the ingest server's publish time
 * (the x-publish-ms header), in milliseconds.
 *
 * Samples are grouped by the second in which the message was published, so the
 * lag of any publish-time window (for example, one test run without its warmup)
 * can be read without resetting anything. Each second holds a histogram with
 * 1 ms buckets below 1 s, 10 ms buckets below 10 s, and 100 ms buckets up to 60 s
 * (the queue TTL), plus the exact maximum.
 */
final class LagStats {

    static final String HEADER = "x-publish-ms";

    // Seconds of publish time kept; older seconds are dropped as new ones arrive.
    private static final int MAX_SECONDS = 900;

    private static final int FINE = 1000;               // 0..999 ms, 1 ms wide
    private static final int MEDIUM = 900;              // 1000..9999 ms, 10 ms wide
    private static final int COARSE = 500;              // 10000..59999 ms, 100 ms wide
    private static final int BUCKETS = FINE + MEDIUM + COARSE + 1; // last bucket: >= 60 s

    final AtomicLong missingHeader = new AtomicLong();
    final AtomicLong negative = new AtomicLong();

    private final ConcurrentSkipListMap<Long, Second> seconds = new ConcurrentSkipListMap<>();

    private static final class Second {
        final AtomicLongArray counts = new AtomicLongArray(BUCKETS);
        final AtomicLong max = new AtomicLong();
    }

    /** Records one message; publishMs is null when the header was missing. */
    void record(Long publishMs, long nowMs) {
        if (publishMs == null) {
            missingHeader.incrementAndGet();
            return;
        }
        long lag = nowMs - publishMs;
        if (lag < 0) {
            // Clock offset between hosts; counted, and recorded as 0.
            negative.incrementAndGet();
            lag = 0;
        }
        long second = publishMs / 1000;
        Second s = seconds.computeIfAbsent(second, k -> new Second());
        s.counts.incrementAndGet(bucket(lag));
        s.max.accumulateAndGet(lag, Math::max);

        if (seconds.size() > MAX_SECONDS) {
            seconds.headMap(seconds.lastKey() - MAX_SECONDS).clear();
        }
    }

    /** Lag over all retained seconds. */
    Map<String, Object> summary() {
        return stats(seconds);
    }

    /** Lag of messages published in [fromMs, toMs), at whole-second granularity. */
    Map<String, Object> window(long fromMs, long toMs) {
        // Only seconds that lie entirely inside the window, so nothing outside it is counted.
        long first = (fromMs + 999) / 1000;
        long end = toMs / 1000;
        Map<String, Object> out = stats(first < end
                ? seconds.subMap(first, true, end, false) : new ConcurrentSkipListMap<>());
        out.put("fromSecond", first);
        out.put("toSecondExclusive", end);
        out.put("oldestRetainedSecond", seconds.isEmpty() ? null : seconds.firstKey());
        return out;
    }

    private static Map<String, Object> stats(ConcurrentNavigableMap<Long, Second> range) {
        long[] merged = new long[BUCKETS];
        long max = 0;
        long n = 0;
        for (Second s : range.values()) {
            for (int i = 0; i < BUCKETS; i++) {
                long c = s.counts.get(i);
                merged[i] += c;
                n += c;
            }
            max = Math.max(max, s.max.get());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("samples", n);
        out.put("p50Ms", n == 0 ? null : percentile(merged, n, 0.50));
        out.put("p99Ms", n == 0 ? null : percentile(merged, n, 0.99));
        out.put("maxMs", n == 0 ? null : max);
        return out;
    }

    /** Upper edge of the bucket holding the p-th sample: exact below 1 s, conservative above. */
    private static long percentile(long[] counts, long n, double p) {
        long rank = (long) Math.ceil(p * n);
        long seen = 0;
        for (int i = 0; i < BUCKETS; i++) {
            seen += counts[i];
            if (seen >= rank) {
                return upperEdge(i);
            }
        }
        return upperEdge(BUCKETS - 1);
    }

    static int bucket(long lag) {
        if (lag < 1000) {
            return (int) lag;
        }
        if (lag < 10_000) {
            return FINE + (int) ((lag - 1000) / 10);
        }
        if (lag < 60_000) {
            return FINE + MEDIUM + (int) ((lag - 10_000) / 100);
        }
        return BUCKETS - 1;
    }

    static long upperEdge(int bucket) {
        if (bucket < FINE) {
            return bucket;
        }
        if (bucket < FINE + MEDIUM) {
            return 1000 + (bucket - FINE + 1) * 10L - 1;
        }
        if (bucket < FINE + MEDIUM + COARSE) {
            return 10_000 + (bucket - FINE - MEDIUM + 1) * 100L - 1;
        }
        return 60_000; // the >= 60 s bucket; the queue TTL keeps messages from waiting longer
    }
}
