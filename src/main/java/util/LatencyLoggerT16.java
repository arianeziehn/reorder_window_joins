package util;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class logs the latency as average of all result tuples (Tuple16) received within a second
 */

public class LatencyLoggerT16 extends RichFlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(LatencyLoggerT16.class);
    private long totalLatencySum = 0;
    private long matchedPatternsCount = 0;
    private long lastLogTimeMs = -1;
    private boolean logPerTuple = false; //enables logging per tuple

    public LatencyLoggerT16() {
    }

    public LatencyLoggerT16(boolean logPerTuple) {
        this.logPerTuple = logPerTuple;
    }

    @Override
    public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> dp, Collector<String> collector) throws Exception {
        Long last_timestamp = dp.f15;
        String mes = log_latency(last_timestamp);
        if (!mes.equals("")) collector.collect(mes);
    }

    public String log_latency(long last_timestamp) {
        String message = "";
        long currentTime = System.currentTimeMillis();
        long detectionLatency = currentTime - last_timestamp;

        this.totalLatencySum += detectionLatency;
        this.matchedPatternsCount += 1;

        if (lastLogTimeMs == -1) { //init
            lastLogTimeMs = currentTime;
            LOG.info("Starting Latency Logging for matched patterns with frequency 1 second.");
        }

        long timeDiff = currentTime - lastLogTimeMs;
        if (timeDiff >= 1000 || this.logPerTuple) {
            long eventDetectionLatencyAVG = this.totalLatencySum / this.matchedPatternsCount;
            message = "LatencyLogger: $ On Worker: During the last $" + timeDiff + "$ ms, AVGEventDetLatSum: $" + eventDetectionLatencyAVG + "$, derived from a LatencySum: $" + totalLatencySum +
                    "$, and a matche Count of : $" + matchedPatternsCount + "$";
            //Log.info(message);
            lastLogTimeMs = currentTime;
            totalLatencySum = 0;
            matchedPatternsCount = 0;
            return message;
        }
        return message;
    }

}
