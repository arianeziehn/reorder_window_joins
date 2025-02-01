package util;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class UDFs {
    /**
     * TimeStampAssigners
     */

    public static class ExtractTimestamp implements AssignerWithPeriodicWatermarks<Tuple3<Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private long currentMaxTimestamp;

        public ExtractTimestamp() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestamp(long periodMs) {
            this.maxOutOfOrderness = (periodMs);
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<Integer, Integer, Long> element, long l) {
            long timestamp = element.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestamp_T4 implements AssignerWithPeriodicWatermarks<Tuple4<Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private long currentMaxTimestamp;

        public ExtractTimestamp_T4() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestamp_T4(long periodMs) {
            this.maxOutOfOrderness = (periodMs);
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple4<Integer, Integer, Long, Long> element, long l) {
            long timestamp = element.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAB implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAB() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAB(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple6<Integer, Integer, Long, Integer, Integer, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("B")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f2; // automatically propagate 'A' if timestamp assignment is wrong
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampBD implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampBD() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampBD(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple6<Integer, Integer, Long, Integer, Integer, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("B")) {
                timestamp = element.f2;
            } else {
                timestamp = element.f5; // automatically propagate last time stamp
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAB_T7 implements AssignerWithPeriodicWatermarks<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAB_T7() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAB_T7(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("B")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f2; // automatically propagate 'A' if timestamp assignment is wrong
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampABC implements AssignerWithPeriodicWatermarks<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampABC() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampABC(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("A")) {
                timestamp = element.f2;
            } else if (timePropagation.equals("B")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f8; // automatically propagate the last joined stream
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAC implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAC() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAC(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple6<Integer, Integer, Long, Integer, Integer, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("C")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f2; // automatically propagate 'A' if timestamp assignment is wrong
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAC_T7 implements AssignerWithPeriodicWatermarks<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAC_T7() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAC_T7(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("C")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f2; // automatically propagate 'A' if timestamp assignment is wrong
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAR_T7 implements AssignerWithPeriodicWatermarks<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAR_T7() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAR_T7(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("A")) {
                timestamp = element.f2;
            } else {
                timestamp = element.f5; // automatically propagate R timestamp
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAR_T10 implements AssignerWithPeriodicWatermarks<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAR_T10() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAR_T10(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("A")) {
                timestamp = element.f2;
            } else if (timePropagation.equals("B")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f8; // automatically propagate R timestamp
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampBR_T10 implements AssignerWithPeriodicWatermarks<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampBR_T10() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampBR_T10(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("B")) {
                timestamp = element.f2;
            } else if (timePropagation.equals("C")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f8; // automatically propagate R timestamp
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAR_T13 implements AssignerWithPeriodicWatermarks<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampAR_T13() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampAR_T13(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("A")) {
                timestamp = element.f2;
            } else if (timePropagation.equals("B")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f8; // automatically propagate R timestamp
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampBR_T7 implements AssignerWithPeriodicWatermarks<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampBR_T7() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampBR_T7(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("B")) {
                timestamp = element.f2;
            } else {
                timestamp = element.f5; // automatically propagate R timestamp
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampBC implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private final long maxOutOfOrderness;

        private String timePropagation;

        private long currentMaxTimestamp;

        public ExtractTimestampBC() {
            this.maxOutOfOrderness = 0;
        }

        public ExtractTimestampBC(long periodMs, String timePropagation) {

            this.maxOutOfOrderness = (periodMs);
            this.timePropagation = timePropagation;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple6<Integer, Integer, Long, Integer, Integer, Long> element, long l) {
            long timestamp = 0L;
            if (timePropagation.equals("C")) {
                timestamp = element.f5;
            } else {
                timestamp = element.f2; // automatically propagate B if not declared otherwise
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    /**
     * KeySelectors
     */

    public static class getKeyT3 implements KeySelector<Tuple3<Integer, Integer, Long>, Integer> {
        @Override
        public Integer getKey(Tuple3<Integer, Integer, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class getKeyT4 implements KeySelector<Tuple4<Integer, Integer, Long, Long>, Integer> {
        @Override
        public Integer getKey(Tuple4<Integer, Integer, Long, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class getKeyT6 implements KeySelector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Integer> {
        @Override
        public Integer getKey(Tuple6<Integer, Integer, Long, Integer, Integer, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class getKeyT7 implements KeySelector<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>, Integer> {
        @Override
        public Integer getKey(Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class getKeyT9 implements KeySelector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>, Integer> {
        @Override
        public Integer getKey(Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class getKeyT10 implements KeySelector<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Integer> {
        @Override
        public Integer getKey(Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class getKeyT13 implements KeySelector<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Integer> {
        @Override
        public Integer getKey(Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> data) throws Exception {
            return data.f0;
        }
    }

    public static class filterPosInt implements FilterFunction<Tuple3<Integer, Integer, Long>> {
        @Override
        public boolean filter(Tuple3<Integer, Integer, Long> tuple3) throws Exception {
            return tuple3.f1 > 0;
        }
    }

    public static class filterPosIntT4 implements FilterFunction<Tuple4<Integer, Integer, Long, Long>> {
        @Override
        public boolean filter(Tuple4<Integer, Integer, Long, Long> tuple3) throws Exception {
            return tuple3.f1 > 0;
        }
    }
}
