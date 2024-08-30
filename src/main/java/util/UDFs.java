package util;

import oldStuff_delete.KeyedDataPointGeneral;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class UDFs {
    /**
     * TimeStampAssigners
      */

    public static class ExtractTimestamp implements AssignerWithPeriodicWatermarks<Tuple3<Integer,Integer,Long>> {
        private static final long serialVersionUID = 1L;
        private long maxOutOfOrderness;

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
        public long extractTimestamp(Tuple3<Integer,Integer,Long> element, long l) {
            long timestamp = element.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampAB implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private long maxOutOfOrderness;

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
            if (timePropagation.equals("A")){
                timestamp = element.f2;
            }else
            {
                timestamp = element.f5;
            }
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }

    public static class ExtractTimestampBC implements AssignerWithPeriodicWatermarks<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> {
        private static final long serialVersionUID = 1L;
        private long maxOutOfOrderness;

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
            if (timePropagation.equals("B")){
                timestamp = element.f2;
            }else
            {
                timestamp = element.f5;
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

    public static class getKeyT6 implements KeySelector<Tuple6<Integer, Integer, Long,Integer, Integer, Long>, Integer> {
        @Override
        public Integer getKey(Tuple6<Integer, Integer, Long,Integer, Integer, Long> data) throws Exception {
            return data.f0;
        }
    }
}
