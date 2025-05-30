import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

import static java.lang.Math.max;

/**
 * This is class join a stream [[AxBxD] x C]w1 and creates an output ABCD
 */

public class SWJ_ABD_T10 {
    DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamA;
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamB;
    Integer w1Size;
    Integer w1Slide;


    public SWJ_ABD_T10(DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamA, DataStream<Tuple4<Integer, Integer, Long, Long>> streamB, int w1Size, int w1Slide) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.w1Size = w1Size;
        this.w1Slide = w1Slide;
    }

    public DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> run() {
        // join A B
        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = streamA.join(streamB)
                .where(new UDFs.getKeyT10())
                .equalTo(new UDFs.getKeyT4())
                .window(SlidingEventTimeWindows.of(Time.minutes(w1Size), Time.minutes(w1Slide)))
                .apply(new FlatJoinFunction<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>>() {
                    @Override
                    public void join(Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Tuple4<Integer, Integer, Long, Long> d2, Collector<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> collector) throws Exception {
                        long maxSysTime = max(d1.f6, d2.f3);
                        collector.collect(new Tuple13<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d2.f0, d2.f1, d2.f2, d1.f6, d1.f7, d1.f8, maxSysTime));
                    }
                });

        return streamAB;
    }

}
