import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

import static java.lang.Math.max;

/**
 * This is class join a stream [[AxBxCxD] x E]w1 and creates an output ABCDE
 */

public class SWJ_AC_T13 {
    DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamA;
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamB;
    Integer w1Size;
    Integer w1Slide;


    public SWJ_AC_T13(DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamA, DataStream<Tuple4<Integer, Integer, Long, Long>> streamB, int w1Size, int w1Slide) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.w1Size = w1Size;
        this.w1Slide = w1Slide;
    }

    public DataStream<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> run() {
        // join A B
        DataStream<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = streamA.join(streamB)
                .where(new UDFs.getKeyT13())
                .equalTo(new UDFs.getKeyT4())
                .window(SlidingEventTimeWindows.of(Time.minutes(w1Size), Time.minutes(w1Slide)))
                .apply(new FlatJoinFunction<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>>() {
                    @Override
                    public void join(Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Tuple4<Integer, Integer, Long, Long> d2, Collector<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> collector) throws Exception {
                        long maxSysTime = max(d1.f6, d2.f3);
                        collector.collect(new Tuple16<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d2.f0, d2.f1, d2.f2, maxSysTime));
                    }
                });

        return streamAB;
    }

}
