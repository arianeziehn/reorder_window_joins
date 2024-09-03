package CorrectnessCheck;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

import static java.lang.Math.max;

/**
 * This is class runs a Sliding Window Join Query with the order [[B X C]^wx x A]^wy
 * x=1 und y=2 if (w1.size > w2.size) and x=2 und y=1 else
 * no clue if that holds!
 */

public class SWJ_a_BCA_parameter_w1_lt_w2 {
    DataStream<Tuple3<Integer, Integer, Long>> streamC;
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    Integer w1Size;
    Integer w1Slide;
    Integer w2Size;
    Integer w2Slide;
    String timePropagation;


    public SWJ_a_BCA_parameter_w1_lt_w2(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, DataStream<Tuple3<Integer, Integer, Long>>streamC, int w1Size, int w1Slide, int w2Size, int w2Slide, String timePropagation) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.streamC = streamC;
        this.w1Size = w1Size;
        this.w1Slide = w1Slide;
        this.w2Size = w2Size;
        this.w2Slide = w2Slide;
        this.timePropagation = timePropagation;
    }

    public DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>  run() {

        Integer w_max_Size;
        Integer w_max_Slide;
        Integer w_min_Size;
        Integer w_min_Slide;

        if (w1Size > w2Size) {
            // w_1 (A:B) is the maximal window
            w_max_Size = w1Size;
            w_max_Slide = w1Slide;
            // it follow w2Size < w1Size and we want to be efficient, so we propagate C and w2Size
            w_min_Size = w2Size;
            w_min_Slide = w2Slide;
            timePropagation = "C";
        } else {
            w_max_Size = w2Size;
            w_max_Slide = w2Slide;
            w_min_Size = w1Size;
            w_min_Slide = w1Slide;
            timePropagation = "B";
        }

        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC = streamB.join(streamC)
                .where(new UDFs.getKeyT3())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w_max_Size), Time.minutes(w_max_Slide)))
                .apply(new FlatJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        // filter the b tuples that fall into the first half of w_max
                        double intervalC = Math.floor(d2.f2 /(float)(60000L*w_max_Size));
                        float intervalB = d1.f2 /(float)(60000L*w_max_Size);

                        if (intervalB - intervalC < 0.5) { //TODO holds only if w_max = 2 * w_min
                            collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                        } else if (intervalC < intervalB && intervalC - intervalB < 1.0) {
                            collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                        }
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBC(60000, timePropagation));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC = streamBC.join(streamA)
                .where(new UDFs.getKeyT6())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w_min_Size), Time.minutes(w_min_Slide)))
                .apply(new FlatJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    public void join(Tuple6<Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                            collector.collect(new Tuple9<>(d2.f0, d2.f1, d2.f2, d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5));
                    }
                });

        return streamABC;

    }

}
