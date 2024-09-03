package CorrectnessCheck;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

/**
 * This is class runs a Sliding Window Join Query with the order [[A X B]^w1 x C]^w2
 * parameters:
 * Run with these parameters:
 */

public class SeWJ_BA_parameter {
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    Integer gap;


    public SeWJ_BA_parameter(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, int gap) {
    this.streamA = streamA;
    this.streamB = streamB;
    this.gap = gap;
    }

    public DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>>  run() {
        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB = streamB.join(streamA)
                .where(new UDFs.getKeyT3())
                .equalTo(new UDFs.getKeyT3())
                .window(EventTimeSessionWindows.withGap(Time.minutes(4)))
                .apply(new FlatJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<Integer, Integer, Long> d2, Tuple3<Integer, Integer, Long> d1, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                });

        return streamAB;
    }

}
