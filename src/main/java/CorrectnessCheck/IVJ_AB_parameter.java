package CorrectnessCheck;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

/**
 * This is class runs a Sliding Window Join Query with the order [[A X B]^w1 x C]^w2
 * parameters:
 * Run with these parameters:
 */

public class IVJ_AB_parameter {
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    Integer lowerBound;
    Integer upperBound;


    public IVJ_AB_parameter(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, int lowerBound, int upperBound) {
    this.streamA = streamA;
    this.streamB = streamB;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    }

    public DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>>  run() {
        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB = streamA
                .keyBy(new UDFs.getKeyT3())
                .intervalJoin(streamB.keyBy(new UDFs.getKeyT3()))
                .between(Time.minutes(lowerBound), Time.minutes(upperBound))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                });
        return streamAB;
    }

}
