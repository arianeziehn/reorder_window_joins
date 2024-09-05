package CorrectnessCheck;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

/**
 * This is class runs an Interval Join Query with the order [B X A]^w1 and return the result stream AB
 */

public class IVJ_BA {
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    Integer lowerBound;
    Integer upperBound;


    public IVJ_BA(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, int lowerBound, int upperBound) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> run() {
        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB = streamB
                .keyBy(new UDFs.getKeyT3())
                .intervalJoin(streamA.keyBy(new UDFs.getKeyT3()))
                .between(Time.minutes(lowerBound), Time.minutes(upperBound))
                .process(new ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> d2, Tuple3<Integer, Integer, Long> d1, ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                });
        return streamAB;
    }

}
