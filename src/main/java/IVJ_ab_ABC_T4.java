import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

import static java.lang.Math.max;

/**
 * This class runs an Interval Join Query with the order [[A X B]^w1 x C]^w2 and returns the stream ABC.
 * assumption: default query
 * parameters:
 * timePropagation: The timestamp (either 'A' or 'B') that is used for the stream AB in [AB x C]^w2
 */

public class IVJ_ab_ABC_T4 {
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamC;
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamA;
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamB;
    Integer lowerBound_w1;
    Integer upperBound_w1;
    Integer lowerBound_w2;
    Integer upperBound_w2;
    String timePropagation;


    public IVJ_ab_ABC_T4(DataStream<Tuple4<Integer, Integer, Long, Long>> streamA, DataStream<Tuple4<Integer, Integer, Long, Long>> streamB, DataStream<Tuple4<Integer, Integer, Long, Long>> streamC, int lowerBound_w1, int upperBound_w1, int lowerBound_w2, int upperBound_w2, String timePropagation) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.streamC = streamC;
        this.lowerBound_w1 = lowerBound_w1;
        this.upperBound_w1 = upperBound_w1;
        this.lowerBound_w2 = lowerBound_w2;
        this.upperBound_w2 = upperBound_w2;
        this.timePropagation = timePropagation;
    }

    public DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> run() {
        // join A B
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = streamA
                .keyBy(new UDFs.getKeyT4())
                .intervalJoin(streamB.keyBy(new UDFs.getKeyT4()))
                .between(Time.minutes(lowerBound_w1), Time.minutes(upperBound_w1))
                .process(new ProcessJoinFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>>() {

                    @Override
                    public void processElement(Tuple4<Integer, Integer, Long, Long> d1, Tuple4<Integer, Integer, Long, Long> d2, ProcessJoinFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>>.Context context, Collector<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> collector) throws Exception {
                        long maxSysTime = 0;
                        if (d1.f2 > d2.f2) {
                            maxSysTime = d1.f3;
                        } else if (d1.f2.equals(d2.f2)) {
                            maxSysTime = max(d1.f3, d2.f3);
                        } else {
                            maxSysTime = d2.f3;
                        }
                        collector.collect(new Tuple7<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2, maxSysTime));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAB_T7(60000, timePropagation));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABC = streamAB
                .keyBy(new UDFs.getKeyT7())
                .intervalJoin(streamC.keyBy(new UDFs.getKeyT4()))
                .between(Time.minutes(lowerBound_w2), Time.minutes(upperBound_w2))
                .process(new ProcessJoinFunction<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>>() {

                    @Override
                    public void processElement(Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long> d1, Tuple4<Integer, Integer, Long, Long> d2, ProcessJoinFunction<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>>.Context context, Collector<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> collector) throws Exception {
                        long maxSysTime = 0;
                        if (max(d1.f5, d1.f2) == d2.f2) {
                            maxSysTime = max(d1.f6, d2.f3);
                        } else if (max(d1.f5, d1.f2) < d2.f2) {
                            maxSysTime = d2.f3;
                        } else {
                            maxSysTime = d1.f6;
                        }
                        collector.collect(new Tuple10<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d2.f0, d2.f1, d2.f2, maxSysTime));
                    }
                });

        return streamABC;

    }

}
