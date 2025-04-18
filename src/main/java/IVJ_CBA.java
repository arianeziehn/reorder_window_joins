import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

/**
 * This class runs an Interval Join Query with the order [[C X B]^w1 x A]^w2 and returns the stream ABC.
 * timePropagation: The timestamp (either 'B' or 'C') that is used for the stream BC in [BC x A]^w2
 */

public class IVJ_CBA {
    DataStream<Tuple3<Integer, Integer, Long>> streamC;
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    Integer lowerBound_w1;
    Integer upperBound_w1;
    Integer lowerBound_w2;
    Integer upperBound_w2;
    String timePropagation;


    public IVJ_CBA(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, DataStream<Tuple3<Integer, Integer, Long>> streamC, int lowerBound_w1, int upperBound_w1, int lowerBound_w2, int upperBound_w2, String timePropagation) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.streamC = streamC;
        this.lowerBound_w1 = lowerBound_w1;
        this.upperBound_w1 = upperBound_w1;
        this.lowerBound_w2 = lowerBound_w2;
        this.upperBound_w2 = upperBound_w2;
        this.timePropagation = timePropagation;
    }

    public DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> run() {
        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC = streamC
                .keyBy(new UDFs.getKeyT3())
                .intervalJoin(streamB.keyBy(new UDFs.getKeyT3()))
                .between(Time.minutes(lowerBound_w1), Time.minutes(upperBound_w1))
                .process(new ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {

                    @Override
                    public void processElement(Tuple3<Integer, Integer, Long> d2, Tuple3<Integer, Integer, Long> d1, ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBC(60000, timePropagation));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC = streamBC
                .keyBy(new UDFs.getKeyT6())
                .intervalJoin(streamA.keyBy(new UDFs.getKeyT3()))
                .between(Time.minutes(lowerBound_w2), Time.minutes(upperBound_w2))
                .process(new ProcessJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    @Override
                    public void processElement(Tuple6<Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, ProcessJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple9<>(d2.f0, d2.f1, d2.f2, d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5));
                    }
                });

        return streamABC;

    }

}
