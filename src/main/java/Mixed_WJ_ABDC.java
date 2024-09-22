import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

/**
 * This class runs a Sliding Window Join Query with the order [[[A X B]^w1 x C]^w2 x D]^w3 and returns the stream ABCD.
 * assumption: default query
 * parameters:
 * timePropagation: The timestamp (either 'A' or 'B') that is used for the stream AB in [AB x C]^w2
 */

public class Mixed_WJ_ABDC {
    DataStream<Tuple3<Integer, Integer, Long>> streamC;
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    DataStream<Tuple3<Integer, Integer, Long>> streamD;
    Integer w1Size;
    Integer w1Slide;
    Integer w2Size;
    Integer w2Slide;
    Integer w3Size;
    Integer w3Slide;
    String timePropagation1;
    String timePropagation2;


    public Mixed_WJ_ABDC(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, DataStream<Tuple3<Integer, Integer, Long>> streamC, DataStream<Tuple3<Integer, Integer, Long>> streamD, int w1Size, int w1Slide, int w2Size, int w2Slide, int w3Size, int w3Slide, String timePropagation1, String timePropagation2) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.streamC = streamC;
        this.streamD = streamD;
        this.w1Size = w1Size;
        this.w1Slide = w1Slide;
        this.w2Size = w2Size;
        this.w2Slide = w2Slide;
        this.w3Size = w3Size;
        this.w3Slide = w3Slide;
        this.timePropagation1 = timePropagation1;
        this.timePropagation2 = timePropagation2;
    }

    public DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> run() {
        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB = streamA.join(streamB)
                .where(new UDFs.getKeyT3())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w1Size), Time.minutes(w1Slide)))
                .apply(new FlatJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAB(60000, timePropagation2));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABD = streamAB.join(streamD)
                .where(new UDFs.getKeyT6())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w3Size), Time.minutes(w3Slide)))
                .apply(new FlatJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    public void join(Tuple6<Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple9<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d2.f0, d2.f1, d2.f2));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampABC(60000, timePropagation1));

        DataStream<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABCD = streamABD
                .keyBy(new UDFs.getKeyT9())
                .intervalJoin(streamC.keyBy(new UDFs.getKeyT3()))
                .between(Time.minutes(w2Size), Time.minutes(w2Slide))
                .process(new ProcessJoinFunction<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {


                    @Override
                    public void processElement(Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, ProcessJoinFunction<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>.Context context, Collector<Tuple12<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple12<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d2.f0, d2.f1, d2.f2, d1.f6, d1.f7, d1.f8));
                    }
                });

        return streamABCD;
    }
}
