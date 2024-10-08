import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.UDFs;

import static java.lang.Math.max;

/**
 * This is class runs an Interval Join Query with the order [A X B]^w1 and return the result stream AB
 */

public class IVJ_BA_T4 {
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamA;
    DataStream<Tuple4<Integer, Integer, Long, Long>> streamB;
    Integer lowerBound;
    Integer upperBound;


    public IVJ_BA_T4(DataStream<Tuple4<Integer, Integer, Long, Long>> streamA, DataStream<Tuple4<Integer, Integer, Long, Long>> streamB, int lowerBound, int upperBound) {
        this.streamA = streamA;
        this.streamB = streamB;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> run() {
        // join A B
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = streamA
                .keyBy(new UDFs.getKeyT4())
                .intervalJoin(streamB.keyBy(new UDFs.getKeyT4()))
                .between(Time.minutes(lowerBound), Time.minutes(upperBound))
                .process(new ProcessJoinFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>>() {

                    @Override
                    public void processElement(Tuple4<Integer, Integer, Long, Long> d2, Tuple4<Integer, Integer, Long, Long> d1, ProcessJoinFunction<Tuple4<Integer, Integer, Long, Long>, Tuple4<Integer, Integer, Long, Long>, Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>>.Context context, Collector<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> collector) throws Exception {
                        long maxSysTime = max(d1.f3, d2.f3);
                        collector.collect(new Tuple7<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2, maxSysTime));
                    }
                });
        return streamAB;
    }

}
