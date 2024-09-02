package CorrectnessCheck;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.ArtificalSourceFunction;
import util.ThroughputLogger;
import util.UDFs;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

/**
 * This is class runs a Sliding Window Join Query with the order [[A X C]^w2 x B]^w1
 * parameters:
 * Run with these parameters:
 * --input ./src/main/resources/QnV_R2000070.csv
 */

public class SWJ_a_ACB_parameter {
    DataStream<Tuple3<Integer, Integer, Long>> streamC;
    DataStream<Tuple3<Integer, Integer, Long>> streamA;
    DataStream<Tuple3<Integer, Integer, Long>> streamB;
    Integer w1Size;
    Integer w1Slide;
    Integer w2Size;
    Integer w2Slide;
    String timePropagation;


    public SWJ_a_ACB_parameter(DataStream<Tuple3<Integer, Integer, Long>> streamA, DataStream<Tuple3<Integer, Integer, Long>> streamB, DataStream<Tuple3<Integer, Integer, Long>>streamC, int w1Size, int w1Slide, int w2Size, int w2Slide, String timePropagation) {
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

        // join A B
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC = streamA.join(streamC)
                .where(new UDFs.getKeyT3())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w2Size), Time.minutes(w2Slide)))
                .apply(new FlatJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple6<Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple6<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2));
                    }
                }).assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAB(60000, timePropagation));

        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC = streamAC.join(streamB)
                .where(new UDFs.getKeyT6())
                .equalTo(new UDFs.getKeyT3())
                .window(SlidingEventTimeWindows.of(Time.minutes(w1Size), Time.minutes(w1Slide)))
                .apply(new FlatJoinFunction<Tuple6<Integer, Integer, Long, Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {

                    public void join(Tuple6<Integer, Integer, Long, Integer, Integer, Long> d1, Tuple3<Integer, Integer, Long> d2, Collector<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple9<>(d1.f0, d1.f1, d1.f2, d2.f0, d2.f1, d2.f2, d1.f3, d1.f4, d1.f5));
                    }
                });

    return streamABC;
    }

}
