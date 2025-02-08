import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import util.*;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * In this class we test a three-way Window Join with mixed windows, i.e., [[[A X B]^w1_A x C]^w2_B x D]^w3,
 * where w1 is a SWJ creating overlapping windows, w2 is an IntervalJoin and, w3 is a non-overlapping SWJ
 * Using our window assignment algorithms, we get the following window assignments:
 * streams A, B, C, and D;
 * windows: w_1, w_2, W_3,
 * timestamp: W_2 : A, W_3 : B
 * <p>
 * window_assignment:
 * A:B : w_1
 * A:C : w_2
 * B:C : w_1,W_2
 * B:D : W_3
 * A:D : W_1,W_2,W_3
 * C:D:  W_1,W_2,W_3
 */

public class JoinReorderingTest_4waySWJ_QnV {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple4<Integer, Integer, Long, Long>> streamA;
    private DataStream<Tuple4<Integer, Integer, Long, Long>> streamB;
    private DataStream<Tuple4<Integer, Integer, Long, Long>> streamC;
    private DataStream<Tuple4<Integer, Integer, Long, Long>> streamD;
    private DataStream<Tuple4<Integer, Integer, Long, Long>> streamE;
    private int w1Size;
    private int w1Slide;
    private int w2Size;
    private int w2Slide;
    private int w3Size;
    private int w3Slide;
    private int w4Size;
    private int w4Slide;
    private String timePropagation1;
    private String timePropagation2;

    @Before
    public void setup() {
        // Initialize the StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        String file = "./src/main/resources/QnV_R2000070.csv";
        String filePM = "./src/main/resources/luftdaten_11245.csv";
        String fileTH = "./src/main/resources/luftdaten_11246.csv";
        // the number of keys, should be equals or more as parallelism
        Integer para = 2;
        Integer numberOfKeys = 5;
        Integer velFilter = 90;
        Integer quaFilter = 80;
        Integer pm10Filter = 20;
        Integer pm2Filter = 20;
        Integer tempFilter = 25;
        // we except minutes
        long throughput = 500; // we do it very slowly to be sure system grap it all

        // Create test streams A, B, C with some example data
        streamA = env.addSource(new Tuple4ParallelSourceFunction(file, numberOfKeys, ",", throughput, "V"))
                .setParallelism(para)
                .filter(t -> t.f1 > velFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

        streamB = env.addSource(new Tuple4ParallelSourceFunction(file, numberOfKeys, ",", throughput, "Q"))
                .setParallelism(para)
                .filter(t -> t.f1 > quaFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

        streamC = env.addSource(new Tuple4ParallelSourceFunction(filePM, numberOfKeys, ";", throughput, "PM10"))
                .setParallelism(para)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

        streamD = env.addSource(new Tuple4ParallelSourceFunction(filePM, numberOfKeys, ";", throughput, "PM2"))
                .setParallelism(para)
                .filter(t -> t.f1 > tempFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));

        streamE = env.addSource(new Tuple4ParallelSourceFunction(fileTH, numberOfKeys, ";", throughput, "Temp"))
                .setParallelism(para)
                .filter(t -> t.f1 > pm2Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp_T4(60000));
    }


    @Test
    public void testMixedWindowsS1() throws Exception {
        w1Size = 20;
        w1Slide = 10;
        w2Size = 20;
        w2Slide = 10;
        w3Size = 20;
        w3Slide = 20;
        w4Size = 20;
        w4Slide = 5;
        timePropagation1 = "B";

        String testCase = "5Streams";
        String outputPath = "./src/main/resources/result_4_way_";
        // Execute each join operation
        //default case ABCDE - [[[A X B]^w1_B x C]^w2_B x D]^w3_B x E]^w4
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABC = new SWJ_AC_T7(streamAB, streamC, w2Size, w2Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCD = new SWJ_AC_T10(streamABC, streamD, w3Size, w3Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABCDE = new SWJ_AC_T13(streamABCD, streamE, w4Size, w4Slide).run().flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
            @Override
            public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
            }
        });

        // Collect the results into lists
        streamABCDE
                .writeAsText(outputPath + "ABCDE_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //case ABECD - [[[A X B]^w1_B x E]^w4_B x C]^w2_B x D]^w3
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB2 = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABE = new SWJ_AC_T7(streamAB2, streamE, w4Size, w4Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCE = new SWJ_ABD_T10(streamABE, streamC, w2Size, w2Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABECD = new SWJ_ABCE_T13(streamABCE, streamD, w3Size, w3Slide).run().flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
            @Override
            public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
            }
        });

        // Collect the results into lists
        streamABECD
                .writeAsText(outputPath + "ABECD_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //case BDCAE - [[[B X D]^w3_B x C]^w2_B x A]^w1_B x E]^w4
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBD = new SWJ_AR_T4(streamB, streamD, w3Size, w3Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamBCD = new SWJ_AB_T7(streamBD, streamC, w2Size, w2Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCD2 = new SWJ_AR_T10(streamBCD, streamA, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBDCAE = new SWJ_AC_T13(streamABCD2, streamE, w4Size, w4Slide).run()
                .flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
                    }
                });

        // Collect the results into lists
        streamBDCAE
                .writeAsText(outputPath + "BDCAE_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //case BEDAC - [[[B X E]^w4_B x D]^w3_B x A]^w1_B x C]^w2
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBE = new SWJ_AR_T4(streamB, streamE, w4Size, w4Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamBDE = new SWJ_AB_T7(streamBE, streamD, w3Size, w3Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABDE = new SWJ_AR_T10(streamBDE, streamA, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBEDAC = new SWJ_ABDE_T13(streamABDE, streamC, w2Size, w2Slide).run()
                .flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
                    }
                });

        // Collect the results into lists
        streamBEDAC
                .writeAsText(outputPath + "BEDAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABCDE = envBatch.readTextFile(outputPath + "ABCDE_" + testCase + ".csv").distinct().collect();
        List<String> resultBDCAE = envBatch.readTextFile(outputPath + "BDCAE_" + testCase + ".csv").distinct().collect();
        List<String> resultABECD = envBatch.readTextFile(outputPath + "ABECD_" + testCase + ".csv").distinct().collect();
        List<String> resultBEDAC = envBatch.readTextFile(outputPath + "BEDAC_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABCDE.size(), resultBDCAE.size());
        assertEquals(resultABCDE, resultBDCAE);
        assertEquals(resultABCDE.size(), resultABECD.size());
        assertEquals(resultABCDE, resultABECD);
        assertEquals(resultABCDE.size(), resultBEDAC.size());
        assertEquals(resultABCDE, resultBEDAC);
    }

    @Test
    public void testMixedWindowsS2() throws Exception {
        w1Size = 15;
        w1Slide = 5;
        w2Size = 15;
        w2Slide = 15;
        w3Size = 30;
        w3Slide = 30;
        w4Size = 15;
        w4Slide = 5;
        timePropagation1 = "B";

        String testCase = "5Streams";
        String outputPath = "./src/main/resources/result_4_way_";
        // Execute each join operation
        //default case ABCDE - [[[A X B]^w1_B x C]^w2_B x D]^w3_B x E]^w4
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABC = new SWJ_AC_T7(streamAB, streamC, w2Size, w2Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCD = new SWJ_AC_T10(streamABC, streamD, w3Size, w3Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABCDE = new SWJ_AC_T13(streamABCD, streamE, w4Size, w4Slide).run().flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
            @Override
            public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
            }
        });

        // Collect the results into lists
        streamABCDE
                .writeAsText(outputPath + "ABCDE_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //case ABECD - [[[A X B]^w1_B x E]^w4_B x C]^w2_B x D]^w3
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamAB2 = new SWJ_AR_T4(streamA, streamB, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABE = new SWJ_AC_T7(streamAB2, streamE, w4Size, w4Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCE = new SWJ_ABD_T10(streamABE, streamC, w2Size, w2Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABECD = new SWJ_ABCE_T13(streamABCE, streamD, w3Size, w3Slide).run().flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
            @Override
            public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
            }
        });

        // Collect the results into lists
        streamABECD
                .writeAsText(outputPath + "ABECD_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //case BDCAE - [[[B X D]^w3_B x C]^w2_B x A]^w1_B x E]^w4
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBD = new SWJ_AR_T4(streamB, streamD, w3Size, w3Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamBCD = new SWJ_AB_T7(streamBD, streamC, w2Size, w2Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABCD2 = new SWJ_AR_T10(streamBCD, streamA, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBDCAE = new SWJ_AC_T13(streamABCD2, streamE, w4Size, w4Slide).run()
                .flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
                    }
                });

        // Collect the results into lists
        streamBDCAE
                .writeAsText(outputPath + "BDCAE_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //case BEDAC - [[[B X E]^w4_B x D]^w3_B x A]^w1_B x C]^w2
        DataStream<Tuple7<Integer, Integer, Long, Integer, Integer, Long, Long>> streamBE = new SWJ_AR_T4(streamB, streamE, w4Size, w4Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T7(60000, timePropagation1));

        DataStream<Tuple10<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamBDE = new SWJ_AB_T7(streamBE, streamD, w3Size, w3Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampBR_T10(60000, timePropagation1));

        DataStream<Tuple13<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>> streamABDE = new SWJ_AR_T10(streamBDE, streamA, w1Size, w1Slide).run()
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestampAR_T13(60000, timePropagation1));

        DataStream<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBEDAC = new SWJ_ABDE_T13(streamABDE, streamC, w2Size, w2Slide).run()
                .flatMap(new FlatMapFunction<Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long>, Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(Tuple16<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Long> d1, Collector<Tuple15<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> collector) throws Exception {
                        collector.collect(new Tuple15<>(d1.f0, d1.f1, d1.f2, d1.f3, d1.f4, d1.f5, d1.f6, d1.f7, d1.f8, d1.f9, d1.f10, d1.f11, d1.f12, d1.f13, d1.f14));
                    }
                });

        // Collect the results into lists
        streamBEDAC
                .writeAsText(outputPath + "BEDAC_" + testCase + ".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABCDE = envBatch.readTextFile(outputPath + "ABCDE_" + testCase + ".csv").distinct().collect();
        List<String> resultBDCAE = envBatch.readTextFile(outputPath + "BDCAE_" + testCase + ".csv").distinct().collect();
        List<String> resultABECD = envBatch.readTextFile(outputPath + "ABECD_" + testCase + ".csv").distinct().collect();
        List<String> resultBEDAC = envBatch.readTextFile(outputPath + "BEDAC_" + testCase + ".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABCDE.size(), resultBDCAE.size());
        assertEquals(resultABCDE, resultBDCAE);
        assertEquals(resultABCDE.size(), resultABECD.size());
        assertEquals(resultABCDE, resultABECD);
        assertEquals(resultABCDE.size(), resultBEDAC.size());
        assertEquals(resultABCDE, resultBEDAC);
    }

}
