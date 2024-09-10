import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import util.Tuple3ParallelSourceFunction;
import util.UDFs;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CommutativeTest_QnVData {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple3<Integer, Integer, Long>> streamA;
    private DataStream<Tuple3<Integer, Integer, Long>> streamB;

    private DataStream<Tuple3<Integer, Integer, Long>> streamC;
    private int w1Size;
    private int w1Slide;

    @Before
    public void setup() {
        // Initialize the StreamExecutionEnvironment
        // Initialize the StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        String file = "./src/main/resources/QnV_R2000070.csv";
        String filePM = "./src/main/resources/luftdaten_11245.csv";
        // the number of keys, should be equals or more as parallelism
        Integer para = 2; // this is 16/3 TODO make it automatic
        Integer numberOfKeys = 5;
        Integer velFilter = 90;
        Integer quaFilter = 80;
        Integer pm10Filter = 20;
        // we except minutes
        long throughput = 1000; // we do it very slowly to be sure system grap it all

        // Create test streams A, B, C with some example data
        streamA = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "V"))
                .setParallelism(para)
                .filter(t -> t.f1 > velFilter )
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamB = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "Q"))
                .setParallelism(para)
                .filter(t -> t.f1 > quaFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamC = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput))
                .setParallelism(para)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));

    }

    @Test
    //Case C1: s < l
    public void testCaseC1() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        String testCase = "C1";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new SWJ_AB(streamA, streamB, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new SWJ_BA(streamA, streamB, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC =
                new SWJ_AB(streamA, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCA =
                new SWJ_BA(streamA, streamC, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC =
                new SWJ_AB(streamB, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCB =
                new SWJ_BA(streamB, streamC, w1Size, w1Slide).run();

        String outputPath = "./src/main/resources/result_Commu_SWJ_";
        // Collect the results into lists
        streamAB
                .writeAsText(outputPath + "AB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBA
                .writeAsText(outputPath + "BA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamAC
                .writeAsText(outputPath + "AC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCA
                .writeAsText(outputPath + "CA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamBC
                .writeAsText(outputPath + "BC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCB
                .writeAsText(outputPath + "CB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultAB = envBatch.readTextFile(outputPath + "AB_"+testCase+".csv").distinct().collect();
        List<String> resultBA = envBatch.readTextFile(outputPath + "BA_"+testCase+".csv").distinct().collect();
        List<String> resultAC = envBatch.readTextFile(outputPath + "AC_"+testCase+".csv").distinct().collect();
        List<String> resultCA = envBatch.readTextFile(outputPath + "CA_"+testCase+".csv").distinct().collect();
        List<String> resultBC = envBatch.readTextFile(outputPath + "BC_"+testCase+".csv").distinct().collect();
        List<String> resultCB = envBatch.readTextFile(outputPath + "CB_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultAB.size(), resultBA.size());
        assertEquals(resultAB,resultBA);
        assertEquals(resultAC.size(), resultCA.size());
        assertEquals(resultAC,resultCA);
        assertEquals(resultBC.size(), resultCB.size());
        assertEquals(resultBC,resultCB);
    }

    @Test
    //Case C1: s < l
    public void testCaseC1_s_gt_l() throws Exception {
        w1Size = 5;
        w1Slide = 10;
        String testCase = "C1_s_gt_l";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new SWJ_AB(streamA, streamB, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new SWJ_BA(streamA, streamB, w1Size, w1Slide).run();

        String outputPath = "./src/main/resources/result_Commu_SWJ_";
        // Collect the results into lists
        streamAB
                .writeAsText(outputPath + "AB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBA
                .writeAsText(outputPath + "BA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultAB = envBatch.readTextFile(outputPath + "AB_"+testCase+".csv").distinct().collect();
        List<String> resultBA = envBatch.readTextFile(outputPath + "BA_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultAB.size(), resultBA.size());
        assertEquals(resultAB,resultBA);
    }

    @Test
    //Case C2: Session Windows
    public void testCaseC2() throws Exception {
        w1Size = 5;
        String testCase = "C2";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new SeWJ_AB(streamA, streamB, w1Size).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new SeWJ_BA(streamA, streamB, w1Size).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC =
                new SeWJ_AB(streamA, streamC, w1Size).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCA =
                new SeWJ_BA(streamA, streamC, w1Size).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC =
                new SeWJ_AB(streamB, streamC, w1Size).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCB =
                new SeWJ_BA(streamB, streamC, w1Size).run();

        String outputPath = "./src/main/resources/result_Commu_SeWJ_";
        // Collect the results into lists
        streamAB
                .writeAsText(outputPath + "AB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBA
                .writeAsText(outputPath + "BA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamAC
                .writeAsText(outputPath + "AC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCA
                .writeAsText(outputPath + "CA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamBC
                .writeAsText(outputPath + "BC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCB
                .writeAsText(outputPath + "CB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultAB = envBatch.readTextFile(outputPath + "AB_"+testCase+".csv").distinct().collect();
        List<String> resultBA = envBatch.readTextFile(outputPath + "BA_"+testCase+".csv").distinct().collect();
        List<String> resultAC = envBatch.readTextFile(outputPath + "AC_"+testCase+".csv").distinct().collect();
        List<String> resultCA = envBatch.readTextFile(outputPath + "CA_"+testCase+".csv").distinct().collect();
        List<String> resultBC = envBatch.readTextFile(outputPath + "BC_"+testCase+".csv").distinct().collect();
        List<String> resultCB = envBatch.readTextFile(outputPath + "CB_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultAB.size(), resultBA.size());
        assertEquals(resultAB,resultBA);
        assertEquals(resultAC.size(), resultCA.size());
        assertEquals(resultAC,resultCA);
        assertEquals(resultBC.size(), resultCB.size());
        assertEquals(resultBC,resultCB);
    }

    @Test
    //Case C3 IVJ equal boundaries
    public void testCaseC3() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        String testCase = "C3";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new IVJ_AB(streamA, streamB, w1Size, w1Size).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new IVJ_BA(streamA, streamB, w1Size, w1Size).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC =
                new IVJ_AB(streamA, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCA =
                new IVJ_BA(streamA, streamC, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC =
                new IVJ_AB(streamB, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCB =
                new IVJ_BA(streamB, streamC, w1Size, w1Slide).run();

        String outputPath = "./src/main/resources/result_Commu_IVJ_";
        // Collect the results into lists
        streamAB
                .writeAsText(outputPath + "AB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBA
                .writeAsText(outputPath + "BA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamAC
                .writeAsText(outputPath + "AC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCA
                .writeAsText(outputPath + "CA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamBC
                .writeAsText(outputPath + "BC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCB
                .writeAsText(outputPath + "CB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultAB = envBatch.readTextFile(outputPath + "AB_"+testCase+".csv").distinct().collect();
        List<String> resultBA = envBatch.readTextFile(outputPath + "BA_"+testCase+".csv").distinct().collect();
        List<String> resultAC = envBatch.readTextFile(outputPath + "AC_"+testCase+".csv").distinct().collect();
        List<String> resultCA = envBatch.readTextFile(outputPath + "CA_"+testCase+".csv").distinct().collect();
        List<String> resultBC = envBatch.readTextFile(outputPath + "BC_"+testCase+".csv").distinct().collect();
        List<String> resultCB = envBatch.readTextFile(outputPath + "CB_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultAB.size(), resultBA.size());
        assertEquals(resultAB,resultBA);
        assertEquals(resultAC.size(), resultCA.size());
        assertEquals(resultAC,resultCA);
        assertEquals(resultBC.size(), resultCB.size());
        assertEquals(resultBC,resultCB);
    }


    @Test
    //Case C3 IVJ equal boundaries
    public void testCaseC4_lB_lt_uB() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lowerBound
        w1Slide = 0; // upperBound
        String testCase = "C3";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new IVJ_AB(streamA, streamB, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new IVJ_BA(streamA, streamB, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC =
                new IVJ_AB(streamA, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCA =
                new IVJ_BA(streamA, streamC, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC =
                new IVJ_AB(streamB, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCB =
                new IVJ_BA(streamB, streamC, w1Size, w1Slide).run();

        String outputPath = "./src/main/resources/result_Commu_SWJ_";
        // Collect the results into lists
        streamAB
                .writeAsText(outputPath + "AB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBA
                .writeAsText(outputPath + "BA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamAC
                .writeAsText(outputPath + "AC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCA
                .writeAsText(outputPath + "CA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamBC
                .writeAsText(outputPath + "BC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCB
                .writeAsText(outputPath + "CB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultAB = envBatch.readTextFile(outputPath + "AB_"+testCase+".csv").distinct().collect();
        List<String> resultBA = envBatch.readTextFile(outputPath + "BA_"+testCase+".csv").distinct().collect();
        List<String> resultAC = envBatch.readTextFile(outputPath + "AC_"+testCase+".csv").distinct().collect();
        List<String> resultCA = envBatch.readTextFile(outputPath + "CA_"+testCase+".csv").distinct().collect();
        List<String> resultBC = envBatch.readTextFile(outputPath + "BC_"+testCase+".csv").distinct().collect();
        List<String> resultCB = envBatch.readTextFile(outputPath + "CB_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultAB,resultBA);
        assertNotEquals(resultAC,resultCA);
        assertNotEquals(resultBC,resultCB);
    }

    @Test
    //Case C3 IVJ equal boundaries
    public void testCaseC4_lB_gt_uB() throws Exception {
        // Set up the testing environment
        w1Size = 0; // lowerBound
        w1Slide = 10; // upperBound
        String testCase = "C3_lB_gt_uB";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new IVJ_AB(streamA, streamB, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new IVJ_BA(streamA, streamB, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAC =
                new IVJ_AB(streamA, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCA =
                new IVJ_BA(streamA, streamC, w1Size, w1Slide).run();

        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBC =
                new IVJ_AB(streamB, streamC, w1Size, w1Slide).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamCB =
                new IVJ_BA(streamB, streamC, w1Size, w1Slide).run();

        String outputPath = "./src/main/resources/result_Commu_SWJ_";
        // Collect the results into lists
        streamAB
                .writeAsText(outputPath + "AB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBA
                .writeAsText(outputPath + "BA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamAC
                .writeAsText(outputPath + "AC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCA
                .writeAsText(outputPath + "CA_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        streamBC
                .writeAsText(outputPath + "BC_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamCB
                .writeAsText(outputPath + "CB_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultAB = envBatch.readTextFile(outputPath + "AB_"+testCase+".csv").distinct().collect();
        List<String> resultBA = envBatch.readTextFile(outputPath + "BA_"+testCase+".csv").distinct().collect();
        List<String> resultAC = envBatch.readTextFile(outputPath + "AC_"+testCase+".csv").distinct().collect();
        List<String> resultCA = envBatch.readTextFile(outputPath + "CA_"+testCase+".csv").distinct().collect();
        List<String> resultBC = envBatch.readTextFile(outputPath + "BC_"+testCase+".csv").distinct().collect();
        List<String> resultCB = envBatch.readTextFile(outputPath + "CB_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultAB,resultBA);
        assertNotEquals(resultAC,resultCA);
        assertNotEquals(resultBC,resultCB);
    }
}
