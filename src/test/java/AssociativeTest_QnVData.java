import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
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

/**
 * Test the Associate Test with real world data. 
 * Attention running the full test takes app. 3 hours, you may increase parallelism if trying on a large server.
 */
public class AssociativeTest_QnVData {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple3<Integer, Integer, Long>> streamA;
    private DataStream<Tuple3<Integer, Integer, Long>> streamB;
    private DataStream<Tuple3<Integer, Integer, Long>> streamC;
    private int w1Size;
    private int w1Slide;
    private int w2Size;
    private int w2Slide;
    private String timePropagation;

    @Before
    public void setup() {
        // Initialize the StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        String file = "./src/main/resources/QnV_R2000070.csv";
        String filePM = "./src/main/resources/luftdaten_11245.csv";
        // the number of keys, should be equals or larger than parallelism of the environment
        Integer para = 2;
        Integer numberOfKeys = 5;
        Integer velFilter = 90;
        Integer quaFilter = 80;
        Integer pm10Filter = 20;
        // we except minutes
        long throughput = 1000;

        // Create test streams A, B, C with some example data
        streamA = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "V"))
                .setParallelism(para)
                .filter(t -> t.f1 > velFilter )
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamB = env.addSource(new Tuple3ParallelSourceFunction(file, numberOfKeys, ",", throughput, "Q"))
                .setParallelism(para)
                .filter(t -> t.f1 > quaFilter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(60000));

        streamC = env.addSource(new Tuple3ParallelSourceFunction(filePM, numberOfKeys, ";", throughput, "PM10"))
                .setParallelism(para)
                .filter(t -> t.f1 > pm10Filter)
                .assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(180000));
    }

    @Test
    //Case A1: W1 = W2, W2.s < W2.l 
    public void CaseA1() throws Exception {
        w1Size = 10;
        w1Slide = 5;
        w2Size = 10;
        w2Slide = 5;
        
        String testCase = "A1_QnV";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A1: W1 = W2, W2.s < W2.l 
    public void CaseA1_outer_edge_case() throws Exception {
        w1Size = 10;
        w1Slide = 9;
        w2Size = 10;
        w2Slide = 9;
        
        String testCase = "A1 ";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A1: W1 = W2, W2.s < W2.l // Proof Section of Paper
    public void CaseA1_outer_edge_case_2() throws Exception {
        w1Size = 20;
        w1Slide = 15;
        w2Size = 20;
        w2Slide = 15;
        
        String testCase = "A1_";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }


    @Test
    //Case A2 W1=W2, W2.s >= W2.l
    public  void CaseA2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 10;
        w2Slide = 15;
        timePropagation = "A";
        String testCase = "A2_";
        // Execute each join operation
        // first we use a in q_1 = ABC_a and q_2 = BCA_a
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC_a.size(), resultBCA_a.size());
        assertEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A2 W1=W2, W2.s >= W2.l
    public  void CaseA2_TWJ() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 10;
        w2Size = 10;
        w2Slide = 10;
        
        String testCase = "A2_";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC_a.size(), resultBCA_a.size());
        assertEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A2 W1=W2, W2.s >= W2.l
    public  void CaseA2_slide_no_divisor() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 13;
        w2Size = 10;
        w2Slide = 13;
       
        String testCase = "A2 ";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // second we use b in q_1 = ABC_b and q_2 = BCA_b
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertEquals(resultABC_a.size(), resultBCA_a.size());
        assertEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A3 W1 != W2, W2.s < W2.l and W1 is non overlapping
    public  void CaseA3_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 30;
        w1Slide = 15;
        w2Size = 15;
        w2Slide = 5;
       
        String testCase = "A3 _w1_gt_w2";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A3 W1 != W2, W2.s < W2.l and W1 is non overlapping
    public  void CaseA3_w1_lth_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 5;
        w2Size = 15;
        w2Slide = 5;

        String testCase = "A3 _w1_lt_w2";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A3 W1 != W2, W2.s < W2.l and W1.l >= W2.l
    public  void CaseA3_w1_non_overlapping() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 30;
        w2Size = 15;
        w2Slide = 5;

        String testCase = "A3 ";
        // Execute each join operation
        // first we use a as time propagation in q_1 = ABC_a and q_2 = BCA_a
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_eq_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 30;
        w2Size = 15;
        w2Slide = 30;
        
        String testCase = "A4 ";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_eq_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 15;
        w2Slide = 20;

        String testCase = "A4 ";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_eq_w1_lt_w2_s1_gt_s2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 20;
        w2Size = 15;
        w2Slide = 15;

        String testCase = "A4 ";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_2TWJs_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 20;
        w2Size = 15;
        w2Slide = 15;

        String testCase = "A4_TWJ_w1_gt_w2";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_neq_w1_gt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 25;
        w2Size = 15;
        w2Slide = 15;

        String testCase = "A4_w1_gt_w2";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_2TWJs_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 10;
        w2Size = 25;
        w2Slide = 25;

        String testCase = "A4_2TWJs";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4: W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_neq_w1_lt_w2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 15;
        w2Size = 25;
        w2Slide = 35;

        String testCase = "A4_w1_lt_w2";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_neq_w1_lt_w2_s1_lt_s2() throws Exception {
        // Set up the testing environment
        w1Size = 10;
        w1Slide = 45;
        w2Size = 33;
        w2Slide = 35;

        String testCase = "A4_w1_lt_w2";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A4 W1 != W2, W2.s >= W2.l and W1.l >= W2.l
    public  void CaseA4_slide_neq_w2_gt_w1() throws Exception {
        // Set up the testing environment
        w1Size = 20;
        w1Slide = 20;
        w2Size = 20;
        w2Slide = 30;
        
        String testCase = "A4_w2_gt_w1";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SWJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SWJ_BC_w2_A_w1(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_SWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A5: Session Windows
    public void CaseA5() throws Exception {
        w1Size = 5;
        w2Size = 5;
        timePropagation = "A";
        String testCase = "A5_QnV";
        // Execute each join operation
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new SeWJ_ABC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new SeWJ_BCA(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();

        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new SeWJ_ABC(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new SeWJ_BCA(streamA, streamB, streamC, w1Size, w2Size, timePropagation).run();

        String outputPath = "./src/main/resources/result_SeWJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the result
        assertNotEquals(resultABC_a,resultBCA_a);
        assertNotEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A6 IVJ lB = uB
    public void CaseA6_w1_eq_w2() throws Exception {
        // Set up the testing environment
        w1Size = -10; // lB W1
        w1Slide = 10; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 10; // uB W1

        timePropagation = "A";
        String testCase = "A6_QnV_lB_eq_uB_w1_eq_w2";
        // Execute each join operation  with timestamp propagation of A
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A6 IVJ lB = uB
    public  void CaseA6_w1_neq_w2() throws Exception {
        // Set up the testing environment
        w1Size = -5; // lB W1
        w1Slide = 5; // uB W1
        w2Size = -10; // lB W1
        w2Slide = 10; // uB W1

        String testCase = "A6 _QnV_lB_eq_uB_w1_eq_w2";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertEquals(resultABC_b.size(), resultBCA_b.size());
        assertEquals(resultABC_b,resultBCA_b);
    }

    @Test
    //Case A6 IVJ lB = uB
    public  void CaseA7() throws Exception {
        // Set up the testing environment
        w1Size =  0; // lB W1
        w1Slide = 5; // uB W1
        w2Size = -7; // lB W1
        w2Slide = 7; // uB W1

        String testCase = "A7_QnV_lB_eq_uB_w1_neq_w2";
        // Execute each join operation  with timestamp propagation of A
        timePropagation = "A";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_a =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_a =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        // Execute each join operation  with timestamp propagation of B
        timePropagation = "B";
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamABC_b =
                new IVJ_ABC(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();
        DataStream<Tuple9<Integer, Integer, Long, Integer, Integer, Long, Integer, Integer, Long>> streamBCA_b =
                new IVJ_BCA(streamA, streamB, streamC, w1Size, w1Slide, w2Size, w2Slide, timePropagation).run();

        String outputPath = "./src/main/resources/result_IVJ_";
        // Collect the results into lists
        streamABC_a
                .writeAsText(outputPath + "ABC_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_a
                .writeAsText(outputPath + "BCA_a_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamABC_b
                .writeAsText(outputPath + "ABC_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        streamBCA_b
                .writeAsText(outputPath + "BCA_b_"+testCase+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

        final ExecutionEnvironment envBatch = ExecutionEnvironment.getExecutionEnvironment();
        envBatch.setParallelism(1);

        List<String> resultABC_a = envBatch.readTextFile(outputPath + "ABC_a_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_a = envBatch.readTextFile(outputPath + "BCA_a_"+testCase+".csv").distinct().collect();
        List<String> resultABC_b = envBatch.readTextFile(outputPath + "ABC_b_"+testCase+".csv").distinct().collect();
        List<String> resultBCA_b = envBatch.readTextFile(outputPath + "BCA_b_"+testCase+".csv").distinct().collect();

        // Compare the results
        assertNotEquals(resultABC_a,resultBCA_a);
        assertNotEquals(resultABC_b,resultBCA_b);
    }
}
