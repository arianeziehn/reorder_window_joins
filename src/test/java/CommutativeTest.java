import CorrectnessCheck.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import util.UDFs;

import java.util.List;

import static org.junit.Assert.*;

public class CommutativeTest {

    private StreamExecutionEnvironment env;
    private DataStream<Tuple3<Integer, Integer, Long>> streamA;
    private DataStream<Tuple3<Integer, Integer, Long>> streamB;
    private int w1Size;
    private int w1Slide;

    @Before
    public void setup() {
        // Initialize the StreamExecutionEnvironment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Create test streams A, B, C with some example data
        streamA = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 2, (21*60000L)),
                new Tuple3<>(1, 3, (23*60000L)),
                new Tuple3<>(1, 2, (25*60000L)),
                new Tuple3<>(1, 3, (27*60000L)),
                new Tuple3<>(1, 2, (29*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (41*60000L)),
                new Tuple3<>(1, 3, (43*60000L)),
                new Tuple3<>(1, 2, (45*60000L)),
                new Tuple3<>(1, 3, (47*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (63*60000L)),
                new Tuple3<>(1, 2, (65*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamB = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (4*60000L)),
                new Tuple3<>(1, 2, (6*60000L)),
                new Tuple3<>(1, 3, (8*60000L)),
                new Tuple3<>(1, 2, (10*60000L)),
                new Tuple3<>(1, 3, (12*60000L)),
                new Tuple3<>(1, 2, (14*60000L)),
                new Tuple3<>(1, 3, (16*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 2, (22*60000L)),
                new Tuple3<>(1, 3, (24*60000L)),
                new Tuple3<>(1, 2, (26*60000L)),
                new Tuple3<>(1, 3, (28*60000L)),
                new Tuple3<>(1, 2, (30*60000L)),
                new Tuple3<>(1, 3, (32*60000L)),
                new Tuple3<>(1, 2, (34*60000L)),
                new Tuple3<>(1, 3, (36*60000L)),
                new Tuple3<>(1, 2, (38*60000L)),
                new Tuple3<>(1, 3, (40*60000L)),
                new Tuple3<>(1, 2, (42*60000L)),
                new Tuple3<>(1, 3, (44*60000L)),
                new Tuple3<>(1, 2, (46*60000L)),
                new Tuple3<>(1, 3, (48*60000L)),
                new Tuple3<>(1, 3, (50*60000L)),
                new Tuple3<>(1, 2, (52*60000L)),
                new Tuple3<>(1, 3, (54*60000L)),
                new Tuple3<>(1, 2, (56*60000L)),
                new Tuple3<>(1, 3, (58*60000L)),
                new Tuple3<>(1, 2, (60*60000L)),
                new Tuple3<>(1, 3, (62*60000L)),
                new Tuple3<>(1, 2, (64*60000L)),
                new Tuple3<>(1, 3, (66*60000L)),
                new Tuple3<>(1, 2, (68*60000L)),
                new Tuple3<>(1, 3, (70*60000L)),
                new Tuple3<>(1, 2, (72*60000L)),
                new Tuple3<>(1, 3, (74*60000L)),
                new Tuple3<>(1, 2, (76*60000L)),
                new Tuple3<>(1, 3, (78*60000L)),
                new Tuple3<>(1, 2, (80*60000L)),
                new Tuple3<>(1, 3, (82*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

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
        streamA = env.fromElements(
                new Tuple3<>(1, 2, (1*60000L)),
                new Tuple3<>(1, 3, (3*60000L)),
                new Tuple3<>(1, 2, (5*60000L)),
                new Tuple3<>(1, 3, (7*60000L)),
                new Tuple3<>(1, 2, (9*60000L)),
                new Tuple3<>(1, 3, (11*60000L)),
                new Tuple3<>(1, 2, (13*60000L)),
                new Tuple3<>(1, 3, (15*60000L)),
                new Tuple3<>(1, 2, (17*60000L)),
                new Tuple3<>(1, 3, (19*60000L)),
                new Tuple3<>(1, 3, (31*60000L)),
                new Tuple3<>(1, 2, (33*60000L)),
                new Tuple3<>(1, 3, (35*60000L)),
                new Tuple3<>(1, 2, (37*60000L)),
                new Tuple3<>(1, 3, (39*60000L)),
                new Tuple3<>(1, 2, (49*60000L)),
                new Tuple3<>(1, 3, (51*60000L)),
                new Tuple3<>(1, 2, (53*60000L)),
                new Tuple3<>(1, 3, (55*60000L)),
                new Tuple3<>(1, 2, (57*60000L)),
                new Tuple3<>(1, 3, (59*60000L)),
                new Tuple3<>(1, 2, (61*60000L)),
                new Tuple3<>(1, 3, (67*60000L)),
                new Tuple3<>(1, 2, (69*60000L)),
                new Tuple3<>(1, 3, (71*60000L)),
                new Tuple3<>(1, 2, (73*60000L)),
                new Tuple3<>(1, 3, (75*60000L)),
                new Tuple3<>(1, 2, (77*60000L)),
                new Tuple3<>(1, 3, (79*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        streamB = env.fromElements(
                new Tuple3<>(1, 2, (2*60000L)),
                new Tuple3<>(1, 3, (4*60000L)),
                new Tuple3<>(1, 2, (6*60000L)),
                new Tuple3<>(1, 3, (8*60000L)),
                new Tuple3<>(1, 2, (10*60000L)),
                new Tuple3<>(1, 3, (12*60000L)),
                new Tuple3<>(1, 2, (14*60000L)),
                new Tuple3<>(1, 3, (16*60000L)),
                new Tuple3<>(1, 2, (18*60000L)),
                new Tuple3<>(1, 3, (20*60000L)),
                new Tuple3<>(1, 2, (30*60000L)),
                new Tuple3<>(1, 3, (32*60000L)),
                new Tuple3<>(1, 2, (34*60000L)),
                new Tuple3<>(1, 3, (36*60000L)),
                new Tuple3<>(1, 2, (38*60000L)),
                new Tuple3<>(1, 3, (40*60000L)),
                new Tuple3<>(1, 2, (42*60000L)),
                new Tuple3<>(1, 3, (50*60000L)),
                new Tuple3<>(1, 2, (52*60000L)),
                new Tuple3<>(1, 3, (54*60000L)),
                new Tuple3<>(1, 2, (56*60000L)),
                new Tuple3<>(1, 3, (58*60000L)),
                new Tuple3<>(1, 2, (60*60000L)),
                new Tuple3<>(1, 3, (62*60000L)),
                new Tuple3<>(1, 2, (68*60000L)),
                new Tuple3<>(1, 3, (70*60000L)),
                new Tuple3<>(1, 2, (72*60000L)),
                new Tuple3<>(1, 3, (74*60000L)),
                new Tuple3<>(1, 2, (76*60000L)),
                new Tuple3<>(1, 3, (78*60000L)),
                new Tuple3<>(1, 2, (80*60000L)),
                new Tuple3<>(1, 3, (82*60000L))
        ).assignTimestampsAndWatermarks(new UDFs.ExtractTimestamp(1000));

        w1Size = 4;
        String testCase = "C2";
        // Execute each join operation
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamAB =
                new SeWJ_AB(streamA, streamB, w1Size).run();
        DataStream<Tuple6<Integer, Integer, Long, Integer, Integer, Long>> streamBA =
                new SeWJ_BA(streamA, streamB, w1Size).run();

        String outputPath = "./src/main/resources/result_SeWJ_";
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

        String outputPath = "./src/main/resources/result_IVJ_";
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

        String outputPath = "./src/main/resources/result_IVJ_";
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
        assertNotEquals(resultAB,resultBA);
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

        String outputPath = "./src/main/resources/result_IVJ_";
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
        assertNotEquals(resultAB,resultBA);
    }
}
