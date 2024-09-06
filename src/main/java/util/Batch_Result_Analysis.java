package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class Batch_Result_Analysis {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // "./src/main/resources/QnV_R2000070_resultQ5_ASP.csv/6"
        //"./src/main/resources/QnV_large_resultQ2_ASP.csv/6"
        //"./src/main/resources/QnV_largeresultQ4_1ASP.csv/7"
        //./src/main/resources/QnV_R2000070_resultQ5_ASP.csv/6
        //String file = "src/main/resources/QnV_large_resultQ7_I2_ASP.csv/11";
        String file = "./src/main/resources/resultSWJ_BCA_A1.csv";
        //String outputPath = "./src/main/resources/QnV_R2000070_resultQ5_NoDupl.csv";

        DataSet<String> in = env.readTextFile(file);

        DataSet<String> in_distinct = in.distinct();

        in_distinct.print();
            //.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
        System.out.println(in_distinct.count());

    }

}