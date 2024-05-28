package util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;


public class NASDAQSourceFunction implements SourceFunction<KeyedDataPointNASDAQ> {
    private volatile boolean isRunning = true;

    private String file;  // the source file

    /*
     Read the wave file from the .csv file
     */
    public NASDAQSourceFunction(String fileName) {
        this.file = fileName;
    }

    public void run(SourceContext<KeyedDataPointNASDAQ> sourceContext) throws Exception {

        // the data look like this...
        // <ticker>,<date>,  <open>,<high>, <low>, < close>,<vol>
        //  AAPL,201010110900,295.01,295.05,294.82,294.82,5235
        //AAPL,201010110905,294.81,294.9,294.8,294.85,7441
        //AAPL,201010110910,294.85,294.98,294.84,294.85,4268


        // open each file and get line by line
        try {

            Scanner scan;
            File file = new File(this.file);
            scan = new Scanner(file);
            boolean header = true;

            while (scan.hasNext()) {


                String rawData = scan.nextLine();
                String[] data = rawData.split(",");

                if(!data[0].equals("<ticker>")) {

                    String key = data[0].trim();
                    String timestampString = data[1].trim();

                    String timestampStringFinal = timestampString.substring(0, 4) + "-" + timestampString.substring(4, 6) + "-" + timestampString.substring(6, 8) + " " + timestampString.substring(8, 10) + ":" + timestampString.substring(10);

                    //System.out.println("Before formatting: " + timestampStringFinal);

                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
                    LocalDateTime timestampDT = LocalDateTime.parse(timestampStringFinal, dtf);
                    //System.out.println("Before formatting2: " + timestampDT);
                    long timestamp = timestampDT.atZone(ZoneId.systemDefault())
                            .toInstant().toEpochMilli();
                    double open = Double.parseDouble(data[2].trim());
                    double high = Double.parseDouble(data[3].trim());
                    double low = Double.parseDouble(data[4].trim());
                    double close = Double.parseDouble(data[5].trim());
                    int vol = Integer.parseInt(data[6].trim());

                    KeyedDataPointNASDAQ event = new KeyedDataPointNASDAQ(key, timestamp, open, high, low, close, vol);

                    sourceContext.collect(event);

                }
            }
            scan.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void cancel() {
        this.isRunning = false;
    }
}
