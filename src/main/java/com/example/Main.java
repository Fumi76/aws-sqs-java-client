package com.example;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class Main {

    public static final long ONESEC = 1 * 1000 * 1000 * 1000;

    private static final Properties properties;

    static {
        properties = new Properties();
        try {
            properties.load(Files.newBufferedReader(Paths.get("./sqs_client.properties")
                    , StandardCharsets.UTF_8));
        } catch (IOException e) {
            // ファイル読み込みに失敗
            System.out.println(String.format("ファイルの読み込みに失敗しました。ファイル名:%s", "sqs_client.properties"));
        }
    }

    public static void main(String[] args) throws Exception {

        String workloadType = args[0];
        Long duration = Long.parseLong(args[1]);
        System.out.println("duration="+duration);
        Integer concurrency = Integer.parseInt(args[2]);
        System.out.println("concurrency="+concurrency);
        Integer count = Integer.parseInt(args[3]);
        System.out.println("count="+count);
        Integer col1ValueLength = Integer.parseInt(args[4]);
    	System.out.println("col1ValueLength="+col1ValueLength);
        Integer maxId = -1;
        if("select".equals(workloadType)) {
            maxId = Integer.parseInt(args[5]);
            System.out.println("maxId="+maxId);
        }

        // javaコマンドの引数に-DclientName=myClient1を指定
        String clientName = System.getProperty("clientName");
        
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(1024);
        config.setUseTcpKeepAlive(true);

		AmazonSQSClientBuilder builder = AmazonSQSClientBuilder.standard();
		builder.setClientConfiguration(config);
		builder.setCredentials(new InstanceProfileCredentialsProvider(false));
        final AmazonSQS sqs = builder.build();
        
        final String myQueueUrl = properties.getProperty("queue_url");
        System.out.println("queue_url="+myQueueUrl);
		
        List<BaseRunnable> list = new ArrayList<>();

        for(int i=0; i < concurrency; i++){
            if("select".equals(workloadType)) {
                //list.add(new SelectTask(table, maxId, duration, count));
            }else if("insert".equals(workloadType)){
                list.add(new InsertTask(sqs, duration, count, col1ValueLength, clientName, myQueueUrl));
            }else if("tx".equals(workloadType)) {
            	//list.add(new TxTask(table, duration, count, col1ValueLength, clientName));
            }
        }

        ExecutorService es = Executors.newFixedThreadPool(concurrency);

        List<Future<?>> futures = new ArrayList<>();

        for(BaseRunnable runnable : list){
            futures.add(es.submit(runnable));
        }

        es.shutdown();

        for(Future<?> future : futures){
            future.get();
        }

        long sum = 0;
        long actualCount = 0;
        long start1 = Long.MAX_VALUE;
        long end1 = 0;
        int errorCount = 0;
        List<String> results = new ArrayList<>();

        for(BaseRunnable runnable : list){
            sum += runnable.getSum();
            actualCount += runnable.getActualCount();

            if(runnable.getStart() < start1){
                start1 = runnable.getStart();
            }

            if(runnable.getEnd() > end1){
                end1 = runnable.getEnd();
            }

            results.add(runnable.getResult());

            errorCount += runnable.getErrorCount();
        }

        try {
            Files.writeString(Paths.get("result_by_thread.txt"), "");
        }catch(Exception e){
            throw new RuntimeException(e);
        }

        Files.write(Paths.get("result_by_thread.txt"),
                results,
                Charset.forName("UTF-8"),
                StandardOpenOption.APPEND);

        long sum2 = end1 - start1;

        BigDecimal spt = new BigDecimal(sum).divide(new BigDecimal(actualCount), 9, RoundingMode.HALF_UP).divide(new BigDecimal(ONESEC), 9, RoundingMode.HALF_UP);
        //BigDecimal tps = new BigDecimal(1).divide(spt, 10, RoundingMode.HALF_UP);
        //System.out.println("Overall1 " + workloadType + " " + tps.toPlainString() + " t/sec | "+spt.toPlainString() +" sec/t | "+sum+" / "+ actualCount);

        BigDecimal tps2 = new BigDecimal(actualCount).divide(new BigDecimal(sum2).divide(new BigDecimal(1000), 9, RoundingMode.HALF_UP), 9, RoundingMode.HALF_UP);
        BigDecimal spt2 = new BigDecimal(sum2).divide(new BigDecimal(actualCount), 12, RoundingMode.HALF_UP).divide(new BigDecimal(1000), 9, RoundingMode.HALF_UP);

        String result = "Overall2\t" + workloadType
                + "\tduration\t" + duration + "\t"
                + "concurrency\t" + concurrency
                + "\tcount\t" + count
                //+ "\tminPool\t" + minPool
                //+ "\tmaxId\t" + maxId
                + "\tcol1ValueLength\t" + col1ValueLength
                + "\terrors\t" + errorCount
                + "\tt/sec\t" + tps2.toPlainString()
                + "\tspt2 sec/t\t"+spt2.toPlainString()
                + "\tspt1 sec/t\t"+spt.toPlainString()
                + "\ttotal millis\t"
                + sum2+"\tactual count\t"+ actualCount
                + "\t" + start1 + "\t" + end1 + "\n";

        try {
            Files.writeString(Paths.get("result.txt"), result);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        
        System.out.println(System.currentTimeMillis()+" Finished");
    }
}
