package com.example;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.lang3.RandomStringUtils;

import com.amazonaws.services.sqs.AmazonSQS;

public class TxTask implements BaseRunnable {

    private Long duration;
    private long sum = 0;
    private int actualCount = 0;
    private Long start;
    private Long end;
    private Integer count;
    private String result;
    private Integer col1ValueLength;
    private long previousSuccessTime = 0;
    private long failedTime = 0;
    private boolean isSuccess = true;
    private String clientName;

    public TxTask(AmazonSQS sqs
            , Long paramDuration
            , Integer paramCount
            , Integer paramCol1ValueLength
            , String paramClientName){

        this.duration = paramDuration;
        this.count = paramCount;
        this.col1ValueLength = paramCol1ValueLength;
        this.clientName = paramClientName;
    }

    @Override
    public void run() {

        String random = RandomStringUtils.randomAlphanumeric(col1ValueLength);
        long d = duration * 60L * 1000L * 1000L * 1000L;

        long s0 = System.currentTimeMillis();
        long s1 = System.nanoTime();

        long p2;
        long p3;

        while(true) {
            long p1 = System.nanoTime();
            process(actualCount, random);
            p2 = System.nanoTime();
            actualCount++;
            sum += (p2 - p1);
            if(count > 0 && actualCount == count){
                break;
            }
            if((p2-s1) > d){
                break;
            }
        }
        p3 = System.currentTimeMillis();
        BigDecimal tps = new BigDecimal(actualCount).divide(new BigDecimal(sum).divide(new BigDecimal(Main.ONESEC), 9, RoundingMode.HALF_UP), 9, RoundingMode.HALF_UP);
        BigDecimal spt = new BigDecimal(sum).divide(new BigDecimal(actualCount), 9, RoundingMode.HALF_UP).divide(new BigDecimal(Main.ONESEC), 9, RoundingMode.HALF_UP); // 秒にしている
        result = "Insert "+tps.toPlainString() + " t/s | "+spt.toPlainString() +" sec/t | total nano "+sum+" / "+ actualCount + " | " + Thread.currentThread().getName();

        this.start = s0;
        this.end = p3;
    }

    private void process(Integer index, String randomString) {
    }

    public Long getSum(){
        return sum;
    }

    public Integer getActualCount(){
        return actualCount;
    }

    public Long getStart(){
        return start;
    }

    public Long getEnd(){
        return end;
    }

    public String getResult(){
        return result;
    }

    public Integer getErrorCount(){
        return 0;
    }
}
