package com.example;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.amazonaws.services.sqs.AmazonSQS;

public class SelectTask implements BaseRunnable {

    private Integer maxId;
    private Long duration;
    private Long sum = 0L;
    private volatile Integer actualCount = 0;
    private Long start;
    private Long end;
    private Integer count;
    private String result;
    private Integer errorCount = 0;
    private long previousSuccessTime = 0;
    private long failedTime = 0;
    private boolean isSuccess = true;
	
    public SelectTask(AmazonSQS sqs
            , Integer paramMaxId
            , Long paramDuration
            , Integer paramCount){

        this.maxId = paramMaxId;
        this.duration = paramDuration;
        this.count = paramCount;
    }

    @Override
    public void run() {
        long d = duration * 60L * 1000L * 1000L * 1000L;
        long s0 = System.currentTimeMillis();
        long s1 = System.nanoTime();
        long p2;
        long p3;

        while(true) {
            long p1 = System.nanoTime();
            select();
            p2 = System.nanoTime();
            actualCount++;
            sum += (p2 - p1);
            if(count > 0 && actualCount == count){
                break;
            }
            if((p2 - s1) > d){
                break;
            }
        }
        p3 = System.currentTimeMillis();

        BigDecimal tps = new BigDecimal(actualCount).divide(new BigDecimal(sum).divide(new BigDecimal(Main.ONESEC), 9, RoundingMode.HALF_UP), 9, RoundingMode.HALF_UP);
        BigDecimal spt = new BigDecimal(sum).divide(new BigDecimal(actualCount), 9, RoundingMode.HALF_UP).divide(new BigDecimal(Main.ONESEC), 9, RoundingMode.HALF_UP);
        result = "Select " + tps.toPlainString()
                + " t/s | "+spt.toPlainString()
                +" sec per t | total nanos "+sum+" / actual count "+ actualCount + " | "
                + Thread.currentThread().getName();

        this.start = s0;
        this.end = p3;
    }

    private void select() {
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
        return errorCount;
    }
}
