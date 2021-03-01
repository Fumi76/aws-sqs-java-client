package com.example;

public interface BaseRunnable extends Runnable {

    Long getSum();
    Integer getActualCount();
    Long getStart();
    Long getEnd();
    String getResult();
    Integer getErrorCount();
}
