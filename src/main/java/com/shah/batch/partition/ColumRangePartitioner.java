package com.shah.batch.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ColumRangePartitioner implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        var min=1;
        var max=1000;
        var targetSize=(max-min)/gridSize +1;  //500
        log.info("targetSize :- {}",targetSize);
        var executionContext=new HashMap<String,ExecutionContext>();
        var number=0;
        var start=min;
        var end =start+targetSize-1;

        //1. 1 -- 500 items if gridSize -- 2
        //2. 501 --- 1000 items

        while(start<=end){
            var value=new ExecutionContext();
            executionContext.put("partition"+number,value);
            if(end>=max){
                end=max;
            }
            value.putInt("minValue",start);
            value.putInt("max",end);
            start+=targetSize;
            end+=targetSize;
            number++;
        }
        log.info("partition result :{}", executionContext);
        return executionContext;
    }
}
