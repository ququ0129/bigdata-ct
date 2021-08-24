package org.cg.ct.consumer;

import org.cg.common.bean.Consumer;
import org.cg.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/*
boot consumer
1.flume to kafka
2.kafka to hbase
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        //create consumer
        Consumer consumer = new CalllogConsumer();

        //consume data
        consumer.consume();

        //close consumer
        consumer.close();
    }
}
