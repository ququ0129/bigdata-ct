package org.cg.ct.consumer.bean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.cg.common.bean.Consumer;
import org.cg.common.constant.Names;
import org.cg.ct.consumer.dao.HBaseDao;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CalllogConsumer implements Consumer {

    @Override
    public void consume() throws Exception {
        //kafka configuration
        Properties prop = new Properties();
        try {
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //get flume data
        KafkaConsumer<String,String > consumer = new KafkaConsumer<String, String>(prop);
        //subscribe kafka topic
        consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

        HBaseDao dao = new HBaseDao();
        dao.init();
        //consume
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
                dao.insertData(consumerRecord.value());
                //Calllog log = new Calllog(consumerRecord.value());
                //dao.insertData(log);
            }
        }
    }

    @Override
    public void close() throws IOException {

    }
}
