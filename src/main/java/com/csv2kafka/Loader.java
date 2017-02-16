package com.csv2kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import java.util.Properties;

public class Loader
{
    public KafkaProducer producer;

    public void init()
    {
        Properties p = this.getProps();
        this.producer = new KafkaProducer(p);
    }

    public void load(JSONObject data)
    {
        System.out.println(data.toString());
        ProducerRecord record = new ProducerRecord("kafka-channel-name", data.toString());

        this.producer.send(record);
    }


    private Properties getProps()
    {
        // Todo: setup the maven props plugin and move to config
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        p.setProperty("acks", "all");
        p.setProperty("retries", "0");
        p.setProperty("batch.size", "16384");
        p.setProperty("auto.commit.interval.ms", "1000");
        p.setProperty("longer.ms", "0");
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("block.on.buffer.full", "true");
        return p;
    }

}
