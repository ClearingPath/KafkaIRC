/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lang.pat.kafkairc;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author ClearingPath
 */
public class Producer {
    private Properties props;            
    private KafkaProducer<String,String> producer;
    private String key = "lang.pat.key";
    
    
    public Producer(){
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producer = new KafkaProducer<String,String>(props);
    }
    
    public void send(String Message){
        for (String c : ClientMain.ChannelList){
            send(Message,c);
        }
    }
    
    public void send(String Message, String Channel){
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(Channel, key, Message);
        producer.send(producerRecord);
    }
    
    @Override
    protected void finalize() throws Throwable{
        producer.close();
        super.finalize();
    }
    
}
