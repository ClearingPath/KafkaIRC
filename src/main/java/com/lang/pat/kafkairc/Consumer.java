/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lang.pat.kafkairc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author ClearingPath
 */
public class Consumer implements Runnable {

  private KafkaStream m_stream;
  private ExecutorService executor;
  private final ConsumerConnector consumer;
  private final String Topic;
  public boolean listen;

  public Consumer(String Topicname) {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    Topic = Topicname;
    listen = true;
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(Topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    m_stream = consumerMap.get(Topic).get(0);
    System.out.println("Consumer for " + Topicname + " created");
  }

//  public Consumer(){
//    
//  }
  private static ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", ClientMain.HOSTNAME + ":" + ClientMain.PORT);
    props.put("group.id", ClientMain.USERNAME);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);
  }

  public void shutdown() {
    if (consumer != null) {
      consumer.shutdown();
    }
    if (executor != null) {
      executor.shutdown();
    }
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
	System.out.println("! Error clean close, exiting uncleanly.");
      }
    } catch (InterruptedException e) {
      System.out.println("! Process interrupted during shutdown, exiting uncleanly.");
    }
  }

  public void consume() throws ParseException, InterruptedException, Throwable {
    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
    JSONParser parse = new JSONParser();
    SimpleDateFormat formatDate = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
    while (it.hasNext() && listen) {
        if (ClientMain.ChannelList.contains(Topic)) {
            String temp = new String(it.next().message());
            if (!temp.contains("{")){
                System.out.println(temp);
            } else {
                JSONObject JSONMessage = (JSONObject) parse.parse(temp);

                Date sendDat = new Date();
                sendDat.setTime((long) JSONMessage.get("timestamp"));
                System.out.println("[" + Topic + "] "
                      + "[" + JSONMessage.get("username")
                      + "] " + JSONMessage.get("message")
                      + " || " + formatDate.format(sendDat));

                if (JSONMessage.get("username").toString().equals(ClientMain.USERNAME)) {
                      if (!JSONMessage.get("token").toString().equals(ClientMain.token)) {
                          System.out.print("! Duplicate username, please change to avoid conflict!");
                      }
                }
            }
            System.out.print("> ");
        } else {
          listen = false;
          break;
        }
    }
  }

  @Override
  public void run() {
      try {
          consume();
      } catch (ParseException ex) {
          Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
      } catch (InterruptedException ex) {
          Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
      } catch (Throwable ex) {
          Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
      }
  }
}
