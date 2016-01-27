package com.kafka;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class KafkaConsumer {

    private static ConsumerConnector consumerConnector = null;

    public static void main(String[] args) {
        Properties properties = new Properties();
        //Parametres Consumer :
        //Zookeeper est en charge du pilotage des consommateurs.
        properties.put("zookeeper.connect", "localhost:2181");
        //Pour chaque partition, un seul consommateur appartient a  un groupe lire les messages.
        properties.put("group.id", "group_test");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put("test", new Integer(1));
    
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("test").get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        
        while (it.hasNext()) {
            String line = new String(it.next().message());
            System.out.println(line);

        }
    }

}
