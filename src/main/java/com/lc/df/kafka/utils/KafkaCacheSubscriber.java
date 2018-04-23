package com.lc.df.kafka.utils;

import com.lc.df.controlclient.utils.GlobalVariables;
import com.lc.df.controlclient.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.aop.framework.adapter.GlobalAdvisorAdapterRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaCacheSubscriber
{
	public static KafkaConsumer<Object,Object> consumer = null;
	public static void initializeKafkaConsumer()
	{
        Properties props = new Properties();
        props.put("bootstrap.servers", GlobalVariables.KAFKA_CONSUMER_NODELIST);
        props.put("group.id", GlobalVariables.KAFKA_CACHE_TOPIC_CONSUMER_GROUP);
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", GlobalVariables.KAFKA_CONSUMER_KEYDESERIALIZER);
        props.put("value.deserializer", GlobalVariables.KAFKA_CONSUMER_VALUEDESERIALIZER);
        props.put("max.poll.records", GlobalVariables.KAFKA_CONSUMER_MAXPOLL);
        props.putAll(GlobalVariables.KAFKA_CONSUMER_ADDITONAL_PROPS);
        
        consumer = new KafkaConsumer<Object,Object>(props);
        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        String[] topicArray = new String[]{GlobalVariables.KAFKA_CATEGORIES_TOPIC,
                                            GlobalVariables.KAFKA_CLASSES_TOPIC,
                                            GlobalVariables.KAFKA_TYPES_TOPIC,
                                            GlobalVariables.KAFKA_EVENTS_TOPIC,
                                            GlobalVariables.KAFKA_MARKETS_TOPIC,
                                            GlobalVariables.KAFKA_SELECTIONS_TOPIC};
        for (int t = 0; t < topicArray.length ; t++) {
            for (int i = 0; i < GlobalVariables.KAFKA_CONSUMER_TOPIC_PARTITIONS; i++) {
                partitions.add(new TopicPartition(topicArray[t], i));
            }
        }
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
	}
	public static List<KafkaRecord> getRecords()
	{
		List<KafkaRecord> recordList = new ArrayList<KafkaRecord>();
        ConsumerRecords<Object, Object> records = consumer.poll(1000);
        for (ConsumerRecord<Object, Object> record : records) 
        {
            KafkaRecord kRecord = new KafkaRecord(record.key(),record.value());
            kRecord.setOffset(record.offset());
            kRecord.setTopic(record.topic());
            kRecord.setPartition(record.partition());
            kRecord.setTimestamp(record.timestamp());
           recordList.add(kRecord);
        }
        if (recordList.size() == 0 && GlobalVariables.KAFKA_CONSUMER_SLEEP > 0)
        {
            Utils.sleep(GlobalVariables.KAFKA_CONSUMER_SLEEP);
        }
        return recordList;
	}
	public static void close(){
	    consumer.close();
    }
}
