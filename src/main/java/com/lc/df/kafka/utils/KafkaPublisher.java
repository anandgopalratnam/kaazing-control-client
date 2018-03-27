package com.lc.df.kafka.utils;

import java.util.Properties;
import java.util.UUID;

import com.lc.df.controlclient.utils.GlobalVariables;
import com.lc.df.controlclient.utils.Logger;
import com.lc.df.controlclient.utils.Utils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaPublisher
{
    private static Producer<Object, Object> producer;
    private static final String DEFAULT_KEY= "999999999999";
    private static volatile boolean exception = false;
    private static final Callback kafkaCallback = new KafkaCallback(); 
    public static String initializeKafkaProducer() 
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", GlobalVariables.KAFKA_PRODUCER_NODELIST);
        props.put("acks", GlobalVariables.KAFKA_PRODUCER_ACKS);
        props.put("retries", GlobalVariables.KAFKA_PRODUCER_RETRIES);
        props.put("batch.size", GlobalVariables.KAFKA_PRODUCER_BATCHSIZE);
        props.put("linger.ms", GlobalVariables.KAFKA_PRODUCER_LINGER_MS);
        props.put("buffer.memory", GlobalVariables.KAFKA_PRODUCER_BUFFER_MEMORY);
        if (Utils.isValidString(GlobalVariables.KAFKA_PRODUCER_COMPRESSION_TYPE))
        {
        	props.put("compression.type", GlobalVariables.KAFKA_PRODUCER_COMPRESSION_TYPE);
        }
        props.put("max.in.flight.requests.per.connection", "1");
        props.put("key.serializer", GlobalVariables.KAFKA_PRODUCER_KEYSERIALIZER);
        props.put("value.serializer", GlobalVariables.KAFKA_PRODUCER_VALUESERIALIZER);
        props.putAll(GlobalVariables.KAFKA_PRODUCER_ADDITONAL_PROPS);
        producer = new KafkaProducer<Object, Object>(props);
        return "OK";
    }
    public KafkaPublisher()
    {
    	this.getClass().getName();
    }
    /**
     *
     * @param value
     * @return
     */
    public static int publish(String topicName, Object value)
    {
    	return publish(topicName,DEFAULT_KEY,value);
    }
    public static synchronized boolean hasExceptions()
    {
    	return exception;
    }
    public static synchronized void setException(boolean error)
    {
    	exception = error;
    }
    public static int publish(String topicName,Object key,Object value)
    {
        try 
        {
        	if (!hasExceptions())
        	{
//				Logger.logInfoMessage("Sending Key ["+key+"] .........");
				producer.send(new ProducerRecord<Object, Object>(topicName,key,value),kafkaCallback);
				return 0;
        	}
		}
        catch (Exception e) 
		{
			Logger.logErrorMessage("Exception sending to Kafka Destination ["+topicName+"]",e);
		}
	
        return -1;
    }

}
