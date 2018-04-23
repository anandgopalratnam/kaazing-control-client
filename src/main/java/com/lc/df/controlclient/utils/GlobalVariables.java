package com.lc.df.controlclient.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GlobalVariables 
{

	public static String KAFKA_CONSUMER_NODELIST = null;
	public static String KAFKA_CONSUMER_GROUP = null;
	public static String KAFKA_CONSUMER_MAXPOLL = null;
	public static long KAFKA_CONSUMER_SLEEP = -1;
	public static String KAFKA_CONSUMER_TOPIC = null;
	public static int KAFKA_CONSUMER_TOPIC_PARTITIONS = -1;
	public static String KAFKA_CONSUMER_OFFSET_RESET = null;
	public static String KAFKA_CONSUMER_KEYDESERIALIZER = null;
	public static String KAFKA_CONSUMER_VALUEDESERIALIZER = null;
	public static String KAFKA_CONSUMER_FILTER_KEYLIST = null;
	public static ArrayList<String> KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST = new ArrayList<String>();
	public static Map<String,String> KAFKA_CONSUMER_ADDITONAL_PROPS = new HashMap<String, String>();

	public static String KAFKA_CACHE_TOPIC_CONSUMER_GROUP = null;
	
	public static String KAFKA_PRODUCER_NODELIST = null;
	public static String KAFKA_PRODUCER_ACKS = null;
	public static int KAFKA_PRODUCER_RETRIES = -1;
	public static int KAFKA_PRODUCER_BATCHSIZE = -1;
	public static int KAFKA_PRODUCER_LINGER_MS = -1;
	public static long KAFKA_PRODUCER_BUFFER_MEMORY = Long.MAX_VALUE;
	public static String KAFKA_PRODUCER_COMPRESSION_TYPE = "";
	public static String KAFKA_PRODUCER_TOPIC = null;
	public static String KAFKA_PRODUCER_KEYSERIALIZER = null;
	public static String KAFKA_PRODUCER_VALUESERIALIZER = null;
	public static Map<String,String> KAFKA_PRODUCER_ADDITONAL_PROPS = new HashMap<String, String>();

	public static String KAFKA_CATEGORIES_TOPIC=null;
	public static String KAFKA_CLASSES_TOPIC=null;
	public static String KAFKA_TYPES_TOPIC=null;
	public static String KAFKA_EVENTS_TOPIC=null;
	public static String KAFKA_MARKETS_TOPIC=null;
	public static String KAFKA_SELECTIONS_TOPIC=null;
	public static String KAFKA_INPLAY_TOPIC=null;

	
	public static void setVariables(String name)
	{
		try 
		{
			PropertiesUtil.loadProperties(name);
			
			KAFKA_PRODUCER_NODELIST = PropertiesUtil.getProperty("kafka.producer.bootstrap.servers");
			Logger.logInfoMessage("Set value for kafka.producer.bootstrap.servers = ["+KAFKA_PRODUCER_NODELIST+"]");
			KAFKA_PRODUCER_ACKS = PropertiesUtil.getProperty("kafka.producer.acks");
			Logger.logInfoMessage("Set value for kafka.producer.acks = ["+KAFKA_PRODUCER_ACKS+"]");
			KAFKA_PRODUCER_RETRIES = Integer.parseInt(PropertiesUtil.getProperty("kafka.producer.retries"));
			Logger.logInfoMessage("Set value for kafka.producer.retries = ["+KAFKA_PRODUCER_RETRIES+"]");
			KAFKA_PRODUCER_BATCHSIZE = Integer.parseInt(PropertiesUtil.getProperty("kafka.producer.batch.size"));
			Logger.logInfoMessage("Set value for kafka.producer.batch.size = ["+KAFKA_PRODUCER_BATCHSIZE+"]");
			KAFKA_PRODUCER_LINGER_MS = Integer.parseInt(PropertiesUtil.getProperty("kafka.producer.linger.ms"));
			Logger.logInfoMessage("Set value for kafka.producer.linger.ms = ["+KAFKA_PRODUCER_LINGER_MS+"]");
			KAFKA_PRODUCER_BUFFER_MEMORY = Long.parseLong(PropertiesUtil.getProperty("kafka.producer.buffer.memory"));
			Logger.logInfoMessage("Set value for kafka.producer.buffer.memory = ["+KAFKA_PRODUCER_BUFFER_MEMORY+"]");
			KAFKA_PRODUCER_TOPIC = PropertiesUtil.getProperty("kafka.producer.topic");
			Logger.logInfoMessage("Set value for kafka.producer.topic = ["+KAFKA_PRODUCER_TOPIC+"]");
			KAFKA_PRODUCER_COMPRESSION_TYPE = PropertiesUtil.getProperty("kafka.producer.compressiontype");
			Logger.logInfoMessage("Set value for kafka.producer.compressiontype = ["+KAFKA_PRODUCER_COMPRESSION_TYPE+"]");
			KAFKA_PRODUCER_KEYSERIALIZER = PropertiesUtil.getProperty("kafka.producer.key-serializer");
			Logger.logInfoMessage("Set value for kafka.producer.key-serializer = ["+KAFKA_PRODUCER_KEYSERIALIZER+"]");
			KAFKA_PRODUCER_VALUESERIALIZER = PropertiesUtil.getProperty("kafka.producer.value-serializer");
			Logger.logInfoMessage("Set value for kafka.producer.value-serializer = ["+KAFKA_PRODUCER_VALUESERIALIZER+"]");
			KAFKA_PRODUCER_COMPRESSION_TYPE = PropertiesUtil.getProperty("kafka.producer.compressiontype");
			Logger.logInfoMessage("Set value for kafka.producer.compressiontype = ["+KAFKA_PRODUCER_COMPRESSION_TYPE+"]");
			KAFKA_PRODUCER_ADDITONAL_PROPS.putAll(PropertiesUtil.getProperties("kafka.producer.additional.props"));
			Logger.logInfoMessage(" Kafka Producer Additional Properties ["+KAFKA_PRODUCER_ADDITONAL_PROPS+"]");

			
			KAFKA_CONSUMER_NODELIST = PropertiesUtil.getProperty("kafka.consumer.bootstrap.servers");
			Logger.logInfoMessage("Set value for kafka.bootstrap.servers = ["+KAFKA_CONSUMER_NODELIST+"]");
			KAFKA_CONSUMER_TOPIC = PropertiesUtil.getProperty("kafka.consumer.topic");
			Logger.logInfoMessage("Set value for kafka.consumer.topic = ["+KAFKA_CONSUMER_TOPIC+"]");
			KAFKA_CONSUMER_GROUP = PropertiesUtil.getProperty("kafka.consumer.group");
			Logger.logInfoMessage("Set value for kafka.consumer.group = ["+KAFKA_CONSUMER_GROUP+"]");
			KAFKA_CONSUMER_MAXPOLL = PropertiesUtil.getProperty("kafka.consumer.maxpollrecord");
			Logger.logInfoMessage("Set value for kafka.consumer.maxpollrecord = ["+KAFKA_CONSUMER_MAXPOLL+"]");
			KAFKA_CONSUMER_SLEEP = Long.parseLong(PropertiesUtil.getProperty("kafka.consumer.sleepinterval"));
			Logger.logInfoMessage("Set value for kafka.consumer.sleepinterval = ["+KAFKA_CONSUMER_SLEEP+"]");
			KAFKA_CONSUMER_TOPIC_PARTITIONS = Integer.parseInt(PropertiesUtil.getProperty("kafka.consumer.topic.partitions"));
			Logger.logInfoMessage("Set value for kafka.consumer.topic.partitions = ["+KAFKA_CONSUMER_TOPIC_PARTITIONS+"]");
			KAFKA_CONSUMER_OFFSET_RESET = PropertiesUtil.getProperty("kafka.consumer.offset.reset");
			Logger.logInfoMessage("Set value for kafka.consumer.offset.reset = ["+KAFKA_CONSUMER_OFFSET_RESET+"]");
			KAFKA_CONSUMER_KEYDESERIALIZER = PropertiesUtil.getProperty("kafka.consumer.key-deserializer");
			Logger.logInfoMessage("Set value for kafka.consumer.key-deserializer = ["+KAFKA_CONSUMER_KEYDESERIALIZER+"]");
			KAFKA_CONSUMER_VALUEDESERIALIZER = PropertiesUtil.getProperty("kafka.consumer.value-deserializer");
			Logger.logInfoMessage("Set value for kafka.consumer.value-serializer = ["+KAFKA_CONSUMER_VALUEDESERIALIZER+"]");
			KAFKA_CONSUMER_ADDITONAL_PROPS.putAll(PropertiesUtil.getProperties("kafka.consumer.additional.props"));
			Logger.logInfoMessage(" Kafka Consumer Additional Properties ["+KAFKA_CONSUMER_ADDITONAL_PROPS+"]");
			KAFKA_CONSUMER_FILTER_KEYLIST = PropertiesUtil.getProperty("kafka.consumer.filter.keylist");
			Logger.logInfoMessage("Set value for kafka.consumer.filter.keylist = ["+KAFKA_CONSUMER_FILTER_KEYLIST+"]");
			KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.addAll(Arrays.asList(PropertiesUtil.getProperty("kafka.consumer" +
							".category-exclusion-list","NONE")));
			Logger.logInfoMessage("Set value for kafka.consumer" +
					".category-exclusion-list = ["+KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST+"]");

			KAFKA_CACHE_TOPIC_CONSUMER_GROUP = PropertiesUtil.getProperty("kafka.consumer.cache.group");
			Logger.logInfoMessage("Set value for kafka.consumer.cache.group = ["+KAFKA_CACHE_TOPIC_CONSUMER_GROUP+"]");

			KAFKA_CATEGORIES_TOPIC = PropertiesUtil.getProperty("kafka.categories.topic");
			Logger.logInfoMessage("Set value for kafka.categories.topic = ["+KAFKA_CATEGORIES_TOPIC+"]");
			KAFKA_CLASSES_TOPIC = PropertiesUtil.getProperty("kafka.classes.topic");
			Logger.logInfoMessage("Set value for kafka.classes.topic = ["+KAFKA_CLASSES_TOPIC+"]");
			KAFKA_TYPES_TOPIC = PropertiesUtil.getProperty("kafka.types.topic");
			Logger.logInfoMessage("Set value for kafka.types.topic = ["+KAFKA_TYPES_TOPIC+"]");
			KAFKA_EVENTS_TOPIC = PropertiesUtil.getProperty("kafka.events.topic");
			Logger.logInfoMessage("Set value for kafka.events.topic = ["+KAFKA_EVENTS_TOPIC+"]");
			KAFKA_MARKETS_TOPIC = PropertiesUtil.getProperty("kafka.markets.topic");
			Logger.logInfoMessage("Set value for kafka.markets.topic = ["+KAFKA_MARKETS_TOPIC+"]");
			KAFKA_SELECTIONS_TOPIC = PropertiesUtil.getProperty("kafka.selections.topic");
			Logger.logInfoMessage("Set value for kafka.selections.topic = ["+KAFKA_SELECTIONS_TOPIC+"]");
			KAFKA_INPLAY_TOPIC = PropertiesUtil.getProperty("kafka.inplay.topic");
			Logger.logInfoMessage("Set value for kafka.inplay.topic = ["+KAFKA_INPLAY_TOPIC+"]");

		}
		catch(IllegalStateException ise)
		{
			Logger.logErrorMessage("IllegalStateException in processing GVs" , ise);
			throw ise;
		}
		catch (Exception e) 
		{
			Logger.logErrorMessage("General Exception in processing GVs" , e);
			throw new IllegalStateException(e.getMessage());
		}
		
	}
}
