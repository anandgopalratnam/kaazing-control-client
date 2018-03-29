package com.lc.df.controlclient.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lc.df.controlclient.utils.GlobalVariables;
import com.lc.df.controlclient.utils.Logger;
import com.lc.df.controlclient.utils.Utils;
import com.lc.df.kafka.utils.KafkaCacheSubscriber;
import com.lc.df.kafka.utils.KafkaPublisher;
import com.lc.df.kafka.utils.KafkaRecord;
import com.lc.df.kafka.utils.KafkaSubscriber;

public class KazingControlClient
{
	private static volatile long count = 0;
	private static volatile long lastcount = 0;
	EntityCache entityCache = new EntityCache();
	Map<String, String> cacheTopicCounter = new TreeMap<String, String>();
	private volatile boolean cacheLoaded = false;
	private long startTime = System.currentTimeMillis();
	ObjectMapper mapper = new ObjectMapper();

	public KazingControlClient()
	{
	}
	private void init()
	{
		Logger.logInfoMessage("Initialising....");
		GlobalVariables.setVariables("kaazingControlClient");
		loadCache();
		initializeKafkaSubscriber();
		initializeKafkaPublisher();

	}
	private void loadCache(){
		Logger.logInfoMessage("Loading Cache .....");
		KafkaCacheSubscriber.initializeKafkaConsumer();
		new CacheCounterPrinter().start();
		try
		{
			long recordsTimestamp = System.currentTimeMillis();
			while(!cacheLoaded)
			{
				List<KafkaRecord> msg = KafkaCacheSubscriber.getRecords();
				if (msg != null && msg.size() > 0)
				{
					processCacheMessage(msg);
					recordsTimestamp = System.currentTimeMillis();
				}
				else
				{
					Logger.logInfoMessage("No new messages. Trying again ....");
					cacheLoaded = (System.currentTimeMillis() - recordsTimestamp) > 15000;
				}
			}
			entityCache.printCacheCounts();
		}
		catch (Exception e)
		{
			Logger.logErrorMessage("Exception in Message Consumption ....",e);
		}

	}
	public void processCacheMessage(List<KafkaRecord> msg)
	{
		for (int i = 0; i < msg.size(); i++)
		{
			KafkaRecord kRecord = msg.get(i);
			String topic = kRecord.getTopic();
			String key = (String)kRecord.getKey();
			String value = (String)kRecord.getValue();
			setCacheCounterValue(topic,kRecord.getPartition(),kRecord.getOffset());
			if ("categories".equalsIgnoreCase(topic)){
				entityCache.setCategory(key,value);
			}
			else if ("classes".equalsIgnoreCase(topic)){
				entityCache.setClass(key,value);
			}
			else if ("types".equalsIgnoreCase(topic)){
				entityCache.setType(key,value);
			}
			else if ("events".equalsIgnoreCase(topic)){
				entityCache.setEvent(key,value);
			}
			else if ("markets".equalsIgnoreCase(topic)){
				entityCache.setMarket(key,value);
			}
			else if ("selections".equalsIgnoreCase(topic)){
				entityCache.setSelection(key,value);
			}
		}
	}
	private void initializeKafkaPublisher()
	{
		Logger.logInfoMessage("Kafka Pub Initializing ...");
		KafkaPublisher.initializeKafkaProducer();
		
		Logger.logInfoMessage("Kafka Pub Initialized ...");
	}

	private void initializeKafkaSubscriber()
	{
		Logger.logInfoMessage("Kafka Sub Initializing ...");
		KafkaSubscriber.initializeKafkaConsumer();
		Logger.logInfoMessage("Kafka Sub Initialized ...");
	}
	public void startConsumption()
	{
		try
		{
			while(true)
			{
				List<KafkaRecord> msg = KafkaSubscriber.getRecords();
				if (msg != null && msg.size() > 0)
				{
					processMessage(msg);
				}
			}
		}
		catch (Exception e)
		{
			Logger.logErrorMessage("Exception in Message Consumption ....",e);
		}
	}
    
    public void processMessage(List<KafkaRecord> msg)
    {
		for (int i = 0; i < msg.size(); i++)
		{
			KafkaRecord kRecord = msg.get(i);
			String value = (String)kRecord.getValue();
			JsonNode node = Utils.getJsonNode(value);
			if (node != null){
				try {
					if (Utils.acceptEventMessage(node)){
                        handleEvent(node.path("event"));
                    }
					if (Utils.acceptMarketMessage(node)){
                        handleMarket(node.path("market"));
                    }
					if (Utils.acceptSelectionMessage(node)){
                        handleSelection(node.path("selection"));
                    }
				} catch (Exception e) {
					Logger.logErrorMessage("Error handling Node ["+node+"] ",e);
				}
			}
			else {
				Logger.logInfoMessage("Empty Kafka Message ["+kRecord+"]");
			}
		}
    }
    private void handleEvent(JsonNode eventNode){
		String eventKey = eventNode.path("eventKey").asText("NOTSET");
		String categoryKey = eventNode.path("meta").path("categoryKey").asText("NOTSET");
		if (GlobalVariables.KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.contains(categoryKey))
		{
			return;
		}
		if (Utils.acceptEventCreateMessage(eventNode)){
			JsonNode newEventNode = Utils.applyEventCreateForEventsTopic(entityCache,eventNode);
			String eventJson = Utils.getJsonString(newEventNode);
			entityCache.setEvent(eventKey,eventJson);
			publishToKafka("events",eventKey,eventJson);
		}
		else if (Utils.acceptEventUpdateMessage(eventNode)) {
			JsonNode eventSnapshot = Utils.getJsonNode(entityCache.getEvent(eventKey, null));
			JsonNode newEventNode = Utils.applyEventUpdateForEventsTopic(eventNode, eventSnapshot);
			if (newEventNode != null) {
				String eventJson = Utils.getJsonString(newEventNode);
				entityCache.setEvent(eventKey, eventJson);
				publishToKafka("events", eventKey, eventJson);
				if (Utils.acceptInplayEventMessage(newEventNode.path("event"))) {
					publishToKafka("inplay", eventKey, eventJson);
				}
			}
		}
		else if (Utils.acceptEventDeleteMessage(eventNode)){
			entityCache.setEvent(eventKey,null);
			publishToKafka("events",eventKey,null);
			publishToKafka("inplay",eventKey,null);
		}
	}

	private void handleMarket(JsonNode marketNode){
		String marketKey = marketNode.path("marketKey").asText("NOTSET");
		String categoryKey = marketNode.path("meta").path("categoryKey").asText("NOTSET");
		if (GlobalVariables.KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.contains(categoryKey))
		{
			return;
		}
		if (Utils.acceptMarketCreateMessage(marketNode)){
			String eventKey = marketNode.path("meta").path("eventKey").asText("NOTSET");
			JsonNode eventSnapshot = Utils.getJsonNode(entityCache.getEvent(eventKey,null));
			JsonNode newEventNode = Utils.applyMarketCreateForEventsTopic(marketNode,eventSnapshot);
			if (newEventNode != null){
				String jsonString = Utils.getJsonString(newEventNode);
				entityCache.setEvent(eventKey,jsonString);
				publishToKafka("events",eventKey,jsonString);
			}
			JsonNode newMarketNode = Utils.applyMarketCreateForMarketsTopic(marketNode);
			if (newMarketNode != null){
				String marketJson = Utils.getJsonString(newMarketNode);
				entityCache.setMarket(marketKey,marketJson);
				publishToKafka("markets",marketKey,marketJson);
			}

		}
		else if (Utils.acceptMarketUpdateMessage(marketNode)){
			JsonNode marketSnapshot = Utils.getJsonNode(entityCache.getMarket(marketKey,null));
			JsonNode newMarketNode = Utils.applyMarketUpdateForMarketsTopic(marketNode,marketSnapshot);
			if (newMarketNode != null){
				String marketJson = Utils.getJsonString(newMarketNode);
				entityCache.setMarket(marketKey,marketJson);
				publishToKafka("markets",marketKey,marketJson);
			}

		}
		else if (Utils.acceptMarketDeleteMessage(marketNode)){
			entityCache.setMarket(marketKey,null);
			publishToKafka("markets",marketKey,null);
		}
	}
	private void handleSelection(JsonNode selectionNode){
		String selectionKey = selectionNode.path("selectionKey").asText("NOTSET");
		String categoryKey = selectionNode.path("meta").path("categoryKey").asText("NOTSET");
		if (GlobalVariables.KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.contains(categoryKey))
		{
			return;
		}
		if (Utils.acceptSelectionCreateMessage(selectionNode)){
			String marketKey = selectionNode.path("meta").path("marketKey").asText("NOTSET");
			JsonNode marketSnapshot = Utils.getJsonNode(entityCache.getMarket(marketKey,null));
			JsonNode newMarketNode = Utils.applySelectionCreateForMarketsTopic(selectionNode,marketSnapshot);
			if (newMarketNode != null){
				String marketJson = Utils.getJsonString(newMarketNode);
				entityCache.setMarket(marketKey,marketJson);
				publishToKafka("markets",marketKey,marketJson);
			}
			JsonNode newSelectionNode = Utils.applySelectionCreateForSelectionsTopic(selectionNode);
			if (newSelectionNode != null){
				String selectionJson = Utils.getJsonString(newSelectionNode);
				entityCache.setSelection(selectionKey,selectionJson);
				publishToKafka("selections",selectionKey,selectionJson);
			}

		}
		else if (Utils.acceptSelectionUpdateMessage(selectionNode)){
			JsonNode selectionSnapshot = Utils.getJsonNode(entityCache.getSelection(selectionKey,null));
			JsonNode newSelectionNode = Utils.applySelectionUpdateForSelectionsTopic(selectionNode,selectionSnapshot);
			if (newSelectionNode != null){
				String selectionJson = Utils.getJsonString(newSelectionNode);
				entityCache.setSelection(selectionKey,selectionJson);
				publishToKafka("selections",selectionKey,selectionJson);
			}

		}
		else if (Utils.acceptSelectionDeleteMessage(selectionNode)){
			entityCache.setSelection(selectionKey,null);
			publishToKafka("selections",selectionKey,null);
		}
	}

	private void setCacheCounterValue (String topic,int parition, long offset){
		synchronized (cacheTopicCounter){
			cacheTopicCounter.put(topic + "-" + parition,""+offset);
		}
	}
	private void printCacheCounters(){
		synchronized (cacheTopicCounter){
			Logger.logInfoMessage(cacheTopicCounter);
		}
	}
	class CacheCounterPrinter extends Thread{
		@Override
		public void run() {
			Logger.logInfoMessage("Starting Cache Counter Printer Thread ...");
			while (!cacheLoaded){
				Utils.sleep(15000);
				printCacheCounters();
			}
			Logger.logInfoMessage("Finished Cache Counter Printer Thread");
		}
	}
	public static void publishToKafka(String topic,Object key,Object value){
		int published = -1;
		while (published < 0)
		{
			published = KafkaPublisher.publish(topic,key,value);
			if (published < 0)
			{
				Utils.sleep(5);
			}
		}
	}

	public static void main(String[] args)
	{
		KazingControlClient kCC = new KazingControlClient();
		kCC.init();
		kCC.startConsumption();
	}
}
