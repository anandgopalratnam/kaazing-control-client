package com.lc.df.controlclient.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lc.df.controlclient.utils.GlobalVariables;
import com.lc.df.controlclient.utils.Logger;
import com.lc.df.controlclient.utils.Utils;
import com.lc.df.kafka.utils.KafkaCacheSubscriber;
import com.lc.df.kafka.utils.KafkaPublisher;
import com.lc.df.kafka.utils.KafkaRecord;
import com.lc.df.kafka.utils.KafkaSubscriber;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class KazingControlClient
{
	private static volatile long count = 0;
	private static volatile long lastcount = 0;
	EntityCache entityCache = new EntityCache();
	Map<String, String> cacheTopicCounter = new TreeMap<String, String>();
	ObjectMapper mapper = new ObjectMapper();
	private volatile boolean cacheLoaded = false;
	private long startTime = System.currentTimeMillis();

	public KazingControlClient()
	{
	}

	public static void main(String[] args)
	{
		KazingControlClient kCC = new KazingControlClient();
		kCC.init();
		kCC.startConsumption();
	}

	private void init()
	{
		Logger.logInfoMessage("Initialising....");
		GlobalVariables.setVariables("kaazingControlClient");
		loadCache();
		initializeKafkaSubscriber();
		initializeKafkaPublisher();

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

	private void initializeKafkaSubscriber()
	{
		Logger.logInfoMessage("Kafka Sub Initializing ...");
		KafkaSubscriber.initializeKafkaConsumer();
		Logger.logInfoMessage("Kafka Sub Initialized ...");
	}

	private void initializeKafkaPublisher()
	{
		Logger.logInfoMessage("Kafka Pub Initializing ...");
		KafkaPublisher.initializeKafkaProducer();

		Logger.logInfoMessage("Kafka Pub Initialized ...");
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

	public void processCacheMessage(List<KafkaRecord> msg)
	{
		for (int i = 0; i < msg.size(); i++)
		{
			KafkaRecord kRecord = msg.get(i);
			String topic = kRecord.getTopic();
			Long key = Long.parseLong((String)kRecord.getKey());
			String value = (String)kRecord.getValue();
			setCacheCounterValue(topic,kRecord.getPartition(),kRecord.getOffset());
			if (GlobalVariables.KAFKA_CATEGORIES_TOPIC.equalsIgnoreCase(topic)){
				entityCache.setCategory(key,value);
			}
			else if (GlobalVariables.KAFKA_CLASSES_TOPIC.equalsIgnoreCase(topic)){
				entityCache.setClass(key,value);
			}
			else if (GlobalVariables.KAFKA_TYPES_TOPIC.equalsIgnoreCase(topic)){
				entityCache.setType(key,value);
			}
			else if (GlobalVariables.KAFKA_EVENTS_TOPIC.equalsIgnoreCase(topic)){
				entityCache.setEvent(key,value);
			}
			else if (GlobalVariables.KAFKA_MARKETS_TOPIC.equalsIgnoreCase(topic)){
				entityCache.setMarket(key,value);
			}
			else if (GlobalVariables.KAFKA_SELECTIONS_TOPIC.equalsIgnoreCase(topic)){
				entityCache.setSelection(key,value);
			}
		}
	}

    private void handleEvent(JsonNode eventNode){
		Long eventKey = eventNode.path("eventKey").asLong(-1);
		Parents p = Utils.getParents(eventNode.path("meta").path("parents").asText("NOTSET"));
		if (p.getCategoryKey() != null)
		{
			String categoryKey = Long.toString(p.getCategoryKey());
			if (categoryKey != null && GlobalVariables.KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.contains(categoryKey)){
				return;
			}
		}

		if (Utils.acceptEventCreateMessage(eventNode)){
			JsonNode newEventNode = Utils.applyEventCreateForEventsTopic(entityCache,eventNode,p);
			if (newEventNode != null) {
				String eventJson = Utils.getJsonString(newEventNode);
				entityCache.setEvent(eventKey,eventJson);
				publishToKafka(GlobalVariables.KAFKA_EVENTS_TOPIC,Long.toString(eventKey),eventJson);
			}
		}
		else if (Utils.acceptEventUpdateMessage(eventNode)) {
			JsonNode eventSnapshot = Utils.getJsonNode(entityCache.getEvent(eventKey, null));
			JsonNode newEventNode = Utils.applyEventUpdateForEventsTopic(eventNode, eventSnapshot);
			if (newEventNode != null) {
				String eventJson = Utils.getJsonString(newEventNode);
				entityCache.setEvent(eventKey, eventJson);
				publishToKafka(GlobalVariables.KAFKA_EVENTS_TOPIC, Long.toString(eventKey), eventJson);
				if (Utils.acceptInplayEventMessage(newEventNode.path("event"))) {
					publishToKafka(GlobalVariables.KAFKA_INPLAY_TOPIC, Long.toString(eventKey), eventJson);
				}
			}
		}
		else if (Utils.acceptEventDeleteMessage(eventNode)){
			entityCache.setEvent(eventKey,null);
			publishToKafka(GlobalVariables.KAFKA_EVENTS_TOPIC,Long.toString(eventKey),null);
			publishToKafka(GlobalVariables.KAFKA_INPLAY_TOPIC,Long.toString(eventKey),null);
		}
	}

	private void handleMarket(JsonNode marketNode){
		Long marketKey = marketNode.path("marketKey").asLong(-1);
		Parents p = Utils.getParents(marketNode.path("meta").path("parents").asText("NOTSET"));
		if (p.getCategoryKey() != null)
		{
			String categoryKey = Long.toString(p.getCategoryKey());
			if (categoryKey != null && GlobalVariables.KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.contains(categoryKey)){
				return;
			}
		}
		if (Utils.acceptMarketCreateMessage(marketNode)){
			Long eventKey = p.getEventKey();
			JsonNode eventSnapshot = Utils.getJsonNode(entityCache.getEvent(eventKey,null));
			JsonNode newEventNode = Utils.applyMarketCreateForEventsTopic(marketNode,eventSnapshot);
			if (newEventNode != null){
				String jsonString = Utils.getJsonString(newEventNode);
				entityCache.setEvent(eventKey,jsonString);
				publishToKafka(GlobalVariables.KAFKA_EVENTS_TOPIC,Long.toString(eventKey),jsonString);
			}
			JsonNode newMarketNode = Utils.applyMarketCreateForMarketsTopic(marketNode);
			if (newMarketNode != null){
				String marketJson = Utils.getJsonString(newMarketNode);
				entityCache.setMarket(marketKey,marketJson);
				publishToKafka(GlobalVariables.KAFKA_MARKETS_TOPIC,Long.toString(marketKey),marketJson);
			}

		}
		else if (Utils.acceptMarketUpdateMessage(marketNode)){
			JsonNode marketSnapshot = Utils.getJsonNode(entityCache.getMarket(marketKey,null));
			JsonNode newMarketNode = Utils.applyMarketUpdateForMarketsTopic(marketNode,marketSnapshot);
			if (newMarketNode != null){
				String marketJson = Utils.getJsonString(newMarketNode);
				entityCache.setMarket(marketKey,marketJson);
				publishToKafka(GlobalVariables.KAFKA_MARKETS_TOPIC,Long.toString(marketKey),marketJson);
			}

		}
		else if (Utils.acceptMarketDeleteMessage(marketNode)){
			entityCache.setMarket(marketKey,null);
			publishToKafka(GlobalVariables.KAFKA_MARKETS_TOPIC,Long.toString(marketKey),null);
		}
	}

	private void handleSelection(JsonNode selectionNode){
		Long selectionKey = selectionNode.path("selectionKey").asLong(-1);
		Parents p = Utils.getParents(selectionNode.path("meta").path("parents").asText("NOTSET"));
		if (p.getCategoryKey() != null)
		{
			String categoryKey = Long.toString(p.getCategoryKey());
			if (categoryKey != null && GlobalVariables.KAFKA_CONSUMER_CATEGORY_EXCLUSION_LIST.contains(categoryKey)){
				return;
			}
		}
		if (Utils.acceptSelectionCreateMessage(selectionNode)){
			Long marketKey = p.getMarketKey();
			JsonNode marketSnapshot = Utils.getJsonNode(entityCache.getMarket(marketKey,null));
			JsonNode newMarketNode = Utils.applySelectionCreateForMarketsTopic(selectionNode,marketSnapshot);
			if (newMarketNode != null){
				String marketJson = Utils.getJsonString(newMarketNode);
				entityCache.setMarket(marketKey,marketJson);
				publishToKafka(GlobalVariables.KAFKA_MARKETS_TOPIC,Long.toString(marketKey),marketJson);
			}
			JsonNode newSelectionNode = Utils.applySelectionCreateForSelectionsTopic(selectionNode);
			if (newSelectionNode != null){
				String selectionJson = Utils.getJsonString(newSelectionNode);
				entityCache.setSelection(selectionKey,selectionJson);
				publishToKafka(GlobalVariables.KAFKA_SELECTIONS_TOPIC,Long.toString(selectionKey),selectionJson);
			}

		}
		else if (Utils.acceptSelectionUpdateMessage(selectionNode)){
			JsonNode selectionSnapshot = Utils.getJsonNode(entityCache.getSelection(selectionKey,null));
			JsonNode newSelectionNode = Utils.applySelectionUpdateForSelectionsTopic(selectionNode,selectionSnapshot);
			if (newSelectionNode != null){
				String selectionJson = Utils.getJsonString(newSelectionNode);
				entityCache.setSelection(selectionKey,selectionJson);
				publishToKafka(GlobalVariables.KAFKA_SELECTIONS_TOPIC,Long.toString(selectionKey),selectionJson);
			}

		}
		else if (Utils.acceptSelectionDeleteMessage(selectionNode)){
			entityCache.setSelection(selectionKey,null);
			publishToKafka(GlobalVariables.KAFKA_SELECTIONS_TOPIC,Long.toString(selectionKey),null);
		}
	}

	private void setCacheCounterValue (String topic,int parition, long offset){
		synchronized (cacheTopicCounter){
			cacheTopicCounter.put(topic + "-" + parition,""+offset);
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
}
