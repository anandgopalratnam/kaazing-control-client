package com.lc.df.kafka.utils;

import com.lc.df.controlclient.utils.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaCallback implements Callback
{
	public void onCompletion(RecordMetadata metaData, Exception exception)
	{
		if (exception != null)
		{
			if (metaData != null)
			{
				Logger.logErrorMessage("Exception in sending record to Topic ["+metaData.topic()+"] Partition ["+metaData.partition()+"] at ["+metaData.timestamp()+"]",exception);
			}
			else
			{
				Logger.logErrorMessage("Exception in sending record to Topic ["+metaData+"]",exception);
			}
			KafkaPublisher.setException(true);
		}
//		else
//		{
//			Logger.logInfoMessage("Successfully sent Key ...["+metaData.topic()+"] Partition ["+metaData.partition()+"] at ["+metaData.timestamp()+"]");
//		}
	}

}
