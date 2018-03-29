package com.lc.df.kafka.utils;

import java.util.List;

/**
 * Created by anandgopalratnam on 27/03/2018.
 */
public interface KafkaRecordProcessor {

    public void processMessages(List<KafkaRecord> records);
}
