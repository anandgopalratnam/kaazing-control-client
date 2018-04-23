package com.lc.df.controlclient.utils;

/**
 * An HTTP server that sends back the content of the received HTTP request in a
 * pretty plaintext form.
 */
@SuppressWarnings("unused")
public class Stats {

    public static int total_sent = 0;
    public static int total_error = 0;
    public static long avg_latency = 0;
    public static long sumLatency = 0;
    public static int sent_last_round = 0;

    private static Integer lock_latency = new Integer(1);


    private long lat_sending;
    private long lat_receiving;
    private long lat_total;


    public void setLatency(long latency,boolean isSuccess)
    {
        lat_receiving = latency;
        lat_total = lat_receiving;
        synchronized (lock_latency) {
            sumLatency += lat_total;
            if(!isSuccess){
                total_error++;
            }
        }
    }

//    public static String getStats() {
//
//        long avg_latency = 0;
//        if (total_sent != 0)
//            avg_latency = sumLatency / total_sent;
//
//        // calculation for average TPS rate
//        double t1 = (total_sent * 1000);
//        double t2 = sending_time;
//        double t3 = t1 / t2;
//        long tps_avg = Math.round(t3);
//
//        int stint = total_sent - sent_last_round;
//        sent_last_round = total_sent;
//        t1 = (stint * 1000);
//        t2 = KafkaClientConfig.stats_interval_ms;
//        t3 = t1 / t2;
//        long tps_last = Math.round(t3);
//
//        return "TPS(avg):" + tps_avg + ", TPS(last):" + tps_last + ", TIME: " + sending_time + ", sent:" + total_sent
//                + ", ok:" + total_success + ", error:" + total_error + "(c/t/d/o: " + total_error_connect + "/"
//                + total_error_timeout + "/" + total_discarded + "/" + total_error_unknown + "), avg_latency:"
//                + avg_latency;
//    }
//
//    public long getLatency() {
//        return timeReceived - timeSent;
//    }

}
