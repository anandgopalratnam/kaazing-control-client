# kaazing-control-client

This project requires a properties file names kaazingControlClient. A same has been included in the resources folder.

The runtime command line is java -Dconfig=<path tp properties file> -jar <kaazingcontrolclient.jar> 
It also requires a Logger.properties file with the following content in the same folder where the properties file exists

filename=<path to log>/kaazing-control-client.log
fileout=true
LogLevel=DEBUG
consoleout=true
maxFileSize=20MB
maxBackupSize=100
logpattern=[%t][%d{DATE}] %-5p %x - %m%n

Please change the parameters in the Logger.properties as it suits you.


Further enhancements to be done. 

KafkaConsumer and KafkaProducer classes as Static. I have duplicated the class for Cache Consumer and Main consumer - Inefficient.

Lots of code repetitions that need to be fixed. 

Changing HashMap from Map<String,String> to Map<String,JsonNode> . Need to check memory footprint as this will give us the ability 
to change the JsonNode which will change the object in the map automatically. Now the string to JsonNode and vice versa is taking lot of time



