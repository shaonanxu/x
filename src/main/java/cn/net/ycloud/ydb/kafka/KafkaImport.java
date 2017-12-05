package cn.net.ycloud.ydb.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KafkaImport {
	
    private KafkaProducer<String, String> producer;
	private String topic;
	
    public KafkaImport init(String host, String port, String topic) {
    	this.topic = topic;
    	Properties conf = new Properties();
    	conf.setProperty("bootstrap.servers",  host + ":" + port);
    	conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	this.producer = new KafkaProducer<String, String>(conf); 
    	return this;
    }
    
    public int startImportByFile(String file) {
    	
    	
    	return 0;
    }
    
    public int startImport(String data) {
    	ProducerRecord<String, String> pr = new ProducerRecord<String, String>(this.topic, data);
    	producer.send(pr);
    	producer.close();
    	return 1;
    }
	
	public static void main(String[] args) {
		String host = null, port = null, topic = null, itype = null, data = null;
		if(args.length == 5) {
			host = args[0];
			port = args[1];
			topic = args[2];
			itype = args[3];
			data= args[4];
		} else if (args.length == 4) {
			host = args[0];
			port = "9092";
			topic = args[1];
			itype = args[2];
			data= args[3];
		} else {
			System.err.println("ERROR: Input Params Error .");
		}
		System.out.println(data);
		System.out.println("===================");
		if("1".equals(itype))
			System.out.println(new KafkaImport().init(host, port, topic).startImport(data));
	}

}
