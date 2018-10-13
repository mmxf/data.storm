package data.storm.t6;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class TridentKafkaWordCount {

    public static void main(String[] args) {
        LocalSubmitter localSubmitter = LocalSubmitter.newInstance();
        //localSubmitter.submit("kafkaBolt", LocalSubmitter.defaultConfig(), KafkaProducerTopology.newTopology(zkBrokerUrl[1], topicName));

    }

    public TridentKafkaConfig buildKafkaConfig(String zkUrl,String topicName){
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts,topicName);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return config;
    }
}
