package com.storm;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class KafkaStormTopology {
	public static void main(String[] args) {
		ZkHosts zk = new ZkHosts("localhost:2181");
		SpoutConfig config = new SpoutConfig(zk, "second-topic", "",
				"KafkaStorm");

		config.scheme = new SchemeAsMultiScheme(new StringScheme());

		config.forceFromStart = true;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(config), 1);
		//builder.setBolt("Bolt", new FileBolt(), 1).globalGrouping("KafkaSpout");
		builder.setBolt("solrbolt", new SimpleSolrBolt("http://10.21.1.187:8983/solr/pgstest"), 1).globalGrouping("KafkaSpout");
		builder.setBolt("mybolt", new MySolrBolt(), 1).globalGrouping("KafkaSpout");

		LocalCluster cluster = new LocalCluster();

		Config conf = new Config();
		conf.setDebug(true);
		cluster.submitTopology("SampleTopology4", conf, builder.createTopology());
	}
}