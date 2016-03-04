package com.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MySolrBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = -3984026952440715421L;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		String message = input.getString(0);
		System.out.println("###################: " + message);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
