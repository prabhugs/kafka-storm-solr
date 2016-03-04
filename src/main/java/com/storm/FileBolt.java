package com.storm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class FileBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6984026952440715421L;
	private final String filename="/tmp/stormoutput.txt";
	public void execute(Tuple input, BasicOutputCollector collector) {

		String message = input.getString(0);
		System.out.println("Received message: " + message);


		File file;
		try {
			file = new File(filename);
			if(file.exists())
			{
				FileWriter fw = new FileWriter(file,true);
				fw.write("----------------");
				fw.write(message);
				fw.write('\n');
				fw.close();
			}
			else
			{
				file.createNewFile();
				FileWriter fw = new FileWriter(file,true);
				fw.write(message);
				fw.write('\n');
				fw.close();
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
