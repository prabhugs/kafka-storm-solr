package com.storm;
/*
import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
*/

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
//import backtype.storm.topology.IRichBolt;
//import backtype.storm.topology.base.BaseBasicBolt;
import java.util.Date;

public abstract class SolrBolt extends BaseRichBolt {

    private OutputCollector collector;
    private SolrServer solrServer;
    private HttpSolrServer httpSolrServer;
    private final String solrAddress;

    private static final long serialVersionUID = -1984026952440715421L;
    /**
     * @param solrAddress The full URL address where Solr is running.
     */
    protected SolrBolt(String solrAddress) {
            //"http://10.21.1.187:8983/solr";
            this.solrAddress = solrAddress;
    }

    //@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

            this.collector = collector;
            try {
                    //this.solrServer = new CommonHttpSolrServer(this.solrAddress);
                    this.httpSolrServer = new HttpSolrServer(this.solrAddress);
            } catch (Exception e) {
                    throw new RuntimeException(e);
            }
    }

    //@Override
    public void execute(Tuple input) {
            if (shouldActOnInput(input)) {

                    //SolrInputDocument document = getSolrInputDocumentForInput(input);
            	SolrInputDocument document = new SolrInputDocument();

                    if (document != null) {
                    	try {
                    		//solrServer.add(document);
                    		Date date= new java.util.Date();

                                // Testing document add to solr server
                    		document.addField("id", date.getTime());
                    		document.addField("name", "prabhu gnana sundar");
                    		document.addField("city", "tuty");
                    		httpSolrServer.add(document);
                    		httpSolrServer.commit();
                            collector.ack(input);
                        } catch (SolrServerException e) {
                                    e.printStackTrace();
                                    collector.fail(input);
                        } catch (IOException e) {
                            	e.printStackTrace();
                                collector.fail(input);
                        }
                    }
            } else {
            	collector.ack(input);
            }
    }
  
    /**
     * Decide whether or not this input tuple should trigger a Solr write.
     *
     * @param input the input tuple under consideration
     * @return {@code true} iff this input tuple should trigger a Solr write
     */
    public abstract boolean shouldActOnInput(Tuple input);

    /**
     * Returns the SolrInputDocument to store in Solr for the specified input tuple.
     * 
             * @param input the input tuple under consideration
             * @return the SolrInputDocument to be written to Solr
             */
    public abstract SolrInputDocument getSolrInputDocumentForInput(Tuple input);

    @Override
    public void cleanup() {
    }
}
