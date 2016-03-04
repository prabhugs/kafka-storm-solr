package com.storm;

import java.util.Date;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

//import org.apache.solr.client.solrj.SolrServer;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
//import java.io.IOException;
//import org.apache.solr.client.solrj.SolrServerException;


public class SimpleSolrBolt extends SolrBolt {
	
	private static final long serialVersionUID = -2984026952440715421L;

        /**
         * @param solrAddress The full URL address where Solr is running.
         */
        public SimpleSolrBolt(String solrAddress) {
                super(solrAddress);
                System.out.println("################################################-------------------------------");
        }

        @Override
        public boolean shouldActOnInput(Tuple input) {
                return true;
        }

        @Override
        public SolrInputDocument getSolrInputDocumentForInput(Tuple input) {
                SolrInputDocument document = new SolrInputDocument();

                for (String field : input.getFields()) {
                        Object value = input.getValueByField(field);
                        if (isValidField(value)) {
                                document.addField(field, value);
                        }
                }
                System.out.println("################################################");
                return document;
        }

        private boolean isValidField(Object value) {
                return value instanceof String
                                || value instanceof Date
                                || value instanceof Integer
                                || value instanceof Float
                                || value instanceof Double
                                || value instanceof Short
                                || value instanceof Long;
        }

        //@Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}