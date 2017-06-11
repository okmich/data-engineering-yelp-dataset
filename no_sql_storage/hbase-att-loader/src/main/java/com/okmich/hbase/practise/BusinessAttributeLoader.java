/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.hbase.practise;

import static com.okmich.hbase.practise.Util.as;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author datadev
 */
public class BusinessAttributeLoader {

	private JSONParser jsonParser;
	private final Table table;
	private static final Logger LOG = Logger
			.getLogger(BusinessAttributeLoader.class.getName());

	/**
	 *
	 * @throws IOException
	 */
	public BusinessAttributeLoader() throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
		config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
		Connection connection = ConnectionFactory.createConnection(config);
		this.table = connection.getTable(TableName.valueOf("business"));

		jsonParser = new JSONParser();
	}

	public static void main(String[] args) throws IOException {
		LineIterator lIt;
		try {
			LOG.log(Level.INFO, "Reading the content of file");
			lIt = FileUtils
					.lineIterator(new File(
							"/home/cloudera/Downloads/yelp_academic_dataset_business.json"));
		} catch (IOException ex) {
			Logger.getLogger(BusinessAttributeLoader.class.getName()).log(
					Level.SEVERE, null, ex);
			throw new RuntimeException(ex);
		}
		String line;

		BusinessAttributeLoader loader = new BusinessAttributeLoader();
		while (lIt.hasNext()) {
			line = lIt.nextLine();
			//
			try {
				loader.processAndSave(line);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void processAndSave(String line) throws Exception {
		JSONObject object = (JSONObject) jsonParser.parse(line);
		String businessId = (String) object.get("business_id");
		JSONObject attribute = (JSONObject) object.get("attributes");

		Put put = new Put(as(businessId));
		Map<String, String> colVals = new LinkedHashMap<>();
		loadColumnsAndValues(attribute, colVals, "");
		// save(put);
		System.out.println(colVals.size());
		if (colVals.isEmpty())
			return;

		for (String key : colVals.keySet()) {
			put.addColumn(COLUMN_FAMILY_MAIN, as(key), as(colVals.get(key)));
		}
		save(put);
	}

	/**
	 * TABLE_SENSOR
	 */
	public static final byte[] TABLE_SENSOR = as("business");
	/**
	 * COLUMN_FAMILY_MAIN
	 */
	public static final byte[] COLUMN_FAMILY_MAIN = as("att");

	/**
	 *
	 * @param sensor
	 * @throws Exception
	 */
	public void save(Put put) throws Exception {
		try {
			table.put(put);
		} catch (IOException ex) {
			throw new Exception(ex.getMessage(), ex);
		}
	}

	public static String getString(byte[] obj) {
		return Bytes.toString(obj);
	}

	public static float getFloat(byte[] obj) {
		return Bytes.toFloat(obj);
	}

	/**
	 * 
	 * @param file
	 * @return
	 * @throws FileNotFoundException
	 */
	public BufferedReader getFileBufferedReader(String file)
			throws FileNotFoundException {
		return new BufferedReader(new FileReader(file));
	}

	/**
	 * 
	 * @param attribute
	 * @param colValList
	 * @param prefix
	 */
	private void loadColumnsAndValues(JSONObject attribute,
			Map<String, String> colValList, String prefix) {
		for (Object key : attribute.keySet()) {
			Object value = attribute.get(key);
			if (value instanceof JSONObject) {
				loadColumnsAndValues((JSONObject) value, colValList,
						formatString(prefix, key.toString()));
			} else {
				colValList.put(formatString(prefix, key.toString()),
						value.toString());
			}
		}
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	private String formatString(String prefix, String s) {
		String suffix = s.replace(" ", "_").toLowerCase();
		if (prefix.isEmpty())
			return suffix;
		else
			return prefix + "__" + suffix;
	}
}
