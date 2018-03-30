package com.brofan.table;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public final class TableManager {
	
	private TableManager() {
	}
	
	protected static void createTable(HBaseAdmin admin, byte[] table, byte[] family) throws IOException{
		
		HTableDescriptor desc = new HTableDescriptor(table);
		HColumnDescriptor f = new HColumnDescriptor(family);	// create family r
		desc.addFamily(f);
		admin.createTable(desc);
		String table_name = new String(table);
		System.out.println("Create Table " + table_name + " Success!");
	}
	
	protected static void deleteTable(HBaseAdmin admin, byte[] table) throws IOException{
		String table_name = new String(table);
		if (admin.tableExists(table)) {
			System.out.println( table_name + " Table Exists!");
			
			admin.disableTable(table);
			admin.deleteTable(table);
			System.out.println("Drop " + table_name + " Table Success!");
		}
	}
	
	public static void setup(Configuration conf, byte[] table, byte[] family) throws IOException{

		HBaseAdmin admin = new HBaseAdmin(conf);
		
		deleteTable(admin, table);		
		createTable(admin, table, family);
		
		admin.close();
		
	}
}
