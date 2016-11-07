package com.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.io.Closeable;
import org.mortbay.log.Log;

/**
 * https://github.com/larsgeorge/hbase-book/blob/master/common/src/main/java/util/HBaseHelper.java
 * @author zhihua.yang
 */
public class HBaseHelper implements Closeable {

	private Configuration configuration = null;
	private Connection connection = null;
	private Admin admin = null;

	protected HBaseHelper(Configuration configuration) throws IOException {
		this.configuration = configuration;
		this.connection = ConnectionFactory.createConnection(configuration);
		this.admin = connection.getAdmin();
	}

	public static HBaseHelper getHelper(Configuration configuration)
			throws IOException {
		return new HBaseHelper(configuration);
	}

	/**
	 * �ر�
	 */
	public void close() throws IOException {
		connection.close();
	}

	/**
	 * ��ȡ���Ӷ���
	 * 
	 * @return
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * ��ȡ���ö���
	 * 
	 * @return
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	public void creatNamespace(String namespace) {
		try {
			NamespaceDescriptor nd = NamespaceDescriptor.create(namespace)
					.build();
			admin.createNamespace(nd);
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
	}

	/**
	 * 
	 * @param namespace
	 * @param force
	 *            �Ƿ�ɾ���
	 */
	public void dropNamespace(String namespace, boolean force) {
		if (force) {
			try {
				TableName[] tableNames = admin
						.listTableNamesByNamespace(namespace);

				for (TableName tableName : tableNames) {
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
				}

			} catch (IOException e) {
				Log.info(e.getMessage());
			}

		}
		try {
			admin.deleteNamespace(namespace);
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
	}

	/**
	 * �жϱ����Ƿ����
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean existsTable(TableName tableName) {
		boolean result = false;
		try {
			result = admin.tableExists(tableName);
		} catch (IOException e) {			
			Log.info(e.getMessage());
		}
		return result;
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean existsTable(String tableName) {
		return existsTable(TableName.valueOf(tableName));
	}

	/**
	 * ������
	 * @param tableName
	 * @param maxVersions
	 * @param splitKeys
	 * @param colfamilys
	 */
	public void createTable(TableName tableName, int maxVersions,
			byte[][] splitKeys, String... colfamilys) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (String colfamily : colfamilys) {
			HColumnDescriptor coldef = new HColumnDescriptor(colfamily);
			coldef.setMaxVersions(maxVersions);
			desc.addFamily(coldef);
		}
		try {
			if (splitKeys != null) {
				admin.createTable(desc, splitKeys);
			} else {
				admin.createTable(desc);
			}
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
	}
	/**
	 * ���ñ�
	 * @param tableName
	 */
	public void disableTable(TableName tableName)
	{
		try {			
			if(admin.isTableEnabled(tableName))
			{
			admin.disableTable(tableName);
			}
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
	}
	/**
	 * ���ñ�
	 * @param tableName
	 */
	public void enableTable(TableName tableName)
	{
		try {
			if(admin.isTableDisabled(tableName))
			{
				admin.enableTable(tableName);
			}
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
	}
	/**
	 * ɾ���
	 * @param tableName
	 */
	public void dropTable(TableName tableName)
	{
		if(existsTable(tableName))
		{
			try {
				disableTable(tableName);
				admin.deleteTable(tableName);
			} catch (IOException e) {
				Log.info(e.getMessage());
			}
		}
	}
	/**
	 * ɾ���
	 * @param tableName
	 */
	public void dropTable(String tableName)
	{
		dropTable(TableName.valueOf(tableName));
	}
	
}
