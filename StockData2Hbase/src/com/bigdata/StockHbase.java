package com.bigdata;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;
import com.google.common.io.Closeables;
import com.util.ExcelPoi;


public class StockHbase {
	public static Configuration configuration;
	static String tablename = "stock";
	static {
		// 默认加载已有的数据
		configuration = HBaseConfiguration.create();	 
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1");  
		configuration.set("hbase.zookeeper.property.clientPort", "2181");  
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Log.info("--start--");
		//删除表
		//deleteTable();
		//创建表
		createTable();
		//添加数据
		insertTableData();
		//读取数据
		getData();
		Log.info("--over--");
	}

/**
 * 创建股票数据表
 */
	@SuppressWarnings("deprecation")
	public static void createTable() {
		HBaseAdmin hBaseAdmin;
		try {
			hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tablename)) {
				System.out.println("表名：" + tablename + "已存在！");				
			} else {
				HTableDescriptor hTableDescriptor = new HTableDescriptor(
						tablename);
				//列族
				hTableDescriptor.addFamily(new HColumnDescriptor("data"));
				
				hBaseAdmin.createTable(hTableDescriptor);
				System.out.println(tablename + "表已创建！");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
/**
 * 将excel文件数据插入到hbase表
 */
	public static void insertTableData()
	{
		String execlPath = System.getProperty("user.dir")+"/data/sz002610_20160922_1.xls";
		String suffix=execlPath.substring(execlPath.lastIndexOf(".")+1);
		
		File file = new File(execlPath);
	    if(!file.exists())
	    {
	    	System.out.println("文件不存在");
	    }
		ExcelPoi excelPoi = null;
		try {
			excelPoi = new ExcelPoi(file, suffix);			
			excelPoi.setSheetIndex(0);
			List<List<String>> data = null;
			// 第1个Sheet表的数据			
			HTable  table=new HTable(configuration,Bytes.toBytes(tablename));			
			data = excelPoi.getCurrentSheetData(0);
			int size = data.size();
			
			String rowkey="";
			String code="sz002610";
			Put put=null;
			for (int i = 1; i < size; i++) {						
				rowkey=i+"";
				put=new Put(Bytes.toBytes(rowkey));				
				List<String> rowData = data.get(i);
				//添加数据
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("time"),Bytes.toBytes(rowData.get(0)));
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("price"),Bytes.toBytes(rowData.get(1)));
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("pricechange"),Bytes.toBytes(rowData.get(2)));
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("number"),Bytes.toBytes(rowData.get(3)));
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("turnover"),Bytes.toBytes(rowData.get(4)));
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("nature"),Bytes.toBytes(rowData.get(5)));
				 put.add(Bytes.toBytes("data"),Bytes.toBytes("code"),Bytes.toBytes(code));
				
				 table.put(put);
			}
		} catch (Exception ex) {
		//	CommUtil.writeErrorLog("Excel:" + ex.getMessage());
			System.out.println(ex.getMessage());
		}
	}
	/**
	 * 获取hbase数据
	 */
	public static void getData()
	{
		Scan scan=new Scan();
		ResultScanner rs=null;
		try {
			HTable table=new HTable(configuration,Bytes.toBytes(tablename));
			rs=table.getScanner(scan);
			for(Result r:rs)
			{
				for(KeyValue kv:r.list())
				{
					System.out.println("row:"+Bytes.toString(kv.getRow()));					
					System.out.println("family:"+Bytes.toString(kv.getFamily()));
				     System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:"+ Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:"+ kv.getTimestamp());
                   //组合
                    String rowInfo="row:"+Bytes.toString(kv.getRow()) +" family:"+Bytes.toString(kv.getFamily());
                    rowInfo+=" qualifier:" + Bytes.toString(kv.getQualifier());
                	rowInfo+=" value:"+ Bytes.toString(kv.getValue());
                	rowInfo+=" timestamp:"+ kv.getTimestamp();
                    com.util.Log.writeDataLog(rowInfo);	
                    System.out.println("-------------------------------------------");
				}
			}		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
/**
 * 删除表
 */
	public static void deleteTable()
	{
		try {
			HBaseAdmin admin=new HBaseAdmin(configuration);
			if (admin.tableExists(tablename)) {				
			//1.先禁用
			admin.disableTable(tablename);
			//2.删除
			admin.deleteTable(tablename);
			Closeables.close(admin, false);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}