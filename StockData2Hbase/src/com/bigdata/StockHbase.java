package com.bigdata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import com.google.common.io.Closeables;
import com.util.ExcelPoi;

/**
 * 股票数据下载
 * http://stock.gtimg.cn/data/index.php?appn=detail&action=download&c=sz002610
 * &d=20161101
 * 
 * @author hadoop
 * 
 */
public class StockHbase {
	public static Configuration configuration;
	static String tablename = "stock";
	/**
	 * 买盘0 卖盘1 中性盘2
	 */
	private static String nature="";
	static {
		// 默认加载已有的数据
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Log.info("--start--");
		// 删除表
		//deleteTable();
		// 创建表
		//createTable();
		// 添加数据
		//insertTableData();
		// 读取数据
		List<String> listPrice=new ArrayList<String>();
		listPrice.add("26.19");
		listPrice.add("26.3");
		listPrice.add("26.49");
		listPrice.add("26.5");
		listPrice.add("26.6");
		for(String price:listPrice)
		{
		 getData(price);
		}
		 Log.info("--over--");
		System.out.println("--over--");
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
				// 列族
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
	public static void insertTableData() {
		// 数据存放的路径
		String dataDir = System.getProperty("user.dir") + "/data/";
		// excel文件路径
		String execlPath = System.getProperty("user.dir")
				+ "/data/sz002610_20160922_1.xls";

		// 避免重复，对文件进行重命名
		File newfile = null;
		ExcelPoi excelPoi = null;
		HTable table = null;
		File fileDir = new File(dataDir);
		File[] fileList = fileDir.listFiles();
		// 遍历data文件夹下面的所有文件
		for (File tempFile : fileList) {
			if (!tempFile.isFile())
				continue;
			// 获取文件全路径
			execlPath = tempFile.getAbsolutePath();
			String suffix = execlPath.substring(execlPath.lastIndexOf(".") + 1);
			if (!suffix.contains("xls")) {
				continue;
			}

			try {
				if (table == null) {
					table = new HTable(configuration, Bytes.toBytes(tablename));
				}
				excelPoi = new ExcelPoi(tempFile, suffix);
				excelPoi.setSheetIndex(0);
				List<List<String>> data = null;

				// sz002610_成交明细_20161101 (2)
				String sheetName = excelPoi.getSheetName();
				String[] arrName = sheetName.split("_");
				if (arrName.length < 3) {
					continue;
				}
				// 股票代码
				String code = arrName[0].replace("sz", "").replace("sh", "");
				// 日期
				String date = arrName[2];
				if (date.length() > 8) {
					date = date.substring(0, 8);
				}
				// 第1个Sheet表的数据

				data = excelPoi.getCurrentSheetData(0);
				int size = data.size();

				String rowkey = "";

				for (int i = 1; i < size; i++) {
					List<String> rowData = data.get(i);
					// 92503 --111618
					String time = rowData.get(0).replace(":", "");
					if (time.length() == 5) {
						time = "0" + time;
					}
					// 股票代码+日期+时间
					rowkey = code + date + time;
					insert(table, "data", rowkey, rowData, code);
				}

				// 移动文件
//				newfile = new File(execlPath.replace("data", "data_imported"));
//				if (!newfile.exists()) {					
//					tempFile.renameTo(newfile);
//				}
			} catch (Exception ex) {
				Log.info(ex.getMessage());
			}
		}

	}

	@SuppressWarnings("deprecation")
	public static void insert(HTable table, String colFamilyName,
			String rowKey, List<String> rowData, String code) {
		Put put = new Put(Bytes.toBytes(rowKey));	
		// 添加数据
		put.add(Bytes.toBytes("data"), Bytes.toBytes("time"),
				Bytes.toBytes(rowData.get(0)));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("price"),
				Bytes.toBytes(rowData.get(1)));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("pricechange"),
				Bytes.toBytes(rowData.get(2)));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("number"),
				Bytes.toBytes(rowData.get(3)));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("turnover"),
				Bytes.toBytes(rowData.get(4)));
		
		/* 买盘0 卖盘1 中性盘2*/
		 nature="0";
		if("卖盘".equals(rowData.get(5)))
		{
			nature="1";
		}
		else if("中性盘".equals(rowData.get(5)))
		{
			nature="2";
		}
		put.add(Bytes.toBytes("data"), Bytes.toBytes("nature"),
				Bytes.toBytes(nature));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("code"),
				Bytes.toBytes(code));

		try {
			table.put(put);
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
	}

	/**
	 * 获取hbase数据
	 */
	public static void getData(String price) {
		Scan scan = new Scan();
		
		   Filter filter = new SingleColumnValueFilter(Bytes  
                   .toBytes("data"),Bytes.toBytes("price"), CompareOp.EQUAL, Bytes  
                   .toBytes(price)); 
		   
		   FilterList lstFilter=new FilterList();
		   lstFilter.addFilter(filter);
		   
		   scan.setFilter(lstFilter);
		ResultScanner rs = null;
		
		double number=0;
		double money=0;
		double buyNumber=0;
		double buyMoney=0;
		double saleNumber=0;
		double saleMoney=0;
		HTable table =null;
		try {
			table = new HTable(configuration, Bytes.toBytes(tablename));
			rs = table.getScanner(scan);
			for (Result r : rs) {
			String natureValue=Bytes.toString(r.getValue(("data").getBytes(), "nature".getBytes()));
			double snumber=Double.parseDouble(Bytes.toString(r.getValue(("data").getBytes(), "number".getBytes())));
			double sturnover=Double.parseDouble(Bytes.toString(r.getValue(("data").getBytes(), "turnover".getBytes())));
			if(natureValue.equals("0"))
			{
				buyNumber+=snumber;
				buyMoney+=sturnover;
			}
			else
			{
				saleNumber+=snumber;
				saleMoney+=sturnover;
			}
			for (KeyValue kv : r.list()) {
				/*
					System.out.println("row:" + Bytes.toString(kv.getRow()));
					System.out.println("family:"
							+ Bytes.toString(kv.getFamily()));
					System.out.println("qualifier:"
							+ Bytes.toString(kv.getQualifier()));
					*/
					if(Bytes.toString(kv.getQualifier()).equals("number"))
					{
						number+=Double.parseDouble(Bytes.toString(kv.getValue()));
					}
					
					if(Bytes.toString(kv.getQualifier()).equals("turnover"))
					{
						money+=Double.parseDouble(Bytes.toString(kv.getValue()));
					}
					//System.out.println("value:" + Bytes.toString(kv.getValue()));
					//System.out.println("timestamp:" + kv.getTimestamp());
					// 组合
					String rowInfo = "row:" + Bytes.toString(kv.getRow())
							+ " family:" + Bytes.toString(kv.getFamily());
					rowInfo += " qualifier:"
							+ Bytes.toString(kv.getQualifier());
					rowInfo += " value:" + Bytes.toString(kv.getValue());
					rowInfo += " timestamp:" + kv.getTimestamp();
					com.util.Log.writeDataLog(rowInfo);
					//System.out.println("-------------------------------------------");
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
		
		}
       System.out.println("单价:"+price+"数量:"+number+"成交额:"+money+"买："+buyNumber+"/"+buyMoney+" 卖:"+saleNumber+"/"+saleMoney);
	}

	/**
	 * 删除表
	 */
	public static void deleteTable() {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tablename)) {
				// 1.先禁用
				admin.disableTable(tablename);
				// 2.删除
				admin.deleteTable(tablename);
				Closeables.close(admin, false);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}