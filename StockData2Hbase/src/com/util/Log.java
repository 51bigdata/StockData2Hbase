package com.util;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Log {
	
	static Date date = null;
	static DateFormat format = null;
	static DateFormat formatForFile = null;
	static String time = "";
	static 	String path="";
	/*
	 * 将内容写入文件
	 */
	public static void writeFile(String filePath, String content) {
		File file = new File(filePath);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (Exception e) {
			}
		}
		try {
			OutputStreamWriter write = new OutputStreamWriter(
					new FileOutputStream(file, true), "Utf-8");

			BufferedWriter writer = new BufferedWriter(write);
			writer.write(content);
			writer.close();
			write.close();

		} catch (Exception e) {

		}
	}

	/*
	 * 写数据日志
	 */
	public static void writeDataLog(String content) {
		 path=System.getProperty("user.dir")+"/log/";
		date = new Date();
		format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		time = format.format(date);

		formatForFile = new SimpleDateFormat("yyyy-MM-dd");
		// 判断路径是否存在
		File file = new File(path);
		if (!file.exists() || !file.isDirectory()) {
			file.mkdir();
		}

		writeFile(path+ formatForFile.format(date)
				+ "Data.txt", time + "\r\n" + content + "\r\n");
	}

	/*
	 * 写错误日志
	 */
	public static void writeErrorLog(String content) {
		 path=System.getProperty("user.dir")+"/log/";
		date = new Date();
		format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		time = format.format(date);
		formatForFile = new SimpleDateFormat("yyyy-MM-dd");
		// 判断路径是否存在
		File file = new File(path);
		if (!file.exists() || !file.isDirectory()) {
			file.mkdir();
		}

		writeFile(path + formatForFile.format(date)
				+ "Error.txt", time + "\r\n" + content + "\r\n");
	}
}