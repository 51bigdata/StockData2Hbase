package com.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * 通过poi进行excel的读写
 * 
 */

public class ExcelPoi implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Workbook workBook=null;
	private Sheet sheet = null;
	private int sheetSumNum = 0;
	private String type=null;
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Workbook getWorkBook() {
		return workBook;
	}

	public void setWorkBook(Workbook workBook) {
		this.workBook = workBook;
	}

	private String sheetName = null;

	/*
 * 初始化方法
 * @param：excelFile :是服务器文件路径，type是指excel 的版本 2003、2007、2010等
 */
	public ExcelPoi(File file, String type) throws IOException {
		InputStream  is = new FileInputStream(file);
		if ("xlsx".endsWith(type)) {
			workBook = new XSSFWorkbook(is);
			this.type=type;
		} else {
			workBook = new HSSFWorkbook(is);
			this.type="xls";
		}
		this.sheetSumNum = workBook.getNumberOfSheets();
	}	
	 
	public int getCurrentSheetIndex()
	{
		return workBook.getSheetIndex(this.sheet);
	}
	//返回sheet的名称
	public String getSheetName() {
		return this.sheetName;
	}

	// 返回 sheet的个数
	public int getSheetSum() {
		return this.sheetSumNum;
	}
	
	public Sheet getSheetByIndex(int index)
	{
		sheet = workBook.getSheetAt(index);		
		return sheet;		
	}
	
	public void setSheetIndex(int index)
	{
		sheet=workBook.getSheetAt(index);
		this.sheetName=sheet.getSheetName();
	}
	
	public boolean isCurrenSheetBlank()
	{
		int rows=sheet.getLastRowNum();
		if(rows>0)
			return false;
		else
			return true;
	}
	
	public Sheet getCurrenSheet()
	{
		return sheet;
	}
		
	public ArrayList <String> getRowValues(int rowNum)
	{
		ArrayList<String> values = new ArrayList<String>();
		Row row=sheet.getRow(rowNum);				
		Short colnum=row.getLastCellNum();			
		for(Short i=0;i<colnum;i++){
			Cell cell=row.getCell(i);
			values.add(getCellValue(cell));
		}		
		return values;
	}
	
	public List<List<String>> getCurrentSheetData(  int beginRowIndex) {
		Row row = null;
		List<List<String>> list = new ArrayList<List<String>>();
		try {
			int num = sheet.getLastRowNum();
			if (0 <= beginRowIndex && beginRowIndex <= num) {
				List <String>rowList = null;
				for (int i = beginRowIndex; i <= num; i++) {
					row = sheet.getRow(i);
					rowList = new ArrayList<String>();
					if (row.getLastCellNum() >= 0) {
						for (int j = 0; j <row.getLastCellNum(); j++) {
							Cell cell=row.getCell(j);
							if(cell!=null)
								rowList.add(getCellValue(cell));
							else
								rowList.add("");
						}
					}
					list.add(rowList);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("excel获取工作表错误");
		}
		return list;
	}
	
	public List<List<String>> getSheetData( int sheetIndex, int beginRowIndex) {
		Sheet sheet = null;
		Row row = null;
		List<List<String>> list = new ArrayList<List<String>>();
		try {
			sheet = workBook.getSheetAt(sheetIndex);
			int num = sheet.getLastRowNum();
			if (0 <= beginRowIndex && beginRowIndex <= num) {
				List <String>rowList = null;
				for (int i = beginRowIndex; i <= num; i++) {
					row = sheet.getRow(i);
					rowList = new ArrayList<String>();
					if (row.getLastCellNum() > 0) {
						for (int j = 0; j < row.getLastCellNum(); j++) {
							rowList.add(getCellValue(row.getCell(j)));
						}
					}
					list.add(rowList);
				}
			}
		} catch (Exception e) {
			System.out.println("excel获取工作表错误");
		}
		return list;
	}

/*
 * 获取某个cell的值 针对2003及其以前的版本
 * @param：excel中的某一个单元格，注意处理日期：日期跟数字类型是同一个类型 HSSFCell.CELL_TYPE_NUMERIC，需要分开处理
 * @return cell的值
 */

	private String getCellValue(HSSFCell cell) {		
		String value = null;
		if(cell==null){
			return "";
		}
		switch (cell.getCellType()) {
			case HSSFCell.CELL_TYPE_STRING://
				value = cell.getRichStringCellValue().getString();
				break;
			case HSSFCell.CELL_TYPE_NUMERIC://
				if (HSSFDateUtil.isCellDateFormatted(cell))  //  如果是date类型则 ，获取该cell的date值
				{   			          
			        Date date = HSSFDateUtil.getJavaDate(cell.getNumericCellValue()); 
			        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
			        value=sdf.format(date);
			    } else { // 纯数字   
			    	double number=cell.getNumericCellValue();//科学计算法表示的，后面都添加了小数点
			    	value = String.valueOf(number);		    	 
			    }			
				break;
			case HSSFCell.CELL_TYPE_BLANK:
				value = "";
				break;
			case HSSFCell.CELL_TYPE_FORMULA:
				value = String.valueOf(cell.getCellFormula());
				break;
			case HSSFCell.CELL_TYPE_BOOLEAN://
				value = String.valueOf(cell.getBooleanCellValue());
				break;
			case HSSFCell.CELL_TYPE_ERROR:
				value = String.valueOf(cell.getErrorCellValue());
				break;
			default:
				value = "";
				break;
		}
		return value;
	}
	
	/*
	 * 获取某个cell的值 针对2007及其以前的版本
	 * @param：excel中的某一个单元格，注意处理日期：日期跟数字类型是同一个类型 HSSFCell.CELL_TYPE_NUMERIC，需要分开处理
	 * @return cell的值
	 */
	private String getCellValueX(XSSFCell cell) {
		String value = null;
		if(cell==null){
			return "";
		}		
		switch (cell.getCellType()) {
			case XSSFCell.CELL_TYPE_STRING://
				value = cell.getRichStringCellValue().getString();
				break;
			case XSSFCell.CELL_TYPE_NUMERIC://
				if (HSSFDateUtil.isCellDateFormatted(cell)) {    
				        //  如果是date类型则 ，获取该cell的date值   			    
				        Date date = HSSFDateUtil.getJavaDate(cell.getNumericCellValue()); 
				        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				        value=sdf.format(date);
				    } else { // 纯数字
				    	double number=cell.getNumericCellValue();//科学计算法表示的，后面都添加了小数点
				    	value = String.valueOf(number);          				        
				    }
				break;
			case XSSFCell.CELL_TYPE_BLANK:
				value = "";
				break;
			case XSSFCell.CELL_TYPE_FORMULA:
				value = String.valueOf(cell.getCellFormula());
				break;
			case XSSFCell.CELL_TYPE_BOOLEAN://
				value = String.valueOf(cell.getBooleanCellValue());
				break;
			case XSSFCell.CELL_TYPE_ERROR:
				value = String.valueOf(cell.getErrorCellValue());
				break;
			default:
				value = "";
				break;
		}
		return value;
	}
	
	public String getCellValue(Cell cell) {
		String value = null;
		if(cell==null){
			return "";
		}		
		switch (cell.getCellType()) {
			case Cell.CELL_TYPE_STRING://
				value = cell.getRichStringCellValue().getString();
				break;
			case Cell.CELL_TYPE_NUMERIC://
				if (DateUtil.isCellDateFormatted(cell)) {    
				        //  如果是date类型则 ，获取该cell的date值   			    
				        Date date = DateUtil.getJavaDate(cell.getNumericCellValue()); 
				        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
				        value=sdf.format(date);
				    } else { // 纯数字
				    	double number=cell.getNumericCellValue();//科学计算法表示的，后面都添加了小数点
				    	value = String.valueOf(number);          				        
				    }
				break;
			case Cell.CELL_TYPE_BLANK:
				value = "";
				break;
			case Cell.CELL_TYPE_FORMULA:
				value = String.valueOf(cell.getCellFormula());
				break;
			case Cell.CELL_TYPE_BOOLEAN://
				value = String.valueOf(cell.getBooleanCellValue());
				break;
			case Cell.CELL_TYPE_ERROR:
				value = String.valueOf(cell.getErrorCellValue());
				break;
			default:
				value = "";
				break;
		}
		return value;
	}
	


	/**
	 * 获取单元类型（xlsx）
	 * @param cell
	 * @return
	 */
	private String getCellTypeX(XSSFCell cell) {
		String value = null;
		if(cell==null){
			return "";
		}
		switch (cell.getCellType()) {
			case XSSFCell.CELL_TYPE_STRING://
				value = "字符";
				break;
			case XSSFCell.CELL_TYPE_NUMERIC://
				if (HSSFDateUtil.isCellDateFormatted(cell))   			        
				   value = "日期";   
				else 		    	
				   value="数字";
				break;
			case XSSFCell.CELL_TYPE_BLANK:
				value = "";
				break;
			case XSSFCell.CELL_TYPE_FORMULA:
				value = "";
				break;
			case XSSFCell.CELL_TYPE_BOOLEAN://
				value = "布尔";
				break;
			case XSSFCell.CELL_TYPE_ERROR:
				value ="";
				break;
			default:
				value = "";
				break;
		}
		return value;
	}

	
	/**
	 * 关闭对excle的读取
	 * @throws IOException
	 */
	public void close(InputStream is) throws IOException {
		if (is != null) {
				is.close();
		}
	}

	public void remove() {
		throw new UnsupportedOperationException("excel");
	}

}
