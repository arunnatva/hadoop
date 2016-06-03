package com.arun.udtfs;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

public class SplitRowsUDTF extends GenericUDTF {

	final static Logger logger =  Logger.getLogger(DlvTestResultsFormatUdtf.class);

	private List<PrimitiveObjectInspector> inputOIs;
	
	@Override
	public void close() throws HiveException {	
		logger.info("inside close method");
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args ) throws UDFArgumentException {
		
		inputOIs = new ArrayList<PrimitiveObjectInspector>();
		
		if (args.length != 2) {
			throw new UDFArgumentException("the UDTF takes exactly two arguments");
		}
		
		
		for (ObjectInspector arg : args) {
			if (arg.getCategory() != ObjectInspector.Category.PRIMITIVE
					&& ((PrimitiveObjectInspector) arg).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
				throw new UDFArgumentException("the UDTF takes two parameters, and both of them should be strings");
			}
			inputOIs.add((PrimitiveObjectInspector) arg);
		}
		
				
		List<String> fieldNames = new ArrayList<String>();
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		fieldNames.add("src_colname");
		fieldNames.add("src_coltype");
		fieldNames.add("src_colvalue");
		fieldNames.add("tgt_colname");
		fieldNames.add("tgt_coltype");
		fieldNames.add("tgt_colvalue");
		
		for (int i=0;i<6;i++) {
			fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	
	@Override
	public void process(Object[] record) {
		
		String srcData = (String) inputOIs.get(0).getPrimitiveJavaObject(record[0]);
		String tgtData = (String) inputOIs.get(1).getPrimitiveJavaObject(record[1]);
		
		List<String> srcRows = explodeRows(srcData);
		List<String> tgtRows = explodeRows(tgtData);
	    int rowIdx=0;
	    String[] srcColList = null,tgtColList = null;
		while(rowIdx < srcRows.size() || rowIdx < tgtRows.size()) {
			if (srcRows.get(rowIdx) != null) {
				srcColList = srcRows.get(rowIdx).split(",");
			} else {
				srcColList[0]="";srcColList[1]="";srcColList[2]="";
			}
			if (tgtRows.get(rowIdx) != null) {
				tgtColList = tgtRows.get(rowIdx).split(",");
			} else {
				tgtColList[0]="";tgtColList[1]="";tgtColList[2]="";
			}
			try {
				forward(new Object[] {srcColList[0],srcColList[1],srcColList[2],tgtColList[0],tgtColList[1],tgtColList[2]});
			} catch (HiveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	    
	      
	    }
	
	
	
	public List<String> explodeRows(String compositeRec) {
		
		
		// TODO Auto-generated method stub
		List<String> dataRows = new ArrayList<String>();
		StringBuilder dataRow = new StringBuilder();

		Pattern colNamePattern = Pattern.compile(Pattern.quote("colName:") + "(.*?)" + Pattern.quote(","));
		Pattern colTypePattern = Pattern.compile(Pattern.quote("colType:") + "(.*?)" + Pattern.quote(","));
		Pattern colValuesPattern = Pattern.compile(Pattern.quote("colValues:[") + "(.*?)" + Pattern.quote("]"));

		if (compositeRec != null ) {
			String[] dataRecs = compositeRec.split("},");
			for (int i=0;i<dataRecs.length;i++) {
				String columnNm=" ",columnTyp=" ";
				dataRow.setLength(0);
				Matcher colNameMatcher = colNamePattern.matcher(dataRecs[i]);
				if (colNameMatcher.find()) {	
					columnNm = colNameMatcher.group(1);
				}
				Matcher colTypeMatcher = colTypePattern.matcher(dataRecs[i]);
				if (colTypeMatcher.find()) {	
					columnTyp = colTypeMatcher.group(1);
				}

				Matcher colValuesMatcher = colValuesPattern.matcher(dataRecs[i]);

				if (colValuesMatcher.find()) {	
					String columnVals[] = colValuesMatcher.group(1).split(", ");
					if (columnVals.length > 0) {
						for(int valIdx=0;valIdx<columnVals.length;valIdx++) {
							dataRow.setLength(0);
							dataRow.append(columnNm)
							.append(",")
							.append(columnTyp)
							.append(",")
							.append(columnVals[valIdx]);
							//System.out.println( dataRow.toString());
							dataRows.add(dataRow.toString());
						}

					}
				}			

			}

		}
		return dataRows;
	}
 }

