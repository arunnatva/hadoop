package com.bac.ecr.hdf.components.ds;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.QueryExecutionException;
import org.apache.spark.sql.hive.HiveContext;

import com.bac.ecr.hdf.components.ds.beans.DataSourcingConstants.DSErrorCodes;
import com.bac.ecr.hdf.components.ds.service.DataSourcingService;
import com.bac.ecr.hdf.components.ds.service.DataSourcingServiceFactory;
import com.bac.ecr.hdf.components.ds.utils.DataSourcingException;
import com.bac.ecr.hdf.components.ds.utils.DataSourcingUtil;
import com.bac.ecr.hdf.components.utils.commonbeans.Constants;
import com.bac.ecr.hdf.components.utils.commonbeans.RawConfiguration;
import com.bac.ecr.hdf.components.utils.commonbeans.SchemaMappingList;
import com.bac.ecr.hdf.components.utils.commonutils.CommonUtils;
import com.bac.ecr.hdf.components.utils.commonutils.HdfsUtils;
import com.bac.ecr.hdf.components.utils.commonutils.JsonParseUtil;
import com.bac.ecr.hdf.components.utils.commonutils.ValidationUtil;
import com.bac.ecr.hdf.frameworks.logging.HadoopLogFactory;
import com.bac.ecr.hdf.frameworks.logging.HadoopLogger;
import com.bac.ecr.hdf.frameworks.logging.HadoopLogger.DDI_LAYER;

public class DataSourcingDriver {

	final static Logger logger = Logger.getLogger(DataSourcingDriver.class);

	

	/**
	 * Main method for DataSourcingDriver class. 
	 * @param args
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws InvalidPathException
	 * @throws QueryExecutionException
	 * @throws Exception
	 */
	public static void main(String[] args) throws IllegalArgumentException,IOException,InvalidPathException, QueryExecutionException, Exception {
		
				
		Map<String,String> inputArgsMap = CommonUtils.getInputArgsMap(args);
		SparkConf sparkConf = new SparkConf().setAppName("Data Sourcing App");
		sparkConf.set("spark.sql.parquet.compression.codec", "snappy");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		//jsc.setLogLevel("WARN");
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc.sc());
		HiveContext hiveCtx = new HiveContext(jsc.sc());		
		hiveCtx.setConf("hive.exec.dynamic.partition", "true");
		hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
		
		HadoopLogger hadoopLoger = HadoopLogFactory.getInstance(DataSourcingDriver.class.getSimpleName(), DDI_LAYER.SOURCE, jsc.hadoopConfiguration());
		try {
		
		if (!CommonUtils.validateInputConfigParams(inputArgsMap)) {
			logger.error(DSErrorCodes.DSRC_102.value());
			throw new DataSourcingException(DSErrorCodes.DSRC_102.value());
		};
		
		// TODO Auto-generated method stub
		RawConfiguration dataSrcConfig = new RawConfiguration();
		Configuration conf = new Configuration();			
		FileSystem fs = FileSystem.get(conf);
		String configJson = null;
		String mappingJson = null;
		DataSourcingService dsService = null;
		
			try {
				configJson = HdfsUtils.readHdfsFile(fs, new Path(inputArgsMap.get(Constants.CONFIG_JSON).trim()));
			} catch(Exception e) {
				logger.error(DSErrorCodes.DSRC_100.value());
				throw new DataSourcingException(DSErrorCodes.DSRC_100.value());
			}
			
			try {
				mappingJson = HdfsUtils.readHdfsFile(fs, new Path(inputArgsMap.get(Constants.MAPPING_JSON).trim()));			
			} catch(Exception e) {
				logger.error(DSErrorCodes.DSRC_101.value());
				throw new DataSourcingException(DSErrorCodes.DSRC_101.value(),e);	
			}
			
			SchemaMappingList schemaMapLst;
			try {
				schemaMapLst = (SchemaMappingList)JsonParseUtil.parseJSON(mappingJson,new SchemaMappingList());
			} catch(Exception e) {
				logger.error(DSErrorCodes.DSRC_106.value());
				throw new DataSourcingException(DSErrorCodes.DSRC_106.value(),e);
			}
			
			RawConfiguration config;
			try {
				config = (RawConfiguration) JsonParseUtil.parseJSON(configJson, dataSrcConfig);			
			} catch (Exception e) {
				logger.error(DSErrorCodes.DSRC_107.value());
				throw new DataSourcingException(DSErrorCodes.DSRC_107.value(),e);
			}
			
			try {
				ValidationUtil.validateRawConfig(config);
			} catch(Exception e) {
				logger.error(DSErrorCodes.DSRC_108.value());
				throw new DataSourcingException(DSErrorCodes.DSRC_108.value(),e);
			}
			
			List<String> dynamicValuesLst = DataSourcingUtil.getSrcInAndOutColumnMap(schemaMapLst).get(Constants.COLUMNS_NOT_IN_SRC_FILE);
			
			dynamicValuesLst.forEach(arg -> { 				
				if (null == inputArgsMap.get(arg) ) {
					logger.error(DSErrorCodes.DSRC_103.value() + arg );					
					throw new DataSourcingException(DSErrorCodes.DSRC_103.value());
				}
			});
			
		
			if (config.getSrcFeedType().toUpperCase().equals(Constants.DATABASE)) {
			    dsService = DataSourcingServiceFactory.createService(Constants.DATABASE);
				if(null != inputArgsMap.get(Constants.FILTER_CONDITION)){
					dsService.processFeed(jsc, sqlContext, hiveCtx, conf, fs, inputArgsMap, schemaMapLst, config);				
			    } else {
                    logger.error(DSErrorCodes.DSRC_103.value() +Constants.FILTER_CONDITION);
                    logger.error("Please pass either condition or NONE as "+Constants.FILTER_CONDITION);
 					throw new DataSourcingException(DSErrorCodes.DSRC_103.value());
			    }
			} else {                    	
				dsService = DataSourcingServiceFactory.createService(config.getSrcFileFormat());
				dsService.processFeed(jsc,sqlContext,hiveCtx,conf, fs, inputArgsMap, schemaMapLst, config);								
			}
						
		} catch (DataSourcingException dse) {
			// TODO Auto-generated catch block
			hadoopLoger.exception("Sourcing", dse.getMessage(), dse);
			dse.printStackTrace();
			throw dse;
		} finally {
			hadoopLoger.close();
			jsc.close();
		}

	}

}
