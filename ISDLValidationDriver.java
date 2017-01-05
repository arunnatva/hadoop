package com.bac.ecr.hdf.tools.dlv;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import com.bac.ecr.hdf.tools.dlv.beans.AsOfDate;
import com.bac.ecr.hdf.tools.dlv.beans.AsOfDateEnum;
import com.bac.ecr.hdf.tools.dlv.beans.DlvKbeData;
import com.bac.ecr.hdf.tools.dlv.hadoop.GenerateKbeUniqueKeysMapper;
import com.bac.ecr.hdf.tools.dlv.hadoop.KbeGraphTestDataExtractReducer;
import com.bac.ecr.hdf.tools.dlv.hadoop.KbeGraphValidateReducer;
import com.bac.ecr.hdf.tools.dlv.service.ApplicationNameEnum;
import com.bac.ecr.hdf.tools.dlv.service.KbeListService;
import com.bac.ecr.hdf.tools.dlv.utils.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class ISDLValidationDriver extends Configured implements Tool {

	final static Logger logger = Logger.getLogger(ISDLValidationDriver.class);
	
	public int run(String[] args) throws Exception {
		
		logger.info("ISDLValidationDriver.run() : Starting");
		logger.info("Total Number of Arguments : " + args.length);
		
		if (args.length != 5) {
			logger.info("Usage: [PathToKbeIdFile] [TestResultsPath] [ConfigFilePath] [asOfDate] [NumberOfThreads]");
			System.exit(-1);
		}
		
		/*
		 * The As Of Date can be entered on the command line in one of several
		 * different formats. If the provided As Of Date is in an invalid format, 
		 * there is no sense continuing with the validation process.
		 */
		try{
			AsOfDate asOfDate = new AsOfDate();
			asOfDate.setAsOfDate(args[3]);
		}
		catch(IllegalArgumentException e){
			StringBuilder errorMesage = new StringBuilder();
			errorMesage.append("Provided asOfDate is in an invalid format. ");
			errorMesage.append("Acceptable formats are ");
			for(AsOfDateEnum asOfDateEnum : AsOfDateEnum.values()){
				errorMesage.append(asOfDateEnum.getDateFormat());
				errorMesage.append(" ");
			}
			logger.error(errorMesage, e);
			System.exit(-1);
		}
		
		Configuration conf = getConf();
		conf.set("mapreduce.job.reduces", args[4]);
		//restricting the maximum attempts for reducers to 1 which means that only 1 retry is allowed for reducer tasks in case of failure
		conf.set("mapreduce.reduce.maxattempts", "2");
		conf.set("mapreduce.reduce.memory.mb","5120");
		conf.set("mapreduce.reduce.java.opts", "-Xmx4096m");
		conf.set("mapreduce.task.timeout","0");
		
		
		logger.info(" number of threads requested : " + conf.get("mapreduce.job.reduces"));
		logger.info("Input File Path : "+args[0]);
		logger.info("Test Results Output File Path : " + args[1]);
		logger.info("Configuration File Path : "+ args[2]);
		
		
		
		Job job = Job.getInstance(conf);
					
		//Pass the Configuration File on local fileSystem into Distributed Cache and it can be accessed in either Mapper and/or Reducer
		Configuration jobsConf = job.getConfiguration();
		
		//Inject the command line arguments into Hadoop's Configuration Object
		jobsConf.set("kbeListFile",args[0]);
		jobsConf.set("testResultsOutputPath", args[1]);
		jobsConf.set("asOfDate", args[3]);
		jobsConf.set("ConfigFilePath",args[2]);
		jobsConf.set("NumberOfThreads",args[4]);
		
				
		//Add the Configuration file into Distributed Cache
		addToDistributedCache(job);		
		
		//This method retrieves the list of KBE IDs and populates the list into Input File(Kbe List File)
		if (!populateKbeListFile(jobsConf)) {
			logger.error("Failure retrieving kbeIds from ISDL. Terminating the Validation Process...");
			System.exit(-1);
		}
				
		job.setJarByClass(ISDLValidationDriver.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(GenerateKbeUniqueKeysMapper.class);
		
		//This method sets up the correct reducer based on the functionality you want to run
		setReducer(job);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setPartitionerClass(UniformKbeDistributionPartitioner.class);
				
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
				
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);
		
		/*
		 * Delete output filepath if already exists
		 */
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		return job.waitForCompletion(true) ? 0: 1;
		
	}
		
	public static void main(String[] args) throws Exception {
		logger.info("ISDLValidationDriver.main() : Starting");
		
		/*
		 * java -Dlog4j.configuration=file:///Path/To/log4j/PropertiesFile -cp /Path/to/Jar:/hadoop/ClassPath <ClassName> <PathToKbeIdFile> <appName> <MonthEndDate> <PathToConfigFile> <NumberOfThreads> 
		 * java -Dlog4j.configuration=file:///home/nbsalpid/zkzb9lr/ALV/config/log4j.properties -cp ${CURR_DIR}/ISDLAlvApplication-0.0.1-SNAPSHOT.jar:`hadoop classpath` com.bac.ecrcorese.isdlalv.ISDLValidationDriver /user/nbsalpid/akn PEAKS 08/31/2015 /home/nbsalpid/zkzb9lr/config/application-DEV.properties 20
		 * 
		 * Path to Kbe ID file : args[0]
		 * Application Name    : args[1]
		 * Month End Date	   : args[2]
		 * Path to Configuration File : args[3]
		 * Number of Parallel Processes to be triggered : args[4]
		 */
		
		ISDLValidationDriver isdlValidationDriver = new ISDLValidationDriver();
		int res = ToolRunner.run(isdlValidationDriver, args);
		System.exit(res);
		
	}
	
	/**
	 * Sets the reducer based on the value of the <code>functionType</code> property
	 * in the configuration properties file. If the value of <code>functionType</code> 
	 * is <code>DataExtract</code>, then the reducer is set to extract sample data files. 
	 * If the value of <code>functionType</code> is null or any other value, then the 
	 * reducer is set to perform lineage validation.
	 * @param job Job
	 */
	public void setReducer(Job job) {
		logger.info("Setting Reducer type based on the functionality we need :");

		Configuration conf = job.getConfiguration();

		File configFile = new File(conf.get("ConfigFilePath"));
		Config configProps = ConfigFactory.parseFile(configFile);
		String reducerType = ConfigUtils.getProperty(configProps, "functionType");
		if ("DataExtract".equalsIgnoreCase(reducerType)) {
			job.setReducerClass(KbeGraphTestDataExtractReducer.class);
		} else {
			job.setReducerClass(KbeGraphValidateReducer.class);
		}
	}

	public void addToDistributedCache(Job job) throws IOException, URISyntaxException {
		
		logger.info("adding ticket cache to distributed cache :");
		Configuration conf = job.getConfiguration();

		File configFile = new File(conf.get("ConfigFilePath"));
		Config configProps = ConfigFactory.parseFile(configFile);
			
		String hdfsAppPath = configProps.getString("hdfsAppPath");
		
		job.setJobName("DLV Job for "+ configProps.getString("appName"));
		
		FileSystem fs = FileSystem.newInstance(conf);
								
		fs.copyFromLocalFile(new Path(configFile.getAbsolutePath()),new Path(hdfsAppPath+"/ConfigFile"));
		job.addCacheFile(new URI(hdfsAppPath + "/ConfigFile" + "#ConfigFile.properties"));
			
		fs.copyFromLocalFile(new Path(configProps.getString("KRB5CCNAME")),new Path(hdfsAppPath+"/tktcache"));
		//logger.info("Ticket Cache name is : " + "krb5cc_" + System.getProperty("user.name"));
		job.addCacheFile(new URI(hdfsAppPath + "/tktcache" + "#tktcache"));
			
		fs.copyFromLocalFile(new Path(configProps.getString("gss-jaas")), new Path(hdfsAppPath + "/gss-jaas"));
		job.addCacheFile(new URI(hdfsAppPath + "/gss-jaas" + "#gss-jaas"));
		
		String sampleSet = ConfigUtils.getProperty(configProps, "sampleSet");
		if (!StringUtils.isBlank(sampleSet)) {
			fs.copyFromLocalFile(new Path(sampleSet), new Path(hdfsAppPath + "/sampleSet"));
			job.addCacheFile(new URI(hdfsAppPath + "/sampleSet" + "#sampleSet"));
		}
		
	}
	
	public boolean populateKbeListFile(Configuration jobsConf) throws IOException {

		logger.info("populating KBE list :");
		boolean result=false;
		String adhocKbeId;
		List<String> kbeOrPdeList = new ArrayList<String>();
		logger.info("Here is the config file : " + jobsConf.get("ConfigFilePath"));

		File configFile = new File(jobsConf.get("ConfigFilePath"));
		Config configProperties = ConfigFactory.parseFile(configFile);
		String elementType = ConfigUtils.getProperty(configProperties,"elementType");
		adhocKbeId = ConfigUtils.getProperty(configProperties, "adhocKbeId");
		String urlTemplate="";
		if (StringUtils.isBlank(adhocKbeId)) {
			KbeListService kbeServ = new KbeListService(urlTemplate);
			kbeOrPdeList = kbeServ.retrieveKbesOrPdes(ApplicationNameEnum.valueOf(configProperties.getString("appName")),elementType);
			
		} else {			
			kbeOrPdeList = Arrays.asList(adhocKbeId.split(","));
		}


		if (kbeOrPdeList!= null && kbeOrPdeList.size() > 0) {
			logger.info("list of kbes : " + kbeOrPdeList.toString());
			logger.info("number of KBEs retrieved " + kbeOrPdeList.size());
			
			result = true;

			FileSystem fs = FileSystem.newInstance(jobsConf);

			Path kbeListFilePath = new Path(jobsConf.get("kbeListFile"));

			if (fs.exists(kbeListFilePath)) {
				fs.delete(kbeListFilePath,true);
			}

			FSDataOutputStream fsdos = fs.create(kbeListFilePath,true);

			for (String line : kbeOrPdeList ) {

				logger.info("writing kbe id : "+ line);
				fsdos.writeBytes(line);
				fsdos.writeBytes("\n");

			}

			fsdos.close();
		}

		return result;
	}

	
}
