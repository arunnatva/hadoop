package com.arun.code;

import java.io.IOException;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.logicalLayer.FrontendException;

public class ParseEventMetrics extends EvalFunc <String> {

@Override
public String exec (Tuple tuple) throws IOException {

   if (tuple == null || tuple.size() == 0) {
       return null;
	  }
   try {
      String eventDet = (String)tuple.get(0);
	  StringTokenizer st = new StringTokenizer(eventDet);
	  String eAttr = null;
	  String[] ealist;
	  ealist = new String[13];
	  int isEveFinished = 0;
	  int isFinalCtr = 0;
	  while (st.hasMoreTokens()) {
	     String nt = st.nextToken();
		 if (nt.startsWith("{(job_")) {
		 StringTokenizer jn = new StringTokenizer(nt,"(");
		 jn.nextToken();
		 eAttr = jn.nextToken();
		 ealist[0] = jAttr.substring(0,jAttr.length() - 1 );
		 }
		 if (nt.startsWith("SUBMIT_TIME") || nt.startsWith("USER") || nt.startsWith("JOB_QUEUE") || nt.startsWith("JOB_PRIORITY") || nt.startsWith("FINISHED_MAPS")
		 || nt.startsWith("FINISHED_REDUCES") || nt.startsWith("FAILED_MAPS") || nt.startsWith("FAILED_REDUCES") || nt.startsWith("FINISH_TIME") ||
		 nt.startsWith("JOB_STATUS")) 
		 {
		    StringTokenizer eventProp = new StringTokenizer(nt,"\"");
			while (eventProp.hasMoreTokens()) {
			       eventProp.nextToken();
				   String tmpstr = eventProp.nextToken();
				   eAttr = eAttr.concat(tmpstr);
				   if (nt.startsWith("USER")) {
				         ealist[1] = tmpstr;
					  }
					  if (nt.startsWith("JOB_QUEUE")) {
					     ealist[2] = tmpstr;
					  }
					  if (nt.startsWith("JOB_PRIORITY")) {
					     ealist[3] = tmstr;
					  }
		              if (nt.startsWith("SUBMIT_TIME")) {
					     ealist[4] = tmpstr;
					  }
					  if (nt.startsWith("FINISH_TIME")) {
					      ealist[5] = tmpstr;
					  }
                      if (nt.startsWith("FINISHED_MAPS")) {
                          ealist[6] = tmpstr;
                      }						  
					  if (nt.startsWith("FINISHED_REDUCES")) {
                          ealist[7] = tmpstr;
                      }						  
					  if (nt.startsWith("FAILED_MAPS")) {
                          ealist[8] = tmpstr;
                      }						  
					  if (nt.startsWith("FAILED_REDUCES")) {
                          ealist[9] = tmpstr;
                      }						  
					  if (nt.startsWith("JOB_STATUS")) {
                          if (isJobFinished == 1 ) {
						     ealist [10] = tmpstr;
						     isJobFinished = 0;
							 }
                      }	
                  }
                }
                  if (nt.startsWith("COUNTERS")) {
                       System.out.println(" we see counters" + nt);
						isFinalCtr = 1;
					  }
				  if ( isFinalCtr == 1 ) {
                         if (nt.contains("PHYSICAL_MEMORY") || nt.contains("VIRTUAL_MEMORY")) {
								StringTokenizer st2 = new StringTokenizer(nt,")");
								String cpumemtk = st2.nextToken();
								if (cpumemtk.contains("ms") || cpumemtk.contains("snapshot")) {
								   String temp = st2.nextToken().substring(1);
								   if (cpumemtk.contains("ms")) {
								            ealist[11] = temp;
										}
								   if (cputmemtk.contains("snapshot")) {
										ealist[12] = temp;
										isFinalCtr = 0
										}
							    }
					     	}
						}
					}
					
					String finalJobRec = new String();
					for (String strtemp : ealist ) {
					  if (strtemp == null ) {
					        finalJobRec = finalJobRec.concat("null");
							}
					  else 
						{
						  finalJobRec = finalJobRec.concat(strtemp);
						}
						finalJobRec = finalJobRec.concat(",");
					  }
					  return finalJobRec;
					}
					
					catch (ExecException e ) {
					      throw new IOException ("something went wrong with the string input", e);
						  }
					}
			}
