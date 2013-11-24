package org.ds.logger;

import org.apache.log4j.Logger;
//Class to log activities at two levels debug and info
//level can be set through log4j.properties file
public class DSLogger {
		 static Logger log = Logger.getLogger(DSLogger.class.getName());
		 
		 public static void log(String className, String methodName, String msg){
			 log.debug(className+":"+methodName+"~"+msg);
		 }
		 static Logger admin = Logger.getLogger("admin");
		 
		 public static void logAdmin(String className, String methodName, String msg){
			 admin.debug(className+":"+methodName+"~"+msg);
		 }
		 public static void report(String key, String value){
			 if(value==""){
				 log.info(key);
			 }else{
				 log.info(key+" : "+value);
			 }
		 }
		 static Logger fe = Logger.getLogger("fe");
		 
		 public static void logFE(String className, String methodName, String msg){
			 fe.debug(className+":"+methodName+"~"+msg);
		 }
}
