package org.ds.client;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.ds.logger.DSLogger;
import org.ds.networkConf.XmlParseUtility;


public class ReadNames {
	private static final String FILENAME = "top250";
	public static void read(String lineNo){
		try {
			//String lineNo = "25,71,103,203,241,";
			DSLogger.logFE("ReadNames", "read", "Retreiving titles present in line nos:"+lineNo);
			File currentJavaJarFile = new File(XmlParseUtility.class
					.getProtectionDomain().getCodeSource().getLocation()
					.getPath());
			String currentJavaJarFilePath = currentJavaJarFile
					.getAbsolutePath();
			String currentRootDirectoryPath = currentJavaJarFilePath.replace(
					currentJavaJarFile.getName(), "");
			BufferedReader br = new BufferedReader(new FileReader(new File(currentRootDirectoryPath+ FILENAME)));
			String[] lines = lineNo.split(",");
			//System.out.println(lines.length);
			/*for(String line : lines){
				System.out.println(line);
			}*/
			//System.out.println(lines);
			ArrayList<String> movies = new ArrayList<String>();
			int i=0;
			int currLine = 1;
			while(i < lines.length){
				if(currLine++ == Integer.parseInt(lines[i])){
					try {
						movies.add(br.readLine());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					i++;
				}else{
					br.readLine();
				}
			}
			System.out.println(movies);
		} catch (FileNotFoundException e) {
			
			DSLogger.logFE("ReadNames", "read", e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
		
			DSLogger.logFE("ReadNames", "read", e.getMessage());
			e.printStackTrace();
		}
		
	}
}
