package org.ds.invertedIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvertedIndex {
	Map <String, List<Integer>> index = new HashMap<String, List<Integer>>();
	
	public void indexFile(File file) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(file));
		File outputFile = new File("/tmp/imdb.txt");
		FileOutputStream fos = new FileOutputStream(outputFile);
		for(String line = br.readLine(); line!=null; line=br.readLine()){
			
		}
		
	}
	public static void main(String[] args) {
		String str = "sf TEst";
		String[] at = str.split("\\s+");
		for(String curr: at)
			System.out.println(curr);
	}
}
