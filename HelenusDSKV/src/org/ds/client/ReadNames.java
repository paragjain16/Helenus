package org.ds.client;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class ReadNames {
	public static void main(String[] args){
		try {
			String lineNo = "25,71,103,203,241,";
			BufferedReader br = new BufferedReader(new FileReader(new File("./top250")));
			String[] lines = lineNo.split(",");
			//System.out.println(lines.length);
			for(String line : lines){
				System.out.println(line);
			}
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
