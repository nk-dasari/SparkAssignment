package org.project;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class CreateFile {

	public static void main(String[] args) throws IOException {
		int valCount = Integer.parseInt(args[0]);
		String fileName = args[1];
		writeFile(valCount,fileName);
	}
	
	public static void writeFile(int valCount,String fileName) throws IOException{
		File file = new File(fileName);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		int noOfLines = valCount/2;
		Random rand = new Random();
		String line = new String();
		
		for(int i=0;i<noOfLines;i++){
			line = ThreadLocalRandom.current().nextDouble(1, 2)+","+ThreadLocalRandom.current().nextDouble(2, 3);
			bw.write(line);
			bw.write("\n");
		}
		bw.close();
	}
}
