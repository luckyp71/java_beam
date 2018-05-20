package org.java_beam.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import java.io.IOException;

public class Example1 {
	
	private static PipelineOptions options = PipelineOptionsFactory.create();
	private static Pipeline p = Pipeline.create(options);
	private static PCollection<String> inputData;
	private static PDone outputData;
	private static State result;

	// A method which would read local file, it passes one argument i.e. your local file's path
	public static PCollection<String> readData(String path){
		inputData = p.apply("ReadData", TextIO.read().from(path));
		return inputData;
	}
	
	/* A method which would create and write local file, it passes two arguments i.e. 
	   the readData method and your output file's path
	*/
	public static PDone writeData(PCollection<String> readData, String path) {
		outputData = readData.apply("WriteData", TextIO.write().to(path));
		return outputData;
	}	
	
	public static void main (String[] args) {
		
		// Change the below path with your own local file's path
		writeData(readData("/home/lucky/Data-Lucky/Beam/input.txt"),"/home/lucky/Data-Lucky/Beam/output.txt");
		
		result = p.run().waitUntilFinish();
		System.out.println(result);		
		
		System.out.println(">>> PRESS ENTER TO EXIT <<<");
		try {
			System.in.read();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
}