package org.java_beam.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;

/*
 * Perform a simple data processing with Beam's pipeline
 */

public class Example2 {

	private static PipelineOptions options = PipelineOptionsFactory.create();
	private static Pipeline p = Pipeline.create(options);
	private static PCollection<String> inputData;
	private static PDone outputData;
	private static State result;
	
	private static List<String> data = Arrays.asList("this is example 2","in using",
			"apache beam", "with java direct runner");
	
	public static PCollection<String> readData(List<String> dataInput){
		inputData = p.apply("Read Data", Create.of(dataInput)).setCoder(StringUtf8Coder.of());
		return inputData;
	}
	
	public static PDone writeData(PCollection<String> inputData, String outputPath) {
		outputData = inputData.apply("Write Data", TextIO.write().to(outputPath).withSuffix(".csv"));
		return outputData;
	}
	
	public static void main (String[] args) {
		writeData(readData(data), "/home/lucky/Data-Lucky/Beam/output");
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