package org.java_beam.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import java.util.List;
import java.util.Arrays;

public class Example3 {

	public static void main (String[] args) {
		
		List<String> text = Arrays.asList("This is example 3", "in using",
				"Apache Beam", "with Java Direct Runner");
		
		// Create pipeline
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		
		// Read data and save it as input
		PCollection<String> input = p.apply("Read Data", Create.of(text)).setCoder(StringUtf8Coder.of());
		
		// Transform Data with ParDo.
		// We have input data with PCollection<String> as data type,
		// the method below will compute the length of each element inside list,
		// so that we will transform the input's data type into PCollection<KV<String,Integer>>
		@SuppressWarnings("unused")
		PCollection<KV<String,Integer>> elementWordLength = input.apply( 
				"Compute Word Lengths of Each Element", ParDo.of(new DoFn<String, KV<String,Integer>>(){
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						System.out.println(KV.of(c.element(),"lengths = "+ c.element().length()));
						c.output(KV.of(c.element(),c.element().length()));
					}
				}));
		State result = p.run().waitUntilFinish();
		System.out.println(result);	
	}	
}