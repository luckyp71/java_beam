package org.java_beam.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.List;
import java.util.Arrays;

/*
 * Perform word count
 */

public class Example4 {

	public static void main (String[] args) {

		List<String> data = Arrays.asList("This is example 4","in using Apache Beam","with Java Direct Runner.",
				"Apache Beam is","an open source, unified for defining both",
				"batch and streaming data-parallel processing pipelines.");
		
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		
		// Input: Read data from variable
		PCollection<String> input = pipeline.apply(Create.of(data).withCoder(StringUtf8Coder.of()));
		
		// 1st Orchestration: Splitting sentences into word
		PCollection<String> splitLines = input.apply(ParDo.of(new DoFn<String, String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				for(String word: c.element().split("[^a-zA-Z]+")) {
					if(!word.isEmpty()) {
						c.output(word);
					}
				}
			}
		}));
		
		// 2nd Orchestration: Transform words into pairs of key and value, 
		// in which words belong to key and counts belong to value.
		PCollection<KV<String, Long>> kvTransformation = splitLines.apply(Count.<String>perElement());
		
		// 3rd Orchestration: Transform pairs of words and counts into String 
		PCollection<String> wordCountString = kvTransformation.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(KV<String, Long> element) {
				System.out.println(element.getKey()+":"+element.getValue());
				return element.getKey()+":"+element.getValue();
			}
		}));
		
		// Save the output into local file
		PDone output = wordCountString.apply(TextIO.write().to("/home/lucky/Data-Lucky/Beam/output.txt"));

		// Run and wait the pipeline until finish
		State result = pipeline.run().waitUntilFinish();
		
		// Display the pipeline status
		System.out.println(result);
	}
}