package org.java_beam.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import java.util.List;
import java.util.Arrays;

public class Example5 {

	public static void main (String[] args) {
	
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		
		List<String> list = Arrays.asList("hei lucky", "pratama halo", "lucky hei","hei pratama","where are you");
		
		PCollection<String> input = p.apply(Create.of(list).withCoder(StringUtf8Coder.of()));
		
		PCollection<String> splitLines = input.apply(ParDo.of(new DoFn<String, String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				for(String word: ctx.element().split("[^a-zA-Z]+")){
					if(!word.isEmpty()) {
						ctx.output(word);
					}
				}
			}
		}));
		
		PCollection<KV<String, Long>> kvTransformation = splitLines.apply(Count.<String>perElement());
		
		PCollection<String> wordCountString = kvTransformation.apply(ParDo.of(new DoFn<KV<String, Long>,String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				System.out.println(ctx.element().getKey()+":"+ctx.element().getValue());
				ctx.output(ctx.element().getKey()+":"+ctx.element().getValue());
			}
		}));
		
		PDone output = wordCountString.apply("Write Text", TextIO.write().to("/home/lucky/Data-Lucky/Beam/output.txt"));
		
		State result = p.run().waitUntilFinish();
		
		System.out.println(result);
		
		
		
	}
}
