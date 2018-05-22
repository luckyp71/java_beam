package org.java_beam.example1;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import java.util.Collection;
import java.util.Arrays;
import java.util.Date;

import org.java_beam.example.model.Stock;

/*
 * Read dummy data with simple POJO using BeamSql
 */

public class Example5 {

	public static void main (String[] args) {
		
		Date time = new Date();
		
		// Prepare dummy data
		Collection<Stock> stockList = Arrays.asList(new Stock("AAPL", 2000,"Apple Inc"), 
											  new Stock("MSFT", 3000, "Microsoft Corporation"),
											  new Stock("NVDA", 4000, "NVIDIA Corporation"),
											  new Stock("INTC", 3200, "Intel Corporation"));
		
		// Create Pipeline
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		
		// Define field data type
		RowType fieldDataType = RowSqlType.builder()
							.withVarcharField("symbol")
							.withBigIntField("price")
							.withVarcharField("company")
							.build();
		
		// Read data from stockList and save it as PCollection of Stock
		PCollection<Stock> stockModel = p.apply(Create.of(stockList)
										.withCoder(SerializableCoder.of(Stock.class))); 
		
		// Convert PCollection of Stock to PCollection of Row with the same schema (row type) defined above
		PCollection<Row> stocks = stockModel.apply(ParDo.of(new DoFn<Stock, Row>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				// Get the current Stock instance
				Stock stock = ctx.element();
				
				// Create a Row with the row type and values from the current Stock
				Row stockRow = Row.withRowType(fieldDataType)
								  .addValues(stock.getSymbol(),
										  	stock.getPrice(),
										  	stock.getCompany()
										  ).build();
//				System.out.println(stockRow);
				// Output the Row representing the current Stock
				ctx.output(stockRow);
			}
		})).setCoder(fieldDataType.getRowCoder());
		
		// Get stock by its symbol query
		PCollection<Row> getBySymbolQuery = stocks.apply(BeamSql.query("SELECT symbol, price, company"
																	+" FROM PCOLLECTION WHERE symbol='NVDA'"));
		
		// Transform PCollection<Row> to PCollection<String>
		PCollection<String> getBySymbolString = getBySymbolQuery.apply(ParDo.of(new DoFn<Row, String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				
				System.out.println("Symbol: "+ctx.element().getString(0)+
									"\nPrice: $"+ctx.element().getLong(1)+
									"\nCompany: "+ctx.element().getString(2));
				
				ctx.output("Symbol: "+ctx.element().getString(0)+
						"\nPrice: "+ctx.element().getLong(1)+
						"\nCompany: "+ctx.element().getString(2));
			}
		}));
		
		// Save the output to local file (change the local path below to your own path)
		@SuppressWarnings("unused")
		PDone output = getBySymbolString.apply("Write Data", TextIO.write().to("/home/lucky/Data-Lucky/Beam/output-in:"+time+".txt"));
		
		State pipelineStatus = p.run().waitUntilFinish();
		System.out.println("\nPipeline Status: "+pipelineStatus);
	}
}