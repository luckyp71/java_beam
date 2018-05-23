package org.java_beam.example1;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Arrays;

import org.java_beam.example.model.Stock;

/*
 * Read dummy data with simple POJO and insert those data into Database (PostgreSQL or MySQL)
 *  using Apache Beam's JdbcIO.
 */

public class Example6 {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);

		// Preparing dummy data
		Collection<Stock> stockList = Arrays.asList(new Stock("AAPL", 2000,"Apple Inc"), 
				new Stock("MSFT", 3000, "Microsoft Corporation"),
				new Stock("NVDA", 4000, "NVIDIA Corporation"),
				new Stock("INTC", 3200, "Intel Corporation"));
		
		// Reading dummy data and save it into PCollection<Stock>
		PCollection<Stock> data = p.apply(Create.of(stockList).withCoder(SerializableCoder.of(Stock.class)));

		// Inserting data into MySQL
		@SuppressWarnings("unused")
		PDone insertData =  data.apply(JdbcIO.<Stock>write()
									
						// Data Source Configuration for PostgreSQL
						.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
								.create("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/practice")
								.withUsername("postgres").withPassword("pratama"))
						
						// Data Source Configuration for MySQL
//						.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
//								.create("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/practice?useSSL=false")
//								.withUsername("root").withPassword("pratama"))			

						.withStatement("insert into stock values(?, ?, ?)")
						.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Stock>() {
							private static final long serialVersionUID = 1L;
							
							public void setParameters(Stock element, PreparedStatement query) throws SQLException {
								query.setString(1, element.getSymbol());
								query.setLong(2, element.getPrice());
								query.setString(3, element.getCompany());
							}
						}));

		// Checking pipeline status
		State pipelineStatus = p.run().waitUntilFinish();
		System.out.println("Pipeline Status: "+pipelineStatus);
		System.out.println("Data Inserted Successfully");
	}
}