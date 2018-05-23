package org.java_beam.example1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import java.util.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/*
 * Retrieving data from Database (PostgreSQL or MySQL) using Apache Beam's JdbcIO
 */

public class Example7 {

	public static void main (String[] args) {
		
		Date time = new Date();
		
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		
		// Retrieving data from database
		PCollection<String> data = p.apply(JdbcIO.<String>read()
				
				// Data Source Configuration for PostgreSQL
				.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
						
						// Data Source Configuration for PostgreSQL
						.create("org.postgresql.Driver","jdbc:postgresql://localhost:5432/your_db")
						
						//Data Source Configuration for MySQL
//						.create("com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/your_db?useSSL=false")		
			
						.withUsername("your_username").withPassword("your_password"))
			
				.withQuery("SELECT symbol, price, company from stock WHERE symbol = ?")
				.withCoder(StringUtf8Coder.of())
				.withStatementPreparator(new JdbcIO.StatementPreparator() {
					private static final long serialVersionUID = 1L;
					@Override
					public void setParameters(PreparedStatement preparedStatement) throws Exception {
						preparedStatement.setString(1, "NVDA");
						
					}
				})
				.withRowMapper(new JdbcIO.RowMapper<String>() {
					private static final long serialVersionUID = 1L;
					public String mapRow(ResultSet resultSet) throws Exception {
						return "Symbol: "+resultSet.getString(1)+"\nPrice: "+resultSet.getLong(2)+
								"\nCompany: "+resultSet.getString(3);
					}
				}));
		
		// Display and save the output
		@SuppressWarnings("unused")
		PDone output = data.apply(ParDo.of(new DoFn<String, String>(){
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				System.out.println(ctx.element());
				ctx.output(ctx.element());
			}
		})).apply(TextIO.write().to("/home/lucky/Data-Lucky/Beam/output-in:"+time+".txt"));
	
		// Check pipeline status
		State result = p.run().waitUntilFinish();	
		System.out.println(result);
	}	
}