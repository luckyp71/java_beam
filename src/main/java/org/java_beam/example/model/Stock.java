package org.java_beam.example.model;

import java.io.Serializable;

public class Stock implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String symbol;
	private long price;
	private String company;
	
	public Stock(String symbol, long price, String company) {
		this.symbol = symbol;
		this.price = price;
		this.company = company;
	}
	
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public long getPrice() {
		return price;
	}
	public void setPrice(long price) {
		this.price = price;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
}
