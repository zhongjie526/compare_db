package com.prudential.DBCompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DbCompareApplication implements CommandLineRunner {

	private final Logger logger = LoggerFactory.getLogger(DbCompareApplication.class);
	private final ComparisonDetails comparisonDetails;
	private final DbComparison dbComparison;

	@Autowired
	public DbCompareApplication(ComparisonDetails comparisonDetails, DbComparison dbComparison) {
		this.comparisonDetails = comparisonDetails;
		this.dbComparison = dbComparison;
	}

	public static void main(String[] args) { SpringApplication.run(DbCompareApplication.class, args); }

	@Override
	public void run(String... args) throws Exception {

		dbComparison.rowCountChecks("ifrs17.ifrs4_sungl_extract","ifrs17.PurchaseOrderDetail");

		System.out.println(comparisonDetails.getCriterion());
		System.out.println(comparisonDetails.getTables());

		comparisonDetails.getTables().forEach(table ->{
			System.out.println("source: "+table.get("source")+", target: "+table.get("target"));
		});
	}


}
