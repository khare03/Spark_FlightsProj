package com.anukrati.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class NiaCalculator {
	private transient SQLContext sqlContext;
	private String airport_file = "src/main/resources/airports.dat";
	private String routes_file = "src/main/resources/routes.dat";

	@SuppressWarnings("deprecation")
	public NiaCalculator() {
		sqlContext = new SQLContext(new SparkContext("local[2]", "NiaCalculator"));

	}

	public void run() {
		Dataset<Row> airport = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load(airport_file);
		//different column name but same data file
		Dataset<Row> srcAirport = airport.toDF("airportid","name","city","country","src_iata","icao","src_latitude","src_longitude","altitude","timezone","dst","tz database time","zone","type","src");
		Dataset<Row> dstAirport = airport.toDF("airportid","name","city","country","dst_iata","icao","dst_latitude","dst_longitude","altitude","timezone","dst","tz database time","zone","type","src");
		//reading route data
		Dataset<Row> routes = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load(routes_file);
		//joining source to route on key src_iata =source
		Dataset<Row> joinedDs = srcAirport.join(routes, srcAirport.col("src_iata").equalTo(routes.col("source"))).
				select("src_iata","src_latitude","src_longitude","source","destination");
		//joining at destination
		Dataset<Row> fullyJoinedDs = joinedDs.join(dstAirport,joinedDs.col("destination").
				equalTo(dstAirport.col("dst_iata")));		
        //Filter through BFS and filtering out those src/des where distance>2000
		Dataset<Row> filteredDs = fullyJoinedDs.filter(fullyJoinedDs.col("src_iata").equalTo("BFS")).filter(row -> 
		DistanceUtility.distance(Double.parseDouble(row.getAs("src_latitude")), Double.parseDouble(row.getAs("src_longitude"))
				, Double.parseDouble(row.getAs("dst_latitude")), Double.parseDouble(row.getAs("dst_longitude")), "M")>=2000).distinct();
		//collecting long haul flight data
		filteredDs.collect();
		//filter from data where source =Belfast
		float totalFlightsFromBFS = fullyJoinedDs.filter(fullyJoinedDs.col("src_iata").equalTo("BFS")).distinct().count();
		//no of long haul flights
        float totaLongHaullFlightsFromBFS =  filteredDs.count();
		filteredDs.select("*").show();;
		filteredDs.printSchema();
		
		float percentLongHaulFlights = (totaLongHaullFlightsFromBFS/totalFlightsFromBFS) * 100;
		//calculate % of flights having dist >2000 and src =Belfast
		System.out.println("Percentage of long haul flights flying out from BFS : "+percentLongHaulFlights);
	}
	

	public static void main(String[] args) {
		
		NiaCalculator calculator = new NiaCalculator();
		calculator.run();
	}
}
