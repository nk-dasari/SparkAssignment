package org.project;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SumAvgRDD {
	
	private static final Logger log = LoggerFactory.getLogger(SumAvgRDD.class);
	public static void main(String[] args){
		String filePath = args[0];
		log.info("FilePath:"+filePath);
		SparkConf conf = new SparkConf().setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> file = jsc.textFile(filePath);
		
		JavaRDD<Double> dValues = file.flatMap(new FlatMapFunction<String,Double>() {

			public Iterable<Double> call(String line) throws Exception {
				return Arrays.asList(Double.parseDouble(line.split(",")[0]),Double.parseDouble(line.split(",")[1]));
			}

		});
		
		Double sum = dValues.reduce(new Function2<Double,Double,Double>(){

			public Double call(Double val1, Double val2) throws Exception {
				
				return val1+val2;
			}
			
		});
		long count = dValues.count();
		Double avg = sum/count;
		log.info("No of Elements:"+count);
		log.info("Sum:"+sum);
		log.info("Average:"+avg);
		
		//Converting to Integer and Deriving
		JavaRDD<Integer> iValues = file.flatMap(new FlatMapFunction<String,Integer>() {

			public Iterable<Integer> call(String line) throws Exception {
				Double val1 = Double.parseDouble(line.split(",")[0]);
				Double val2 = Double.parseDouble(line.split(",")[1]);
				return Arrays.asList(val1.intValue(),val2.intValue());
			}

		});
		
		Integer isum = iValues.reduce(new Function2<Integer,Integer,Integer>(){

			public Integer call(Integer val1, Integer val2) throws Exception {
				
				return val1+val2;
			}
			
		});
		
		Integer iavg = (int) (isum/count);
		log.info("No of Elements:"+count);
		log.info("Sum:"+isum);
		log.info("Average:"+iavg);
		jsc.close();
	}
}
