package org.project;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.project.model.DoubleVal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SumAvgDF {

	private static final Logger log = LoggerFactory.getLogger(SumAvgDF.class);
	public static void main(String[] args){
		String filePath = args[0];
		SparkConf conf = new SparkConf().setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sc = new SQLContext(jsc);
		JavaRDD<String> file = jsc.textFile(filePath);
		JavaRDD<DoubleVal> schema = file.map(new Function<String,DoubleVal>(){

			public DoubleVal call(String line) throws Exception {
				DoubleVal dval = new DoubleVal();
				dval.setValue1(Double.parseDouble(line.split(",")[0]));
				dval.setValue2(Double.parseDouble(line.split(",")[1]));
				return dval;
			}
			
		});
		
		DataFrame dvalDF = sc.createDataFrame(schema, DoubleVal.class);
		dvalDF.registerTempTable("dvalTable");
		
		DataFrame dvalDFResult = sc.sql("SELECT avg(value1),avg(value2) FROM dvalTable");
		List<Row> vals = dvalDFResult.javaRDD().collect();
		Double sum = 0.0;
		for(Row row:vals){
			sum+=row.getDouble(0);
			sum+=row.getDouble(1);
		}
		int count = dvalDFResult.columns().length;
		Double avg = sum/count;
//		log.info("Sum:"+sum);
 		log.info("Average:"+avg);
 		DataFrame dvalDFSample = dvalDF.sample(true, 0.1).limit(2);
 		log.info("Sample Count:"+dvalDFSample.count());
	}

}
