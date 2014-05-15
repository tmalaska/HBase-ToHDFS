package com.cloudera.sa.hbase.to.hdfs;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;

public class ParquetReader {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("AvroReader {dataFile} {max.lines.to.read.optional}");
    }
    
    String dataFile = args[0];
    int recordsToRead = Integer.MAX_VALUE;
    if (args.length > 1) {
      recordsToRead = Integer.parseInt(args[1]);
    }
    
    //Schema.Parser parser = new Schema.Parser();
    //Configuration config = new Configuration();
    //FileSystem fs = FileSystem.get(config);
    
    //Schema schema = parser.parse(fs.open(new Path(schemaFile)));
    
    Path dataFilePath = new Path(dataFile);
    
    AvroParquetReader<GenericRecord> reader =  new AvroParquetReader<GenericRecord>(dataFilePath);
    
    Object tmpValue;
    
    
    
    int counter = 0;
    while ((tmpValue = reader.read()) != null && counter++ < recordsToRead) {
      GenericRecord r = (GenericRecord)tmpValue;
      System.out.println(counter + " : " + r);
    }
  }
}
