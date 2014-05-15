package com.cloudera.sa.hbase.to.hdfs;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroReader {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("AvroReader {dataFile} {schemaFile} {max.lines.to.read.optional}");
    }
    
    
    String dataFile = args[0];
    String schemaFile = args[1];
    int recordsToRead = Integer.MAX_VALUE;
    if (args.length > 2) {
      recordsToRead = Integer.parseInt(args[2]);
    }
    
    Schema.Parser parser = new Schema.Parser();
    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    
    Schema schema = parser.parse(fs.open(new Path(schemaFile)));
    
    Path dataFilePath = new Path(dataFile);
    FileStatus fileStatus = fs.getFileStatus(dataFilePath);
    
    AvroFSInput input = new AvroFSInput(fs.open(dataFilePath), fileStatus.getLen());
    
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(input, datumReader);
    System.out.println("Schema: " + dataFileReader.getSchema());
    System.out.println();
    int counter = 0;
    while (dataFileReader.hasNext() && counter++ < recordsToRead) {
      GenericRecord r = dataFileReader.next();
      System.out.println(counter + " : " + r);
    }
  }
}
