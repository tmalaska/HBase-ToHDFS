package com.cloudera.sa.hbase.to.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


public class SampleAvroWriter {
  public static void main(String[] args) throws IOException {
    
    String schemaString = "{\"namespace\": \"example.avro\"," +
       "\"type\": \"record\"," +
       "\"name\": \"User\"," +
       "\"fields\": [" +
            "{\"name\": \"name\", \"type\": \"string\"}," +
            "{\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}," +
            "{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}" +
        "]" +
       "}";
    
    Schema.Parser parser = new Schema.Parser();
    
    Schema schema = parser.parse(schemaString);
    
    System.out.println(schema.getField("name").schema().getType());
    
    File file = new File("users.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, file);
    
    Record record = new Record(schema);
    record.put("name", "Ted");
    record.put("favorite_number", 5);
    record.put("favorite_color", "red");
    
    
    dataFileWriter.append(record);
    
    record = new Record(schema);
    record.put("name", "Karen");
    record.put("favorite_number", 7);
    record.put("favorite_color", "green");
    
    dataFileWriter.append(record);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
    System.out.println("Schema: " + dataFileReader.getSchema());
    while (dataFileReader.hasNext()) {
      GenericRecord r = dataFileReader.next();
      System.out.println(r.get("name") + " " + r.get("favorite_number"));
      
    }
  }
}
