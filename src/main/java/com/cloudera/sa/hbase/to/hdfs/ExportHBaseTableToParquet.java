package com.cloudera.sa.hbase.to.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import parquet.avro.AvroParquetOutputFormat;

public class ExportHBaseTableToParquet {

  public static final String SHOULD_COMPRESSION_CONF = "custom.compressionCodec";
  public static final String SCHEMA_FILE_LOCATION_CONF = "custom.schema.file.location";
  public static final String OUTPUT_PATH_CONF = "custom.output.path";
  public static final String DELIMITER_CONF = "custom.delimiter";
  public static final String ROW_KEY_COLUMN_CONF = "custom.rowkey.column";

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    if (args.length == 0) {
      System.out
          .println("ExportHBaseTableToParquet {tableName} {ColumnFamily} {outputPath} {compressionCodec snappy,gzip} {schemaLocationOnHdfs} {rowkey.column.optional");
      return;
    }

    String table = args[0];
    String columnFamily = args[1];
    String outputPath = args[2];
    String compressionCodec = args[3];
    String schemaFilePath = args[4];

    String rowKeyColumn = "";
    if (args.length > 5) {
      rowKeyColumn = args[5];
    }

    Job job = Job.getInstance();
    job.getConfiguration().set(ROW_KEY_COLUMN_CONF, rowKeyColumn);
    job.getConfiguration().set(SCHEMA_FILE_LOCATION_CONF, schemaFilePath);

    HBaseConfiguration.addHbaseResources(job.getConfiguration());

    job.setJarByClass(ExportHBaseTableToParquet.class);
    job.setJobName("ExportHBaseTableToParquet ");

    Scan scan = new Scan();
    scan.setCaching(500); // 1 is the default in Scan, which will be bad for
                          // MapReduce jobs
    scan.setCacheBlocks(false); // don't set to true for MR jobs
    scan.addFamily(Bytes.toBytes(columnFamily));

    TableMapReduceUtil.initTableMapperJob(table, // input HBase table name
        scan, // Scan instance to control CF and attribute selection
        MyMapper.class, // mapper
        null, // mapper output key
        null, // mapper output value
        job);
    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setOutputPath(job, new Path(outputPath));

    Schema.Parser parser = new Schema.Parser();

    FileSystem fs = FileSystem.get(job.getConfiguration());
    AvroParquetOutputFormat.setSchema(job, parser.parse(fs.open(new Path(schemaFilePath))));

    if (compressionCodec.equals("snappy")) {
      AvroParquetOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    } else if (compressionCodec.equals("gzip")) {
      AvroParquetOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    } else {
      // nothing
    }

    job.setNumReduceTasks(0);

    boolean b = job.waitForCompletion(true);
  }

  public static class MyMapper extends TableMapper<Void, GenericRecord> {

    FileSystem fs;
    ArrayList<String> columns = new ArrayList<String>();
    HashMap<String, byte[]> columnValueMap = new HashMap<String, byte[]>();
    Schema schema;
    String rowKeyColumn;

    byte[] lastRowKey = null;

    @Override
    public void setup(Context context) throws TableNotFoundException, IOException {
      fs = FileSystem.get(context.getConfiguration());
      String schemaFileLocation = context.getConfiguration().get(SCHEMA_FILE_LOCATION_CONF);
      columns = generateColumnsFromSchemaFile(fs, schemaFileLocation);

      rowKeyColumn = context.getConfiguration().get(ROW_KEY_COLUMN_CONF, "");
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      writeLine(context, lastRowKey);
    }

    protected ArrayList<String> generateColumnsFromSchemaFile(FileSystem fs, String schemaFileLocation) throws IOException {

      Schema.Parser parser = new Schema.Parser();

      schema = parser.parse(fs.open(new Path(schemaFileLocation)));

      ArrayList<String> results = new ArrayList<String>();

      for (Field f : schema.getFields()) {
        results.add(f.name());
        System.out.println("Field:" + f.name());
      }

      return results;
    }

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {

      KeyValue[] kvs = value.raw();

      if (lastRowKey == null) {
        lastRowKey = row.get();
      } else if (Bytes.compareTo(lastRowKey, row.get()) != 0) {
        writeLine(context, lastRowKey);
        columnValueMap.clear();
      }
      for (KeyValue kv : kvs) {
        String qualifier = Bytes.toString(kv.getQualifier());
        byte[] val = kv.getValue();
        columnValueMap.put(qualifier, val);
      }
    }

    protected void writeLine(Context context, byte[] rowKey) throws IOException, InterruptedException {

      if (columnValueMap.size() > 0) {
        Record record = new Record(schema);
  
        for (String col : columns) {
  
          
          byte[] value = columnValueMap.get(col);
          
          System.out.print(col + "," + Bytes.toString(value));
          
          if (value != null) {
            putValue(record, col, value);
          } else if (col.equals(rowKeyColumn)) {
            putValue(record, col, rowKey);
          }
        }
        System.out.println();
        context.write(null, record);
      }
    }

    private void putValue(Record record, String col, byte[] value) {
      if (schema.getField(col).schema().getType().equals(Type.STRING)) {
        record.put(col, Bytes.toString(value));
      } else if (schema.getField(col).schema().getType().equals(Type.INT)) {
        record.put(col, Bytes.toInt(value));
      } else if (schema.getField(col).schema().getType().equals(Type.LONG)) {
        record.put(col, Bytes.toLong(value));
      } else {
        throw new RuntimeException("Unknown datatype: " + schema.getField(col).schema().getType());
      }
    }
  }
}
