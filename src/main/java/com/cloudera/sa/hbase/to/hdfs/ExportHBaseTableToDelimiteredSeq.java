package com.cloudera.sa.hbase.to.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPOutputStream;

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ExportHBaseTableToDelimiteredSeq {
  
  public static final String SHOULD_COMPRESSION_CONF = "custom.should.compress";
  public static final String SCHEMA_FILE_LOCATION_CONF = "custom.schema.file.location";
  public static final String OUTPUT_PATH_CONF = "custom.output.path";
  public static final String DELIMITER_CONF = "custom.delimiter";
  public static final String ROW_KEY_COLUMN_CONF = "custom.rowkey.column";
  
	public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	  if (args.length == 0) {
      System.out
          .println("ExportHBaseTableToDelimiteredSeq {tableName} {ColumnFamily} {outputPath} {compressionCodec} {schemaLocationOnLocal} {delimiter} {rowKeyColumn.optional");
      return;
    }

    String table = args[0];
    String columnFamily = args[1];
    String outputPath = args[2];
    String compressionCodec = args[3];
    String schemaFilePath = args[4];
    String delimiter = args[5];

    String rowKeyColumn = "";
    if (args.length > 6) {
      rowKeyColumn = args[6];
    }
    
    Job job = Job.getInstance();
    job.getConfiguration().set(ROW_KEY_COLUMN_CONF, rowKeyColumn);
    
    HBaseConfiguration.addHbaseResources(job.getConfiguration());
    
    job.getConfiguration().set(SCHEMA_FILE_LOCATION_CONF, schemaFilePath);
    job.getConfiguration().set(OUTPUT_PATH_CONF, outputPath);
    job.getConfiguration().set(DELIMITER_CONF, delimiter);

    job.setJarByClass(ExportHBaseTableToDelimiteredSeq.class);
    job.setJobName("ExportHBaseTableToDelimiteredSeq ");

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
    job.setOutputFormatClass(SequenceFileOutputFormat.class); 
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    if (compressionCodec.equals("snappy")) {
      SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    } else if (compressionCodec.equals("gzip")) {
      SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    } else {
      //nothing
    }
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    
    job.setNumReduceTasks(0);
    
    boolean b = job.waitForCompletion(true);
  }

  public static class MyMapper extends TableMapper<Text, NullWritable> {

    
    String schemaFileLocation;
    int taskId;
    ArrayList<String> columns = new ArrayList<String>();
    HashMap<String, byte[]> columnValueMap = new HashMap<String, byte[]>();
    String delimiter;
    Text text = new Text();
    byte[] lastRowKey = null;
    String rowKeyColumn;
    
    @Override
    public void setup(Context context) throws TableNotFoundException,
        IOException {
      taskId = context.getTaskAttemptID().getTaskID().getId();
      schemaFileLocation = context.getConfiguration().get(SCHEMA_FILE_LOCATION_CONF);
      FileSystem fs = FileSystem.get(context.getConfiguration());
      columns = generateColumnsFromSchemaFile(fs, schemaFileLocation);
      
      delimiter = context.getConfiguration().get(DELIMITER_CONF);
      rowKeyColumn = context.getConfiguration().get(ROW_KEY_COLUMN_CONF);
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      writeLine(context, Bytes.toString(lastRowKey));
      
    }
    
    protected static ArrayList<String> generateColumnsFromSchemaFile(FileSystem fs, String schemaFileLocation) throws IOException {
      ArrayList<String> results = new ArrayList<String>();
      
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(schemaFileLocation))));
      
      String line = reader.readLine();
      
      for (String columnName: line.split(",")) {
        results.add(columnName);
      }
      
      reader.close();
      
      return results;
    }

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context)
        throws InterruptedException, IOException {
      
      KeyValue[] kvs = value.raw();
      
      if (lastRowKey == null) {
        lastRowKey = row.get();
      } else if (Bytes.compareTo(lastRowKey, row.get()) != 0){
        writeLine(context,Bytes.toString(lastRowKey));
        columnValueMap.clear();
      }
      for (KeyValue kv: kvs) {
        String qualifier = Bytes.toString(kv.getQualifier());
        byte[] val = kv.getValue();
        columnValueMap.put(qualifier, val);
      }
    }
    
    protected void writeLine(Context context, String rowKey) throws IOException, InterruptedException {
      StringBuilder strBuilder = new StringBuilder();
      
      boolean isFirst = true;
      
      for (String col: columns) {
        
        if (isFirst) { isFirst = false; } 
        else { strBuilder.append(delimiter); }
        
        byte[] value = columnValueMap.get(col);
        if (value != null) {
          strBuilder.append(Bytes.toString(value));
        } else if (col.equals(rowKeyColumn)) {
          strBuilder.append(rowKey);
        }
      }
      text.set(strBuilder.toString());
      context.write(text, NullWritable.get());
    }
  }
}
