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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class ExportHBaseTableToDelimiteredTxt {
  
  public static final String SHOULD_COMPRESSION_CONF = "custom.should.compress";
  public static final String SCHEMA_FILE_LOCATION_CONF = "custom.schema.file.location";
  public static final String OUTPUT_PATH_CONF = "custom.output.path";
  public static final String DELIMITER_CONF = "custom.delimiter";
  public static final String ROW_KEY_COLUMN_CONF = "custom.rowkey.column";
  
	public static void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	  if (args.length == 0) {
      System.out
          .println("ExportHBaseTableToDelimiteredTxt {tableName} {ColumnFamily} {outputPath} {shouldCompressWithGz} {schemaLocationOnHdfs} {delimiter} {rowKeyColumn.Optional}");
      return;
    }

    String table = args[0];
    String columnFamily = args[1];
    String outputPath = args[2];
    String shouldCompression = args[3];
    String schemaFilePath = args[4];
    String delimiter = args[5];
    
    String rowKeyColumn = "";
    if (args.length > 6) {
      rowKeyColumn = args[6];
    }
    
    Job job = Job.getInstance();
    job.getConfiguration().set(ROW_KEY_COLUMN_CONF, rowKeyColumn);
    
    HBaseConfiguration.addHbaseResources(job.getConfiguration());
    
    job.getConfiguration().set(SHOULD_COMPRESSION_CONF, shouldCompression);
    job.getConfiguration().set(SCHEMA_FILE_LOCATION_CONF, schemaFilePath);
    job.getConfiguration().set(OUTPUT_PATH_CONF, outputPath);
    job.getConfiguration().set(DELIMITER_CONF, delimiter);

    job.setJarByClass(ExportHBaseTableToDelimiteredTxt.class);
    job.setJobName("ExportHBaseTableToDelimiteredTxt ");

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
    job.setOutputFormatClass(NullOutputFormat.class); // because we aren't
                                                      // emitting anything from
                                                      // mapper

    job.setNumReduceTasks(0);
    
    boolean b = job.waitForCompletion(true);
  }

  public static class MyMapper extends TableMapper<Text, Text> {

    FileSystem fs;
    BufferedWriter writer;
    String schemaFileLocation;
    int taskId;
    ArrayList<String> columns = new ArrayList<String>();
    HashMap<String, byte[]> columnValueMap = new HashMap<String, byte[]>();
    String delimiter;
    
    byte[] lastRowKey = null;
    String rowKeyColumn;
    
    @Override
    public void setup(Context context) throws TableNotFoundException,
        IOException {
      fs = FileSystem.get(context.getConfiguration());
      taskId = context.getTaskAttemptID().getTaskID().getId();
      schemaFileLocation = context.getConfiguration().get(SCHEMA_FILE_LOCATION_CONF);
      columns = generateColumnsFromSchemaFile(fs, schemaFileLocation);
      
      String outputPath = context.getConfiguration().get(OUTPUT_PATH_CONF);
      boolean shouldCompress = context.getConfiguration().getBoolean(SHOULD_COMPRESSION_CONF, false);
      
      OutputStream outputStream = fs.create(new Path(outputPath + "/part-m-" + StringUtils.leftPad(Integer.toString(taskId), 5, "0")), true);
      if (shouldCompress) {
        outputStream = new GZIPOutputStream(outputStream);
      }
      writer = new BufferedWriter(new OutputStreamWriter(outputStream));
      
      delimiter = context.getConfiguration().get(DELIMITER_CONF);
      rowKeyColumn = context.getConfiguration().get(ROW_KEY_COLUMN_CONF);
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
      writeLine(Bytes.toString(lastRowKey));
      writer.close();
      fs.close();
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
        writeLine(Bytes.toString(lastRowKey));
        columnValueMap.clear();
      }
      for (KeyValue kv: kvs) {
        String qualifier = Bytes.toString(kv.getQualifier());
        byte[] val = kv.getValue();
        columnValueMap.put(qualifier, val);
      }
    }
    
    protected void writeLine(String rowKey) throws IOException {
      StringBuilder strBuilder = new StringBuilder();
      
      boolean isFirst = true;
      
      for (String col: columns) {
        
        if (isFirst) { isFirst = false; } 
        else { strBuilder.append(delimiter); }
        
        byte[] value = columnValueMap.get(col);
        if (value != null) {
          strBuilder.append(Bytes.toString(value));
        } else if (col.equals(rowKeyColumn)){
          strBuilder.append(rowKey);
        }
      }
      
      writer.write(strBuilder.toString() );
      writer.newLine();
    }
  }
}
