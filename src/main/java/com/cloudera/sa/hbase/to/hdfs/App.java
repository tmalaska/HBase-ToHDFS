package com.cloudera.sa.hbase.to.hdfs;

public class App {
  public static void main(String[] args) throws Exception {
    System.out.println("Version: 0.0.5");
    if (args.length == 0) {
      System.out.println("commends:");
    }
    String command = args[0];

    String[] subArgs = new String[args.length - 1];
    System.arraycopy(args, 1, subArgs, 0, args.length - 1);

    if (command.equals("CreateTable")) {
      CreateTable.main(subArgs);
    } else if (command.equals("PopulateTable")) {
      PopulateTable.main(subArgs);
    } else if (command.equals("ExportHBaseTableToAvro")) {
      ExportHBaseTableToAvro.main(subArgs);
    } else if (command.equals("ExportHBaseTableToDelimiteredSeq")) {
      ExportHBaseTableToDelimiteredSeq.main(subArgs);
    } else if (command.equals("ExportHBaseTableToDelimiteredTxt")) {
      ExportHBaseTableToDelimiteredTxt.main(subArgs);
    } else if (command.equals("ExportHBaseTableToParquet")) {
      ExportHBaseTableToParquet.main(subArgs);
    } else if (command.equals("AvroReader")) {
      AvroReader.main(subArgs);
    } else if (command.equals("ParquetReader")) {
      ParquetReader.main(subArgs);
    } else {
      System.out.println("Involve command:" + command);
    }
  }
}
