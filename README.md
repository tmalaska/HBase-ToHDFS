HBase-toHDFS
-----------------------------

### Problem
We have a HBase table and we would like to export it to Text, Seq, Avro, or Parquet.

### How to Run

#### Set up sample tables
* hadoop jar HBaseToHDFS.jar CreateTable exportTest c 5

#### Populate with Sample Data
* hadoop jar HBaseToHDFS.jar PopulateTable 5 1000 exportTest/output exportTest c 1

#### Exports the data to text 
* hadoop jar HBaseToHDFS.jar ExportHBaseTableToDelimiteredTxt exportTest c export.text false txt.schema \|

#### Exports the data to Seq
* hadoop jar HBaseToHDFS.jar ExportHBaseTableToDelimiteredSeq exportTest c export.seq false txt.schema \

#### Exports the data to Avro
* hadoop jar HBaseToHDFS.jar ExportHBaseTableToAvro exportTest c export.arvo false avro.schema

#### Reads a Avro File
* hadoop jar HBaseToHDFS.jar AvroReader export.avro/part-m-00000 avro.schema 10

#### Exports the data to Parquet
* hadoop jar HBaseToHDFS.jar ExportHBaseTableToParquet exportTest c export.parquet false avro.schema

####
* hadoop jar HBaseToHDFS.jar ParquetReader export.parquet/part-m-00000 10

