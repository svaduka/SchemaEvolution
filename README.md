# SchemaEvolution
This project deals with the schema changes in the meta file.
Currently I am looking in to BigData Hadoop schema accomadations like addition,delition,column position change, upcasting the column.

The base property for the project is PRTS.properties. Based on the requirements we can change the properties to change the project development.

Currently i am creating a loose coupling between the objects creation for files like avro, going forward will implement for Parquet, trevini,ORC etc.. formats.

The files will be created as external tables on hive. So that the end user can get the data for provisioning etc..
