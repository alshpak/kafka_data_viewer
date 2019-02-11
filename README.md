# kafka_data_viewer
Tool that providing ability to access kafka data - items in topics and kafka metadata

# Description

Functionality includes setup of necessary connections and connect to.

When connected:

- Logging mode - allows to listen for selected topics

- Read mode - allows to query particular topic

- Producer mode - allows to publish the message to specified topic

Supported formats are:

- String
- GZIP
- Avro

# How to run

`java -jar <jar_name> [-n defaultGroupName]`

# Java 11 support

The tool is implemented on JavaFX.

In order to run it on Java 11 and later follow the instruction:

- Download JavaFX - https://gluonhq.com/products/javafx/

- Run program with JavaFX module\
`java --module-path <path.to>/javafx-sdk-11/lib/ --add-modules=javafx.controls <package.name>.JavaFX11 -jar <jar_name> [-n defaultGroupName]`