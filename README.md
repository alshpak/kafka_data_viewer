# Kafka Data Viewer

Desktop UI Tool for Kafka that provides ability to read and publish kafka records.

# Description

Functionality includes setup of list necessary connections and connect particular Kafka cluster.

When connected you can use:

- Logging mode - allows to subscribe and listen for selected topics

- Read mode - allows to query particular topic

- Producer mode - allows to publish the message to specified topic

The application supports different message encodings, the currently supported:

- String
- GZIP
- Avro

# Download

The read-to-use binary bundle can be downloaded at github releases page:

https://github.com/alshpak/kafka_data_viewer/releases

# How to run

`java -jar <jar_name> [-n defaultGroupName]`

# Java 11 support

The tool is implemented on JavaFX that is not part of Java 11 anymore.

In order to run it on Java 11 and you need to add JFX module in command line:

**Download JavaFX SDK**
 
Link to OpenJFX: https://openjfx.io/

Link to Download: https://gluonhq.com/products/javafx/

**Run program with JavaFX module**

`java --module-path <path.to>/javafx-sdk-11/lib/ --add-modules=javafx.controls -jar <jar_name> [-n defaultGroupName]`

The example on command line for windows:

`java --module-path "C:\Program Files\Java\javafx-sdk-11.0.2\lib" --add-modules=javafx.controls -jar kafka_data_viewer-all-<version>.jar`
