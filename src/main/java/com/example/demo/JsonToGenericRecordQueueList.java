package com.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonToGenericRecordQueueList {

    public static void main(String[] args) throws IOException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            // Load the schema
            Schema schema = new Schema.Parser().parse(new File("C://Development//SourceCode//demo//src//main//avro//Stream.avsc"));
            // Directory containing the files
            File dir = new File("C://Development//SourceCode//demo//src//main//resources/");
            // Filter for files starting with "Stream" and having .avro extension
            File[] files = dir.listFiles((d, name) -> name.startsWith("Stream_"));
            if (files != null) {
                // output Parquet file path
                String outputPath = "C:\\Development\\SourceCode\\demo\\output\\Stream_Combined.parquet";
                outputPath = outputPath.replace(".json", ".parquet"); // Ensure it's a .parquet file

                List<GenericRecord> allRecords = new ArrayList<>(); // Collect all records here

                for (File file : files) {
                    System.out.println("Processing file: " + file);
                    FileReader fileReader = new FileReader(file);
                    char[] buffer = new char[(int) file.length()];
                    fileReader.read(buffer);
                    String content = new String(buffer);
                    fileReader.close();

                    String modifiedContent = content.replace("\\", "")
                            .replace("\"{", "{")
                            .replace("}\"", "}");
                    Map<?, ?> root = objectMapper.readValue(modifiedContent, Map.class);
                    GenericRecord rootRecord = mapToGenericRecord(root, schema);
                    allRecords.add(rootRecord);
                }
                // Now, write allRecords to a single Parquet file
                writeRecordsToParquet(allRecords, schema, outputPath);
                System.out.println("Successfully wrote all GenericRecords to a single Parquet file: " + outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Successfully converted JSON to GenericRecord.");
    }

    public static void writeRecordsToParquet(List<GenericRecord> records, Schema schema, String outputPath) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(outputPath))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericRecord record : records) {
                writer.write(record);
            }
        }
    }


    private static GenericRecord mapToGenericRecord(Map<?, ?> map, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = map.get(field.name());

            // Check if the value is null before proceeding
            if (value == null) {
                continue; // Skip this field or handle it as you see fit
            }

            Schema fieldSchema = field.schema();
            Schema.Type fieldType = fieldSchema.getType();

            if (fieldType.equals(Schema.Type.RECORD)) {
                // Nested record
                record.put(field.name(), mapToGenericRecord((Map<?, ?>) value, fieldSchema));
            } else if (fieldType.equals(Schema.Type.ARRAY)) {
                // Array of records or primitives
                List<?> valueList = (List<?>) value;
                if (valueList == null) continue; // Ensure the list is not null
                List<Object> resultList = new ArrayList<>();
                Schema elementSchema = fieldSchema.getElementType();

                for (Object item : valueList) {
                    if (item == null) {
                        continue; // Skip null items or handle as needed
                    }
                    if (elementSchema.getType().equals(Schema.Type.RECORD)) {
                        // Nested record within an array
                        resultList.add(mapToGenericRecord((Map<?, ?>) item, elementSchema));
                    } else {
                        // Primitive type within an array
                        resultList.add(item);
                    }
                }
                record.put(field.name(), resultList);
            } else if (fieldType.equals(Schema.Type.UNION)) {
                // Handle UNION type (nullable fields)
                Schema actualSchema = fieldSchema.getTypes().get(1);
                if (actualSchema.getType().equals(Schema.Type.RECORD)) {
                    record.put(field.name(), mapToGenericRecord((Map<?, ?>) value, actualSchema));
                } else {
                    record.put(field.name(), value);
                }
            } else {
                // Simple fields (int, string, etc.)
                record.put(field.name(), value);
            }
        }
        return record;
    }


}