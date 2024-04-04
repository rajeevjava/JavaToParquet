package com.example.JavaToParquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToGenericRecord {

    public static void main(String[] args) throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();
        Schema.Parser parser = new Schema.Parser();

        // Load the schema
        Schema schema = parser.parse(new File("C://Development//SourceCode//GitHub//JavaToParquet//src//main//avro//Test_Data.avsc"));

        // Read the JSON file
        Map<?, ?> root = objectMapper.readValue(new File("C://Development//SourceCode//GitHub//JavaToParquet//src//main//resources/Test_Data.json"), Map.class);

        // Convert the root map to a GenericRecord
        GenericRecord rootRecord = mapToGenericRecord(root, schema);

        // Now rootRecord contains your data and can be used as needed
        // genericRecord is now ready for further processing, such as writing to a Parquet file
        System.out.println("Successfully converted JSON to GenericRecord.");

        String path = "S3_BUCKET_NAME/PATH/file.parquet";
        path = "C://Development//SourceCode//GitHub//JavaToParquet//output//Test_Data.parquet";
        writeRecordToParquet(rootRecord, schema, path);
        System.out.println("Successfully wrote GenericRecord to Parquet file.");
    }

    private static void writeRecordToParquet(GenericRecord record, Schema schema, String filePath) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            writer.write(record);
        }
    }

    private static GenericRecord mapToGenericRecord(Map<?, ?> map, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = map.get(field.name());

//            if (value == null) {
//                if ("asset".equals(field.name())) {
//                    // Handle null 'asset' field specifically
//                    // Example: throw new RuntimeException("Required field 'asset' is missing");
//                } else if (field.schema().getType().equals(Schema.Type.UNION)) {
//                    record.put(field.name(), null);
//                } else {
//                    System.out.println("Warning: Null value for non-nullable field: " + field.name());
//                }
//            } else {
                // Handling non-null values
                Schema fieldSchema = field.schema();
                Schema.Type fieldType = fieldSchema.getType();

                if (fieldType.equals(Schema.Type.RECORD)) {
                    // Nested record
                    record.put(field.name(), mapToGenericRecord((Map<?, ?>) value, fieldSchema));
                } else if (fieldType.equals(Schema.Type.ARRAY)) {
                    // Array of records or primitives
                    List<?> valueList = (List<?>) value;
                    List<Object> resultList = new ArrayList<>();
                    Schema elementSchema = fieldSchema.getElementType();

                    for (Object item : valueList) {
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
                    // Assuming the first type in the union is null for optional fields
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
//            }
        }
        return record;
    }

}