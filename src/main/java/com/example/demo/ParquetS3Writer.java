package com.example.demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetS3Writer {
    public static void main(String[] args) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"age\", \"type\": \"int\"}\n" +
                " ]\n" +
                "}");

        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY");
        conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY");
        // Replace YOUR_BUCKET_NAME and PATH with your actual bucket name and desired path
        String uri = "s3a://YOUR_BUCKET_NAME/PATH/yourfile.parquet";

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(uri))
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            GenericRecord user1 = new GenericData.Record(schema);
            user1.put("name", "John Doe");
            user1.put("age", 30);

            writer.write(user1);

            // Write more records as needed
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
