package com.example.JavaToParquet;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JsonToAvroSchemaGenerator {

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // Load and parse the JSON file
        JsonNode rootNode = mapper.readTree(new File("C://Development//SourceCode//GitHub//JavaToParquet//src//main//resources/test_data.json"));

        Schema schema = processNode("ExampleRecord", "com.example", rootNode);

        System.out.println(schema.toString(true));
    }

    private static Schema processNode(String recordName, String namespace, JsonNode node) {
        SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record(recordName)
                .namespace(namespace)
                .fields();

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();

            Schema fieldSchema = inferSchemaForNode(fieldName, fieldValue);
            schemaFields.name(fieldName).type(fieldSchema).noDefault();
        }

        return schemaFields.endRecord();
    }

    private static Schema inferSchemaForNode(String nodeName, JsonNode node) {
        if (node.isObject()) {
            return processNode(nodeName, nodeName, node);
        } else if (node.isArray()) {
            // For simplicity, assume the array is not empty and get the type from the first element
            JsonNode firstElement = node.elements().next();
            Schema elementSchema = inferSchemaForNode(nodeName, firstElement);
            return Schema.createArray(elementSchema);
        } else if (node.isInt()) {
            return Schema.create(Schema.Type.INT);
        } else if (node.isTextual()) {
            return Schema.create(Schema.Type.STRING);
        } else if (node.isBoolean()) {
            return Schema.create(Schema.Type.BOOLEAN);
        }
        // Add more type cases as needed
        return Schema.create(Schema.Type.STRING); // Fallback type
    }
}
