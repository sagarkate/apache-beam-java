package com.practice.gcp.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SchemaParser_TEMPLATE_DOESNT_WORK {
    public String toJsonString(String schemaJsonPath) throws IOException {
        ClassLoader classLoader = SchemaParser_TEMPLATE_DOESNT_WORK.class.getClassLoader();
        File file = new File(Objects.requireNonNull(classLoader.getResource(schemaJsonPath)).getFile());
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }

    public JSONArray toJsonArray(String schemaJson) throws ParseException {
        JSONParser jsonParser = new JSONParser();
        return (JSONArray) jsonParser.parse(schemaJson);
    }

    public TableSchema toTableSchema(JSONArray jsonArray) {
        TableSchema schema = new TableSchema();
        List<TableFieldSchema> fields = new ArrayList<>();
        for (Object o : jsonArray) {
            JSONObject field = (JSONObject) o;
            String fieldName = (String) field.get("name");
            String fieldType = (String) field.get("type");
            fields.add(new TableFieldSchema().setName(fieldName).setType(fieldType));
        }
        schema.setFields(fields);
        return schema;
    }

    public TableSchema parse(String jsonSchemaPath) throws IOException, ParseException {
        String jsonSchema = toJsonString(jsonSchemaPath);
        JSONArray jsonArray = toJsonArray(jsonSchema);
        return toTableSchema(jsonArray);
    }

}
