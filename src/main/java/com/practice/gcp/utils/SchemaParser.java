package com.practice.gcp.utils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ValueProvider;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

public class SchemaParser {

    public ReadableByteChannel getSchemaReadableByteChannel(String schemaGcsPath) {
        ReadableByteChannel readableByteChannel = null;
        try {
            readableByteChannel = FileSystems.open(FileSystems.matchNewResource(schemaGcsPath, false));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return readableByteChannel;
    }

    public ReadableByteChannel getSchemaReadableByteChannel(ValueProvider<String> schemaGcsPath) {
        ReadableByteChannel readableByteChannel = null;
        try {
            readableByteChannel = FileSystems.open(FileSystems.matchNewResource(schemaGcsPath.get(), false));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return readableByteChannel;
    }

    public String getSchemaJson(ReadableByteChannel schemaReadableByteChannel) {
        String schemaJson = "";
        try {
            schemaJson = new String(StreamUtils.getBytesWithoutClosing(Channels.newInputStream(schemaReadableByteChannel)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return schemaJson;
    }

    public JSONArray toJsonArray(String json) {
        JSONParser jsonParser = new JSONParser();
        JSONArray jsonArray = new JSONArray();
        try {
            jsonArray = (JSONArray) jsonParser.parse(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
    }

    public JSONArray getSchemaJsonArray(String schemaGcsPath) {
        ReadableByteChannel readableByteChannel = getSchemaReadableByteChannel(schemaGcsPath);
        String schemaJson = getSchemaJson(readableByteChannel);
        return toJsonArray(schemaJson);
    }

    public JSONArray getSchemaJsonArray(ValueProvider<String> schemaGcsPath) {
        ReadableByteChannel readableByteChannel = getSchemaReadableByteChannel(schemaGcsPath);
        String schemaJson = getSchemaJson(readableByteChannel);
        return toJsonArray(schemaJson);
    }

    public List<TableFieldSchema> getTableSchemaFields(JSONArray jsonArray) {
        List<TableFieldSchema> fields = new ArrayList<>();
        for (Object o : jsonArray) {
            JSONObject field = (JSONObject) o;
            String fieldName = (String) field.get("name");
            String fieldType = (String) field.get("type");
            fields.add(new TableFieldSchema().setName(fieldName).setType(fieldType));
        }

        return fields;
    }

    public TableSchema getTableSchema(String schemaGcsPath) {
        JSONArray jsonArray = getSchemaJsonArray(schemaGcsPath);
        TableSchema schema = new TableSchema();
        List<TableFieldSchema> fields = getTableSchemaFields(jsonArray);
        schema.setFields(fields);

        return schema;
    }
}
