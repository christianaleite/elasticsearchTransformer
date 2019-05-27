package com.aleite.elasticsearchTransform;

import com.aleite.utilities.io.file.fileReader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class elasticSearchMapper {

    Map<String, String> dataTypeMap;

    public void loadDataTypes(String filePath) {
        fileReader reader = new fileReader(filePath);
        String[] fileContentArray = reader.read();
        reader.close();

        if ((fileContentArray != null) && (fileContentArray.length > 0)) {
            String fileContent = arrayToString(fileContentArray);
            try {
                ObjectMapper mapper = new ObjectMapper();
                this.dataTypeMap = ((Map)mapper.readValue(fileContent, new TypeReference<Map<String, String>>(){}));
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public String[][] readMapping(String filePath, String type) {
        String[][] fields = (String[][])null;

        fileReader reader = new fileReader(filePath);
        String[] fileContentArray = reader.read();
        reader.close();

        if ((fileContentArray != null) && (fileContentArray.length > 0)) {
            String fileContent = arrayToString(fileContentArray);

            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(fileContent);

                JsonNode propertiesNode = jsonNode.path("mappings").path(type).path("properties");

                Iterator itr = propertiesNode.fieldNames();

                fields = new String[propertiesNode.size()][3];

                int i = 0;
                String temp = null;
                while (itr.hasNext()) {
                    fields[i][0] = ((String)itr.next());

                    temp = propertiesNode.path(fields[i][0]).path("type").asText();
                    if (this.dataTypeMap.containsKey(temp)) {
                        fields[i][1] = ((String)this.dataTypeMap.get(temp));
                    }else{
                        System.out.println("DataType [" + temp + "] Not Found. Using Default.");
                        fields[i][1] = "quoted";
                    }
                    fields[i][2] = temp;

                    i++;
                }
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
        return fields;
    }

    private String arrayToString(String[] dataArray){
        String data = "";
        for (int i = 0; i < dataArray.length; i++) {
            data = data + dataArray[i];
        }
        return data;
    }
}
