package com.aleite.elasticsearchTransform;

import com.aleite.utilities.verification.contentVerifier;
import com.aleite.utilities.io.file.fileWriter;
import com.aleite.utilities.io.file.fileReader;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class elasticSearchThread implements Runnable {

    private char delimiter;
    private String file;
    private String inDirectory;
    private String outDirectory;
    private String indexName;
    private String recordType;
    private contentVerifier verifier;
    private String[][] fields;

    public elasticSearchThread(String file, String inDirectory, String outDirectory, String indexName, String recordType, String[][] fields, char delimiter) {
        this.verifier = new contentVerifier();

        this.file = file;
        this.inDirectory = inDirectory;
        this.outDirectory = outDirectory;
        this.indexName = indexName;
        this.recordType = recordType;
        this.fields = fields;

        this.delimiter= delimiter;
    }

    public void run() {
        long startTime = System.nanoTime();

        boolean status = true;

        System.out.println("Processing File: " + this.file);

        String convertedData = "";
        String dataBuffer = "";
        fileWriter writer;

        fileReader reader = new fileReader(this.file);

        writer = new fileWriter(this.outDirectory + this.file.substring(this.file.lastIndexOf("/") + 1, this.file.indexOf(".")) + ".json");

        CSVParser parser = new CSVParserBuilder()
                .withSeparator(this.delimiter)
                .withIgnoreQuotations(false)
                .build();



        String[] data;
        int lineCount = 0;

        try {
            while ((data = parser.parseLine(reader.readLine())) != null) {
                if (lineCount > 0) {
                    writer.writeLine("{\"index\":{\"_index\":\"" + this.indexName + "-" + this.file.substring(this.file.lastIndexOf("-") + 1, this.file.indexOf(".")) + "\",\"_id\":" + lineCount + ",\"_type\":\"" + this.recordType + "\"}}");

                    convertedData = "{";
                    if (data.length > 1) {
                        if (data.length == this.fields.length) {
                            for (int x = 0; x < data.length; x++) {
                                convertedData = convertedData + "\"" + this.fields[x][0] + "\":";
                                dataBuffer = data[x].replaceAll("\"", "").trim().replaceAll(" {2,}", " ");
                                if (this.fields[x][1].equalsIgnoreCase("unquoted")) {
                                    if (dataBuffer.length() > 0) {
                                        if (this.verifier.amountIsValid(dataBuffer)) {
                                            convertedData = convertedData + dataBuffer;
                                        } else {
                                            System.out.println("[ERROR] Invalid " + this.fields[x][2] + " on file \"" + this.file + "\", line \"" + (lineCount + 1) + "\": found " + dataBuffer);
                                            status = false;
                                        }
                                    } else {
                                        System.out.println("[WARNING] No value found for " + this.fields[x][2] + " on file \"" + this.file + "\", line \"" + (lineCount + 1) + "\": using default \"0\"");
                                        convertedData = convertedData + "0";
                                    }
                                } else if (this.fields[x][2].equalsIgnoreCase("date")) {
                                    if ((this.verifier.dateIsValid(dataBuffer)) || (this.verifier.timestampIsValid(dataBuffer))) {
                                        convertedData = convertedData + "\"" + dataBuffer + "\"";
                                    } else if (dataBuffer.length() == 0) {
                                        System.out.println("[WARNING] No value found for " + this.fields[x][2] + " on file \"" + this.file + "\", line \"" + (lineCount + 1) + "\": using default \"1900-01-01\"");
                                        convertedData = convertedData + "\"1900-01-01\"";
                                    } else {
                                        System.out.println("[ERROR] Invalid " + this.fields[x][2] + " on file \"" + this.file + "\", line \"" + (lineCount + 1) + "\": found " + dataBuffer);
                                        status = false;
                                    }
                                } else if (this.fields[x][2].equalsIgnoreCase("geo_point")) {
                                    if (dataBuffer.length() > 0) {
                                        convertedData = convertedData + "\"" + dataBuffer + "\"";
                                    } else {
                                        System.out.println("[WARNING] No value found for " + this.fields[x][2] + " on file \"" + this.file + "\", line \"" + (lineCount + 1) + "\": using default \"0\"");
                                        convertedData = convertedData + "\"0\"";
                                    }
                                } else {
                                    convertedData = convertedData + "\"" + dataBuffer + "\"";
                                }
                                if (x < data.length - 1) {
                                    convertedData = convertedData + ",";
                                }
                            }
                            convertedData = convertedData + "}";

                            writer.writeLine(convertedData);
                        } else {
                            System.out.println("[ERROR] Invalid line length on file \"" + this.file + "\", line \"" + (lineCount + 1) + "\": found " + data.length + " and expected " + this.fields.length);

                            System.out.println("###### Line Debug ######");
                            for (int y = 0; y < data.length; y++) {
                                System.out.println("[" + (y + 1) + "] " + (y < this.fields.length ? this.fields[y][0] : "Desconocido") + " : " + data[y]);
                            }
                            System.out.println("###### End Line Debug ######");
                            status = false;
                        }
                    }
                    if (!status) {
                        break;
                    }
                }
                lineCount++;
            }
            reader.close();
        }catch (java.io.IOException e){
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;

        System.out.println("Finished Processing File: " + this.file + " (" + (timeElapsed / 1000000L) + " milliseconds)");
    }


}
