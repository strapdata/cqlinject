/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elassandra.cqlinject.cmd;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

import org.elassandra.cqlinject.CqlInject;
import org.elassandra.cqlinject.CqlInject.InjectCommand;

import com.datastax.driver.core.TableMetadata;

@Command(name = "csv", description = "Insert a CSV file in cassandra table (first line = columns names, second line = MessageFormat patterns for each field)")
public class CsvCommand extends InjectCommand {

    @Option(name = {"--separator"}, description = "CSV separator, default is ','")
    public String separator = ",";

    @Option(name = {"--language"}, description = "Local language, default is 'en'")
    public String language = "en";
    
    @Option(name = {"--country"}, description = "Local country, default is 'EN'")
    public String country = "EN";
    
    @Arguments(description = "CSV input file")
    public String inputFile = null;
    
    @Override
    public void execute(CqlInject injector) throws IOException, TimeoutException, ParseException, InterruptedException {
        injector.connect();
        Locale.setDefault(new Locale(language, country));
        
        int lineCount = 0;
        int insertCount=0;
        int errorCount=0;
        
        System.out.println("Reading file "+inputFile+" separator='"+separator+"'");
        File f = new File(inputFile);
        if (f.exists()) {
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String headersLine = br.readLine();
            String[] headers = headersLine.split(separator);
            System.out.println(headersLine);
            String formatLine = br.readLine();
            System.out.println(formatLine);
            
            MessageFormat[] messageFormats = new MessageFormat[headers.length];
            int z = 0;
            for(String fm : formatLine.split(separator))
                messageFormats[z++] = new MessageFormat(fm);
            
            TableMetadata metadata = null;
            try {
                metadata = injector.getTableMetadata(injector.keyspace, table);
            } catch(NullPointerException e) {
            }
            
            String insert = String.format("INSERT INTO \"%s\".\"%s\" (", injector.keyspace, table);
            String qmarks = new String();
            for(String h : headers) {
                if (qmarks.length() > 0) {
                    qmarks += ',';
                    insert += ',';
                }
                insert += "\""+h+"\"";
                qmarks += "?";
            }
            insert += ") VALUES ("+qmarks+")";
            
            String line;
            boolean firstLine = true;
            while ( (line = br.readLine()) != null) {
                lineCount++;
                if (line.startsWith("#")) 
                    continue;
                
                String[] fields = line.split(separator);
                Object[] values = new Object[headers.length];
                for(int i= 0; i < fields.length; i++) {
                    try {
                        values[i] = (fields[i] == null || fields[i].length()==0) ? null : messageFormats[i].parse(fields[i])[0];
                    } catch(java.text.ParseException e) {
                        throw new java.text.ParseException("Error parsing format="+messageFormats[i].toPattern()+" line "+lineCount+": "+fields[i], e.getErrorOffset());
                    }
                }
                System.out.println(Arrays.toString(values));
                
                if (firstLine) {
                    int j = 0;
                    if (metadata == null) {
                        String createTable = String.format("CREATE TABLE \"%s\".\"%s\" ( ", injector.keyspace, table);
                        for(String col : headers) {
                            if (j > 0) 
                                createTable+=",";
                            createTable += "\""+ col+"\" "+typeToCql(col, values[j]);
                            j++;
                        }
                        createTable += ", PRIMARY KEY ( (";
                        int x = 0;
                        for(String k : primaryKeyColumns.split(",")) {
                            if (x > 0)
                                createTable+=",";
                            createTable += "\""+ k + "\"";
                            x++;
                            if (x == partitionKeyLength -1)
                                createTable += ")";
                            
                        }
                        createTable += "))";
                        System.out.println(createTable);
                        injector.execute(createTable);
                    } else {
                        for(String col : headers) {
                            if (metadata.getColumn(col) == null) {
                                String schemaUpdate = String.format("ALTER TABLE \"%s\".\"%s\" ADD \"%s\" %s", injector.keyspace, table,col, typeToCql(col, values[j]));
                                System.out.println(schemaUpdate);
                                injector.execute(schemaUpdate);
                            }
                            j++;
                        }
                    }
                }
                
                if (firstLine) 
                    System.out.println(insert);
                put(injector.executeAsync(injector.getPreparedStatement(insert).bind(values)));
                firstLine = false;
                insertCount++;
            } 
        } else {
            throw new java.io.FileNotFoundException(f.getAbsolutePath());
        }
        System.out.println(insertCount+" rows inserted in "+this.keyspace+"."+this.table);
    }
   
    public String typeToCql(String col, Object object) throws IOException {
     // add a new column.
        String cqlType;
        switch (object.getClass().getName()) {
        case "java.lang.Integer" : return "int";
        case "java.lang.Long" : return "bigint";
        case "java.lang.BigInteger" : return "bigint";
        case "java.lang.Double" : return "double";
        case "java.lang.Float" : return "float";
        case "java.lang.String" : return "text";
        case "java.util.Date" : return "timestamp";
        default:
            throw new IOException("Unsupported type "+object.getClass().getName()+" for column "+col);
        }
    }
}
