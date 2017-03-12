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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elassandra.cqlinject.CqlInject;
import org.elassandra.cqlinject.CqlInject.InjectCommand;

@Command(name = "lastfm", description = "Insert lastfm-1K data set")
public class LastfmCommand extends InjectCommand {

    public String separator = "\t";

    public String language = "en";
    
    public String country = "EN";
    
    @Arguments(description = "profile and playlist input files")
    public List<String> inputFiles = null;
    
    public Map<String,String[]> userProfiles = new HashMap<String,String[]>();
    
    public void loadProfiles(String filename) throws Exception {
        System.out.println("Loading file "+filename);
        File f = new File(filename);
        if (f.exists()) {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line;
            while ( (line = br.readLine()) != null) {
                if (line.startsWith("#")) 
                    continue;
                
                String[] fields = line.split(separator);
                userProfiles.put(fields[0],fields);
            }
        }
        System.out.println(userProfiles.size()+" profiles loaded.");
    }
    
    
    @Override
    public void execute(CqlInject injector) throws Exception {
        injector.connect();
        Locale.setDefault(new Locale(language, country));
        
        int lineCount = 0;
        int insertCount=0;
        
        loadProfiles(inputFiles.get(0));
        
        System.out.println("Reading file "+inputFiles.get(1)+" separator='"+separator+"'");
        
        String[] cols = new String[] { "user_id", "datetime", "artist_id",  "artist_name", "song_id", "song", "gender",  "age", "country", "registred" };
        String[] types = new String[] { "text", "timestamp", "text",  "text", "text", "text", "text",  "int", "text", "timestamp" };
        
        String createTable = String.format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" ( ", injector.keyspace, table);
        String insert = String.format("INSERT INTO \"%s\".\"%s\" (", injector.keyspace, table);
        String qmarks = new String();
        for(int i=0; i < cols.length; i++) {
            if (i > 0) {
                createTable += ',';
                insert += ',';
                qmarks += ',';
            }
            createTable += cols[i] + " " +types[i];
            insert += cols[i];
            qmarks += "?";
        }
        createTable += ", PRIMARY KEY ((user_id,datetime)))";
        insert += ") VALUES ("+qmarks+")";
        
        System.out.println(createTable);
        injector.execute(createTable);
        
        SimpleDateFormat df1 = new SimpleDateFormat("MMM dd, yyyy");
        SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        boolean firstLine = true;
        String line;
        File f = new File(inputFiles.get(1));
        if (f.exists()) {
            BufferedReader br = new BufferedReader(new FileReader(inputFiles.get(1)));
            while ( (line = br.readLine()) != null) {
                lineCount++;
                if (line.startsWith("#")) 
                    continue;
                
                String[] fields = line.split(separator);
                Object[] values = new Object[cols.length];
                String[] profile = userProfiles.get(fields[0]);
                System.arraycopy(fields, 0, values, 0, 6);
                values[1] = df2.parse(fields[1]);
                if (profile != null && profile.length >= 5) {
                    values[6] = profile[1]; // gender
                    try {
                        values[7] = (profile[2] == null) ? null : Integer.parseInt(profile[2]); // age
                    } catch(NumberFormatException e) {
                        //System.out.println("NumberFormatException line "+lineCount);
                        values[7] = null;
                    }
                    values[8] = profile[3]; // country
                    try {
                        values[9] = (profile[4] == null) ? null : df1.parse(profile[4]);// registred
                    } catch(ParseException e) {
                        //System.out.println("ParseException line "+lineCount);
                        values[9] = null;
                    }
                }
                
                if (firstLine) {
                    System.out.println(insert);
                    System.out.println(Arrays.toString(values));
                }
                
                put(injector.executeAsync(injector.getPreparedStatement(insert).bind(values)));
                insertCount++;
                firstLine = false;
            } 
        } else {
            throw new java.io.FileNotFoundException(f.getAbsolutePath());
        }
        System.out.println(insertCount+" rows inserted in "+this.keyspace+"."+this.table);
    }
  
}
