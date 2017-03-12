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
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.elassandra.cqlinject.CqlInject;
import org.elassandra.cqlinject.CqlInject.InjectCommand;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Iterables;

@Command(name = "json", description = "Insert an Elasticsearch bulk JSON file in a cassandra table")
public class JsonCommand extends InjectCommand {

    @Option(name = {"--index"}, description = "Elasticsearch index name (Default is destination keyspace name)")
    public String index = null;
    
    @Option(name = {"--type"}, description = "Elasticsearch type name (Default is destination table name)")
    public String type = null;
    
    @Option(name = {"--fields"}, description = "Elasticsearch fields to index (Default all)")
    public String fields = null;
    
    @Arguments(description = "Elasticsearch bulk input file")
    public String inputFile = null;
    
    
    Client esClient;
    
    @Override
    public void execute(CqlInject injector) throws IOException, TimeoutException {
        String defaultIndex = (index==null) ? this.keyspace : index;
        String defaultType = (type==null) ? this.table : type;
        String[] defaultFields = fields != null ? Strings.commaDelimitedListToStringArray(fields) : null;
        
        injector.keyspace = defaultIndex;
        injector.connect();
        
        Settings settings = Settings.settingsBuilder().put("cluster.name", injector.getClusterName()).build();
        this.esClient = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(this.host), 9300));
        
        boolean allowExplicitIndex = true;
        int lineCount = 0;
        int insertCount=0;
        int deleteCount=0;
        int updateCount=0;
        int errorCount=0;
        
        File f = new File(inputFile);
        if (f.exists()) {
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String line;
            while ( (line = br.readLine()) != null) {
                lineCount++;
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(line);
                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                assert token == XContentParser.Token.START_OBJECT;
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String action = parser.currentName();

                String index = defaultIndex;
                String type = defaultType;
                String id = null;
                String routing = null;
                String parent = null;
                String[] fields = defaultFields;
                String timestamp = null;
                Long ttl = null;
                String opType = null;
                long version = Versions.MATCH_ANY;
                VersionType versionType = VersionType.INTERNAL;
                int retryOnConflict = 0;

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters
                token = parser.nextToken();

                if (token == XContentParser.Token.START_OBJECT) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("_index".equals(currentFieldName)) {
                                if (!allowExplicitIndex) {
                                    throw new IllegalArgumentException("explicit index in bulk is not allowed");
                                }
                                index = parser.text();
                            } else if ("_type".equals(currentFieldName)) {
                                type = parser.text();
                            } else if ("_id".equals(currentFieldName)) {
                                id = parser.text();
                            } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                                routing = parser.text();
                            } else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
                                parent = parser.text();
                            } else if ("_timestamp".equals(currentFieldName) || "timestamp".equals(currentFieldName)) {
                                timestamp = parser.text();
                            } else if ("_ttl".equals(currentFieldName) || "ttl".equals(currentFieldName)) {
                                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                                    ttl = TimeValue.parseTimeValue(parser.text(), null, currentFieldName).millis();
                                } else {
                                    ttl = parser.longValue();
                                }
                            } else if ("op_type".equals(currentFieldName) || "opType".equals(currentFieldName)) {
                                opType = parser.text();
                            } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                                version = parser.longValue();
                            } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                                versionType = VersionType.fromString(parser.text());
                            } else if ("_retry_on_conflict".equals(currentFieldName) || "_retryOnConflict".equals(currentFieldName)) {
                                retryOnConflict = parser.intValue();
                            } else if ("fields".equals(currentFieldName)) {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains a simple value for parameter [fields] while a list is expected");
                            } else {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains an unknown parameter [" + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if ("fields".equals(currentFieldName)) {
                                List<Object> values = parser.list();
                                fields = values.toArray(new String[values.size()]);
                            } else {
                                throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                            }
                        } else if (token != XContentParser.Token.VALUE_NULL) {
                            throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                        }
                    }
                } else if (token != XContentParser.Token.END_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected " + XContentParser.Token.START_OBJECT
                            + " or " + XContentParser.Token.END_OBJECT + " but found [" + token + "]");
                }

                if ("delete".equals(action)) {
                    //add(new DeleteRequest(index, type, id).routing(routing).parent(parent).version(version).versionType(versionType), payload);
                    try {
                        delete(injector, index,type,id);
                        deleteCount++;
                    } catch (Exception e) {
                        errorCount++;
                        System.out.println("error:"+e.toString());
                    }
                } else {
                    // read the next json object
                    XContentParser parser2 = XContentFactory.xContent(XContentType.JSON).createParser(br.readLine());
                    Map<String, Object> doc = parser2.map();
                    lineCount++;

                    // order is important, we set parent after routing, so routing will be set to parent if not set explicitly
                    // we use internalAdd so we don't fork here, this allows us not to copy over the big byte array to small chunks
                    // of index request.
                    if (action.equals("index")){
                        try {
                            insert(injector, index,type,id, false, timestamp, ttl, doc);
                            insertCount++;
                        } catch (Exception e) {
                            errorCount++;
                            System.out.println("error:"+e.toString());
                        }
                    } else if (action.equals("create")){
                        try {
                            insert(injector, index,type,id,true, timestamp,ttl,doc);
                            insertCount++;
                        } catch (Exception e) {
                            errorCount++;
                            System.out.println("error:"+e.toString());
                        }
                    } else if (action.equals("update")){
                        try {
                            insert(injector, index,type,id,false, timestamp,ttl,doc);
                            updateCount++;
                        } catch (Exception e) {
                            errorCount++;
                            System.out.println("error:"+e.toString());
                        }
                    } else if (action.equals("delete")){
                    try {
                        delete(injector, index,type,id);
                        deleteCount++;
                    } catch (Exception e) {
                        errorCount++;
                        System.out.println("error:"+e.toString());
                    }
                }else {
                        throw new IllegalArgumentException("Malformed action line [" + lineCount + "]");
                    }
                }
            } 
        } else {
            throw new java.io.FileNotFoundException(f.getAbsolutePath());
        }
        System.out.println(lineCount+" rows, insert/update/delete = "+insertCount+"/"+updateCount+"/"+deleteCount);
    }
    
    
    
    public void insert(final CqlInject injector, final String index, final String type, final String id, final boolean create, final String timestamp, final Long ttl, Map<String,Object> doc) throws IOException, TimeoutException, InterruptedException {
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        DocPrimaryKey pk = parseElasticId(injector, index, type, id);
        for(int i=0; i < pk.names.length; i++) {
            doc.put(pk.names[i], pk.values[i]);
        }
        jsonBuilder.map(doc);
        String jsonDoc = jsonBuilder.string();
        //System.out.println("insert "+jsonDoc);
        PreparedStatement insertStatement = injector.getPreparedStatement(String.format("INSERT INTO \"%s\".\"%s\" JSON ? ", index, type));
        ResultSetFuture result = injector.executeAsync(insertStatement.bind(jsonDoc));
        put(result);
    }

    
    public void delete(final CqlInject injector, final String index, final String type, final String id) throws TimeoutException, IOException, InterruptedException {
        DocPrimaryKey pk = parseElasticId(injector, index, type, id);
        PreparedStatement deleteStatement = injector.getPreparedStatement(String.format("DELETE FROM  \"%s\".\"%s\" WHERE %s", index, type, pk.pkWhere()));
        ResultSetFuture result = injector.executeAsync(deleteStatement.bind(pk.values));
        put(result);
    }
    
    public static class DocPrimaryKey {
        public String[] names;
        public Object[] values;
        
        public DocPrimaryKey(String[] names, Object[] values) {
            this.names = names;
            this.values = values;
        }
        
        public String pkWhere() {
            StringBuilder pkWhereBuilder = new StringBuilder();
            
            for (String col : names) {
                if (pkWhereBuilder.length() > 0) {
                    pkWhereBuilder.append(" AND ");
                }
                pkWhereBuilder.append('\"').append(col).append("\" = ?");
            }
            return pkWhereBuilder.toString();
        }
    }
    
    public static final org.codehaus.jackson.map.ObjectMapper jsonMapper = new org.codehaus.jackson.map.ObjectMapper();

    public DocPrimaryKey parseElasticId(final CqlInject injector, final String ksName, final String cfName, final String id) throws IOException {
        // TODO : Manage case where index <> keyspace
        TableMetadata metadata = injector.getTableMetadata(ksName, cfName);
        List<ColumnMetadata> partitionColumns = metadata.getPartitionKey();
        List<ColumnMetadata> clusteringColumns = metadata.getClusteringColumns();
        if (id.startsWith("[") && id.endsWith("]")) {
            // _id is JSON array of values.
            Object[] elements = jsonMapper.readValue(id, Object[].class);
            Object[] values = new Object[elements.length];
            String[] names = new String[elements.length];
            int i=0;
            for(ColumnMetadata cd : Iterables.concat(partitionColumns, clusteringColumns)) {
                if (i > elements.length) break;
                names[i] = cd.getName();
                values[i] = elements[i];
                if (!elements[i].getClass().isAssignableFrom(cd.getClass())) {
                   System.out.println("Warning: type for column "+cd.getName()+" does not match the json object in "+id);
                } 
                i++;
            }
            return new DocPrimaryKey(names, values) ;
        } else {
            // _id is a single columns.
            return new DocPrimaryKey( new String[] { partitionColumns.get(0).getName() } , new Object[] { id } );
        }
    }
}
