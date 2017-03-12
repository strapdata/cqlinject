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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.elassandra.cqlinject.CqlInject;
import org.elassandra.cqlinject.CqlInject.InjectCommand;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.TableMetadata;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "jdbc", description = "Insert the result of a JDBC query in a cassandra table.")
public class JdbcCommand extends InjectCommand {
    
    @Option(name = "--jdbc.url", required = true, description = "JDBC url")
    public String jdbcUrl = null;
    
    @Option(name = "--jdbc.user", description = "JDBC username")
    public String jdbcUsername = null;
    
    @Option(name = "--jdbc.password", description = "JDBC password")
    public String jdbcPassword = null;
    
    @Option(name = "--jdbc.driver", description = "JDBC driver class name (overwrite jdbc-drivers.properties)")
    public String jdbcDriverClassName = null;
    
    @Arguments(description = "JDBC query")
    public String jdbcQuery = null;
    
    Connection jdbcClient;
    
    public String getDriverClassName(String url) throws SQLException, IOException {
        Properties props = new Properties();
        props.load(JdbcCommand.class.getResourceAsStream("jdbc-drivers.properties"));
        String[] fields = url.split(":");
        String driverClassName = props.getProperty(fields[1]);
        if (driverClassName == null) {
            throw new SQLException("No driver found for url "+url);
        }
        return driverClassName;
    }
    
    public void initJDBC() throws ClassNotFoundException, SQLException, IOException {
        if (jdbcDriverClassName == null) {
            Class.forName(getDriverClassName(jdbcUrl)); 
        } else {
            Class.forName(jdbcDriverClassName);
        }
        if (jdbcUsername != null) {
            jdbcClient = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword); 
        } else {
            jdbcClient = DriverManager.getConnection(jdbcUrl); 
        }
        System.out.println("JDBC Connected.");
    }
    
    public String jdbcToCqlType(int jdbcType, String alias) {
        switch(jdbcType) {
        case java.sql.Types.BIT : 
        case java.sql.Types.BOOLEAN : return "boolean";
        
        case java.sql.Types.INTEGER : return "int";
        case java.sql.Types.TINYINT : return "int";
        case java.sql.Types.SMALLINT : return "int";
        case java.sql.Types.BIGINT : return "bigint";
        case java.sql.Types.DOUBLE : return "double";
        case java.sql.Types.NUMERIC : return "decimal";
        case java.sql.Types.FLOAT : return "float";
        
        case java.sql.Types.BINARY : return "blob"; 
        case java.sql.Types.BLOB : return "blob";
        
        case java.sql.Types.CHAR : return "text"; 
        case java.sql.Types.VARCHAR : return "text"; 
        case java.sql.Types.NVARCHAR : return "text"; 
        case java.sql.Types.LONGVARCHAR : return "text";
        case java.sql.Types.LONGNVARCHAR : return "text";
        
        case -155: // MS DateTimeOffset in ms
        case java.sql.Types.TIMESTAMP :
        case java.sql.Types.DATE : return "timestamp";
        
        default:
            System.out.println("Warning: Unsupported type "+jdbcType+" for "+alias);
            return "void";
        }
    }
   
    BitSet textToUUID = null;
    
    public void createOrUpdateTable(ResultSetMetaData metadata, CqlInject injector) throws SQLException {
        TableMetadata targetTableMetadata = injector.getTableMetadata(table);
        String[] primaryKeyColumnsArray = primaryKeyColumns.split(",");
        if (targetTableMetadata == null) {
            StringBuilder sb = new StringBuilder().append("(");
            for(int j=0; j < primaryKeyColumnsArray.length; j++) {
                boolean found = false;
                
                for(int i=0; i<metadata.getColumnCount(); i++) {
                    System.out.println("query column alias="+metadata.getColumnName(i+1)+" type="+metadata.getColumnType(i+1)+" => "+jdbcToCqlType(metadata.getColumnType(i+1), metadata.getColumnName(i+1)));
                    if (primaryKeyColumnsArray[j].equalsIgnoreCase(metadata.getColumnName(i+1))) {
                        if (sb.length() > 1) sb.append(", ");
                        sb.append("\"").append(metadata.getColumnName(i+1).toLowerCase()).append("\" ");
                        if (j+1 == this.partitionKeyLength) sb.append(")");
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new SQLException("PK column "+primaryKeyColumnsArray[j]+" not found in query result");
                }
            }
            System.out.println();
            StringBuilder cols = new StringBuilder();
            for(int i=0; i<metadata.getColumnCount(); i++) {
                if (cols.length()>0) cols.append(",");
                cols.append("\"")
                    .append(metadata.getColumnName(i+1).toLowerCase())
                    .append("\" ")
                    .append(jdbcToCqlType(metadata.getColumnType(i+1), metadata.getColumnName(i+1).toLowerCase()));
                
                if (metadata.getColumnName(i+1).toLowerCase().matches("(uuid|timeuuid)")) {
                    if (textToUUID == null)
                        textToUUID = new BitSet(metadata.getColumnCount());
                    textToUUID.set(i);
                }
            }
            String q = String.format("CREATE TABLE %s ( %s, PRIMARY KEY (%s))",this.table, cols.toString(), sb.toString());
            System.out.println(q);
            injector.execute( q );
        } else {
            for(int i=0; i<metadata.getColumnCount(); i++) {
                if (targetTableMetadata.getColumn("\""+metadata.getColumnName(i+1).toLowerCase()+"\"") == null) {
                    String q = String.format("ALTER TABLE %s ADD \"%s\" %s",
                            this.table, metadata.getColumnName(i+1).toLowerCase(), 
                            jdbcToCqlType(metadata.getColumnType(i+1), metadata.getColumnName(i+1).toLowerCase()));
                    injector.execute( q );
                    System.out.println(q);
                }
                
                if (targetTableMetadata.getColumn("\""+metadata.getColumnName(i+1).toLowerCase()+"\"").getType().getName().name().toLowerCase().matches("(uuid|timeuuid)")) {
                    if (textToUUID == null)
                        textToUUID = new BitSet(metadata.getColumnCount());
                    textToUUID.set(i);
                }
            }
        }
    }

    
    @Override
    public void execute(CqlInject injector) throws SQLException, TimeoutException, ClassNotFoundException, IOException, InterruptedException {
        injector.connect();
        // Create and execute an SQL statement that returns some data.  
        initJDBC();
        Statement stmt = jdbcClient.createStatement();
        System.out.println("JDBC query="+this.jdbcQuery);
        ResultSet rs = stmt.executeQuery(jdbcQuery);
    
        // Iterate through the data in the result set and display it.  
        boolean first = true;
        Object[] values = null;
        PreparedStatement cqlStatement = null;
        int j=0;
        
        while (rs.next())  {  
           ResultSetMetaData metadata = rs.getMetaData();
           if (first) {
               createOrUpdateTable(metadata, injector);
               values = new Object[metadata.getColumnCount()];
               StringBuilder colNames = new StringBuilder();
               StringBuilder qmarks = new StringBuilder();
               for(int i=0; i<metadata.getColumnCount(); i++) {
                    if (colNames.length()>0) {
                        colNames.append(",");
                        qmarks.append(",");
                    }
                    colNames.append("\"").append(metadata.getColumnName(i+1).toLowerCase()).append("\"");
                    qmarks.append("?");
               }
               String q = String.format("INSERT INTO %s (%s) VALUES (%s)",this.table,colNames,qmarks);
               System.out.println(q);
               cqlStatement = injector.getPreparedStatement(q);
           }
           for(int i=0; i<metadata.getColumnCount(); i++) {
               switch(metadata.getColumnType(i+1)) {
                case java.sql.Types.BIT : 
                case java.sql.Types.BOOLEAN : values[i] = (boolean)rs.getBoolean(i+1); break;
                case java.sql.Types.TINYINT : 
                case java.sql.Types.INTEGER : 
                case java.sql.Types.SMALLINT : values[i] = (int)rs.getInt(i+1); break;
                case java.sql.Types.BIGINT : values[i] = (long)rs.getInt(i+1); break;
                case java.sql.Types.DOUBLE : values[i] = rs.getDouble(i+1); break;
                case java.sql.Types.NUMERIC : 
                case java.sql.Types.DECIMAL : values[i] = rs.getBigDecimal(i+1); break;
                case java.sql.Types.FLOAT : values[i] = (int)rs.getFloat(i+1); break;
                case java.sql.Types.BINARY : values[i] = ByteBuffer.wrap(rs.getBytes(i+1)); break;
                case java.sql.Types.BLOB : values[i] = rs.getBlob(i+1); break;
                case java.sql.Types.CHAR : 
                case java.sql.Types.VARCHAR : 
                case java.sql.Types.LONGVARCHAR : 
                case java.sql.Types.LONGNVARCHAR : 
                case java.sql.Types.NVARCHAR : 
                    values[i] = rs.getString(i+1);
                    if (textToUUID != null && textToUUID.get(i))
                        values[i] = UUID.fromString((String)values[i]);
                    break;
                case -155 : // MS DateTimeOffset ms
                case java.sql.Types.TIMESTAMP : values[i] = new Date(rs.getTimestamp(i+1).getTime()); break;
                case java.sql.Types.DATE : values[i] = rs.getDate(i+1); break;

                default:
                    if (first) System.out.println("WARING: Cannot get value for "+metadata.getColumnName(i+1)+" JDBC type="+metadata.getColumnType(i+1));
                    values[i] = null;
               }
               //System.out.println(values[i].getClass().getName()+"="+values[i]);
           }
           if (first) {
               first = false;
           }
           //System.out.println(Arrays.toString(values));
           ResultSetFuture future = injector.executeAsync(cqlStatement.bind(values));
           put(future);
           j++;
        }
        System.out.println(j+" rows inserted in "+this.keyspace+"."+this.table);
    }

}
