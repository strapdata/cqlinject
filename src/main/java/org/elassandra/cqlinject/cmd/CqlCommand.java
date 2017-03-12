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

import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.elassandra.cqlinject.CqlClient;
import org.elassandra.cqlinject.CqlInject;
import org.elassandra.cqlinject.CqlInject.InjectCommand;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

@Command(name = "cql", description = "Insert the result of a CQL query into a cassandra table.")
public class CqlCommand extends InjectCommand {

    @Option(name = "-sh", description = "Source cluster contact point (default is destination host)")
    public String srcHost = host;

    @Option(name = { "-sp" }, description = "Source cluster contact port (default is destination port)")
    public int srcPort = port;

    @Option(name = "-sks", description = "Source cluster keyspace (default is destination keyspace)")
    public String srcKeyspace = keyspace;

    @Option(name = "-su", description = "Source cluster username (default is destination username)")
    public String srcUsername = username;

    @Option(name = "-spw", description = "Source cluster password (default is destination password)")
    public String srcPassword = password;

    @Option(type = OptionType.GLOBAL, name = {"--local-ranges"}, description = "Restrict the query to local token ranges (default is false)")
    public boolean localToken = false;
    
    @Option(name = "--writer-thread", description = "Number of writer thread (default = #core / 2)")
    public int writerThread = Runtime.getRuntime().availableProcessors() / 2;

    @Option(name = "--queue-depth", description = "Read queue depth (default = 8192)")
    public int queueDepth = 8192;

    @Arguments(description = "CQL query")
    public String query = null;
    
    CqlClient cqlClient = new CqlClient();

    public static int BTACH_SIZE = 1;

    LinkedBlockingQueue<Object[]> queue = new LinkedBlockingQueue<Object[]>(queueDepth);
    ExecutorService writeExecutor = Executors.newFixedThreadPool(writerThread);
    AtomicBoolean finished = new AtomicBoolean(false);
    AtomicLong lineCount = new AtomicLong(0L); // for concurrent updates

    public void createOrUpdateTable(ColumnDefinitions columns, CqlInject injector) throws Exception {
        TableMetadata targetTableMetadata = injector.getTableMetadata(table);
        if (targetTableMetadata == null) {
            StringBuilder sb = new StringBuilder().append("(");
            for (int j = 0; j < this.primaryKeyColumns.split(",").length; j++) {
                boolean found = false;

                for (Definition def : columns.asList()) {
                    System.out.println("query column alias=" + def.getName() + " type=" + def.getType().toString());
                    if (primaryKeyColumns.split(",")[j].equalsIgnoreCase(def.getName())) {
                        if (sb.length() > 1)
                            sb.append(", ");
                        sb.append("\"").append(def.getName()).append("\" ");
                        if (j + 1 == this.partitionKeyLength)
                            sb.append(")");
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new Exception("PK column " + primaryKeyColumns.split(",")[j] + " not found in query result");
                }
            }
            StringBuilder cols = new StringBuilder();
            for (Definition def : columns.asList()) {
                if (cols.length() > 0)
                    cols.append(",");
                cols.append("\"").append(def.getName()).append("\" ").append(def.getType().toString());
            }
            String q = String.format("CREATE TABLE %s ( %s, PRIMARY KEY (%s))", this.table, cols.toString(), sb.toString());
            injector.execute("Executing[" + injector.getSession().getCluster().getClusterName() + "]:" + q);
            injector.execute(q);
        } else {
            for (Definition def : columns.asList()) {
                if (targetTableMetadata.getColumn("\"" + def.getName() + "\"") == null) {
                    String q = String.format("ALTER TABLE %s ADD \"%s\" %s", this.table, def.getName(), def.getType());
                    injector.execute("Executing[" + injector.getSession().getCluster().getClusterName() + "]:" + q);
                    System.out.println(q);
                }
            }
        }
    }

    public void readSource(CqlInject injector) throws Exception {
        System.out.println("Executing[" + cqlClient.getSession().getCluster().getClusterName() + "]: " + query);
        ResultSet result = cqlClient.execute(query);

        createOrUpdateTable(result.getColumnDefinitions(), injector);

        StringBuffer statement = new StringBuffer(String.format("INSERT INTO \"%s\".\"%s\" (", this.keyspace, this.table));
        StringBuffer values = new StringBuffer();
        int i = 0;
        for (Definition def : result.getColumnDefinitions().asList()) {
            if (i > 0) {
                statement.append(',');
                values.append(',');
            }
            statement.append("\"").append(def.getName()).append("\"");
            values.append("?");
            i++;
        }
        statement.append(") VALUES (").append(values.toString()).append(")");
        System.out.println("Executing[" + cqlClient.getSession().getCluster().getClusterName() + "]: " + statement.toString());
        PreparedStatement destStatement = injector.getSession().prepare(statement.toString());

        System.out.println("Starting " + writerThread + " writer threads, queue size=" + queueDepth);
        for (int j = 0; j < writerThread; j++) {
            writeExecutor.execute(new CassandraWriter(destStatement, injector));
        }
        try {
            i = 0;
            for (Row row : result) {
                Object[] rowValues = new Object[result.getColumnDefinitions().asList().size()];
                int j = 0;
                TypeToken<?> tt, tk;
                for (Definition def : row.getColumnDefinitions().asList()) {
                    switch (def.getType().getName()) {
                    case ASCII:
                    case VARCHAR:
                    case TEXT:
                        rowValues[j++] = row.getString(def.getName());
                        break;
                    case INT:
                        rowValues[j++] = row.getInt(def.getName());
                        break;
                    case BIGINT:
                    case COUNTER:
                        rowValues[j++] = row.getLong(def.getName());
                        break;
                    case VARINT:
                        rowValues[j++] = row.getVarint(def.getName());
                        break;
                    case BLOB:
                        rowValues[j++] = row.getBytes(def.getName());
                        break;
                    case BOOLEAN:
                        rowValues[j++] = row.getBool(def.getName());
                        break;
                    case DECIMAL:
                        rowValues[j++] = row.getDecimal(def.getName());
                        break;
                    case FLOAT:
                        rowValues[j++] = row.getFloat(def.getName());
                        break;
                    case DOUBLE:
                        rowValues[j++] = row.getDouble(def.getName());
                        break;
                    case INET:
                        rowValues[j++] = row.getInet(def.getName());
                        break;
                    case UUID:
                    case TIMEUUID:
                        rowValues[j++] = row.getUUID(def.getName());
                        break;
                    case DATE:
                        rowValues[j++] = row.getDate(def.getName());
                        break;
                    case TIMESTAMP:
                        rowValues[j++] = row.getTimestamp(def.getName());
                        break;
                    case LIST:
                        rowValues[j++] = row.getList(def.getName(), CqlClient.typeToken(def.getType().getTypeArguments().get(0)));
                        break;
                    case SET:
                        rowValues[j++] = row.getSet(def.getName(), CqlClient.typeToken(def.getType().getTypeArguments().get(0)));
                        break;
                    case MAP:
                        rowValues[j++] = row.getMap(def.getName(), CqlClient.typeToken(def.getType().getTypeArguments().get(0)), CqlClient.typeToken(def.getType().getTypeArguments().get(1)));
                        break;
                    default:
                        System.out.println("Unsupported data type:" + def.getType().getName());
                        System.exit(1);
                        break;
                    }
                }
                queue.put(rowValues);
                i++;
            }
            System.out.println(i + " rows read from cluster [" + cqlClient.getSession().getCluster().getClusterName().toString() + "].");
            finished.set(true);
            writeExecutor.shutdown();
            writeExecutor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (Throwable e) {
            System.out.println("Error, row " + i + ":" + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        } finally {
            cqlClient.close();
        }
    }

    public class CassandraWriter implements Runnable {
        long insertCount = 0;
        PreparedStatement statement;
        CqlClient destClient;

        public CassandraWriter(PreparedStatement statement, CqlClient destClient) {
            this.statement = statement;
            this.destClient = destClient;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Object[] rowValues;
                    if (!finished.get()) {
                        // blocking read
                        rowValues = queue.poll(60, TimeUnit.SECONDS);
                    } else {
                        // non blocking read
                        rowValues = queue.poll();
                    }
                    if (rowValues == null)
                        break;
                    destClient.execute(statement.bind(rowValues));
                    insertCount++;
                }
                System.out.println(insertCount + " rows inserted in cluster [" + destClient.getSession().getCluster().getClusterName().toString() + "].");
            } catch (Exception e) {
                System.out.println("error:" + e.toString());
            }
            lineCount.addAndGet(insertCount);
        }
    }

    @Override
    protected void execute(CqlInject injector) throws Exception {
        try {
            injector.connect();
            cqlClient.connect(new String[] { srcHost }, srcPort, srcKeyspace, srcUsername, srcPassword);
            long start = System.currentTimeMillis();
            readSource(injector);
            long duration = System.currentTimeMillis() - start;
            System.out.println(lineCount.get() + " insert in " + duration + "ms throughput=" + ((double) lineCount.get() * 1000 / duration) + " row/s");
            System.exit(0);
        } finally {
            if (cqlClient != null)
                cqlClient.close();
        }
    }

}
