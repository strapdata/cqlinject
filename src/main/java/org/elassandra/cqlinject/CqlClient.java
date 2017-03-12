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
package org.elassandra.cqlinject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

public class CqlClient {
    protected Cluster cluster = null;
    protected Session session = null;
    protected LoadBalancingPolicy lbPolicy = null;

    public CqlClient() {

    }

    public void connect(String node) {
        connect(new String[] { node }, null);
    }

    public void connect(String[] nodes, String keyspace) {
        connect(nodes, 9042, keyspace, null, null);
    }

    public void connect(String[] nodes, int port, String keyspace) {
        connect(nodes, port, keyspace, null, null);
    }

    public void connect(String[] nodes, int port, String keyspace, String login, String password) {
        Cluster.Builder clusterBuilder = Cluster.builder();

        clusterBuilder.withPort(port);
        clusterBuilder.withProtocolVersion(ProtocolVersion.V3); // compatible cassandra 2.1, 
        // see https://docs.datastax.com/en/developer/driver-matrix/doc/common/driverMatrix.html
        // see https://github.com/datastax/java-driver/tree/3.0/manual/native_protocol

        if ((login != null) && (password != null))
            clusterBuilder.withCredentials(login, password);

        if (lbPolicy != null) {
            clusterBuilder.withLoadBalancingPolicy(lbPolicy);
        } else if (System.getProperty("localDc") != null) {
            DCAwareRoundRobinPolicy policy = DCAwareRoundRobinPolicy.builder().withLocalDc(System.getProperty("localDc")).withUsedHostsPerRemoteDc(2).build();
            clusterBuilder.withLoadBalancingPolicy(new TokenAwarePolicy(new TokenAwarePolicy(policy)));
        }
        clusterBuilder.withRetryPolicy(new CustomRetryPolicy(3, 3, 3));
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, Integer.parseInt(System.getProperty("localMaxCon", "8")));
        clusterBuilder.withPoolingOptions(poolingOptions);
        clusterBuilder.withReconnectionPolicy(new ConstantReconnectionPolicy(1000));

        if (login != null) {
            clusterBuilder.withAuthProvider(new PlainTextAuthProvider(login, password));
        }

        for (String n : nodes)
            clusterBuilder.addContactPoint(n);
        cluster = clusterBuilder.build();

        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }

        session = (keyspace == null) ? cluster.connect() : cluster.connect(keyspace);
    }

    public void close() {
        if (cluster != null)
            cluster.close();
        session = null;
    }

    public KeyspaceMetadata getKeyspaceMetadata() {
        return getKeyspaceMetadata(session.getLoggedKeyspace());
    }

    public KeyspaceMetadata getKeyspaceMetadata(String keyspace) {
        return session.getCluster().getMetadata().getKeyspace(keyspace);
    }

    public String getClusterName() {
        return session.getCluster().getMetadata().getClusterName();
    }

    public TableMetadata getTableMetadata(String tableName) {
        return getTableMetadata(session.getLoggedKeyspace(), tableName);
    }

    public TableMetadata getTableMetadata(String keyspace, String tableName) {
        return getKeyspaceMetadata(keyspace).getTable(tableName);
    }

    public Session getSession() {
        return session;
    }

    public LoadBalancingPolicy getLbPolicy() {
        return lbPolicy;
    }

    public void setLbPolicy(LoadBalancingPolicy lbPolicy) {
        this.lbPolicy = lbPolicy;
    }

    protected void finalize() throws Throwable {
        try {
            close(); // close open files
        } finally {
            super.finalize();
        }
    }

    private Map<String, PreparedStatement> statementCache = new HashMap<String, PreparedStatement>();

    public ResultSet execute(String query, Object... args) {
        return session.execute(getPreparedStatement(query).bind(args));
    }

    public ResultSet execute(Statement stmt) {
        return session.execute(stmt);
    }

    public ResultSetFuture executeAsync(String query, Object... args) {
        return session.executeAsync(getPreparedStatement(query).bind(args));
    }

    public ResultSetFuture executeAsync(Statement stmt) {
        return session.executeAsync(stmt);
    }

    public PreparedStatement getPreparedStatement(String query) {
        PreparedStatement stmt = statementCache.get(query);
        if (stmt == null) {
            synchronized (statementCache) {
                stmt = statementCache.get(query);
                if (stmt == null) {
                    stmt = session.prepare(query);
                    statementCache.put(query, stmt);
                }
            }
        }
        return stmt;
    }

	static <K, V> TypeToken<Map<K, V>> mapToken(TypeToken<K> keyToken, TypeToken<V> valueToken) {
        return new TypeToken<Map<K, V>>() {}
          .where(new TypeParameter<K>() {}, keyToken)
          .where(new TypeParameter<V>() {}, valueToken);
    }
  
    static <V> TypeToken<Set<V>> setToken(TypeToken<V> valueToken) {
        return new TypeToken<Set<V>>() {
        }.where(new TypeParameter<V>() {
        }, valueToken);
    }

    static <V> TypeToken<List<V>> listToken(TypeToken<V> valueToken) {
        return new TypeToken<List<V>>() {
        }.where(new TypeParameter<V>() {
        }, valueToken);
    }

    public static TypeToken typeToken(DataType type) {
        switch (type.getName()) {
        case ASCII:
        case VARCHAR:
        case TEXT:
            return TypeToken.of(String.class);
        case INT:
            return TypeToken.of(Integer.class);
        case BIGINT:
        case COUNTER:
            return TypeToken.of(Long.class);
        case VARINT:
            return TypeToken.of(BigInteger.class);
        case BLOB:
            return TypeToken.of(ByteBuffer.class);
        case BOOLEAN:
            return TypeToken.of(Boolean.class);
        case DECIMAL:
            return TypeToken.of(BigDecimal.class);
        case FLOAT:
            return TypeToken.of(Float.class);
        case DOUBLE:
            return TypeToken.of(Double.class);
        case INET:
            return TypeToken.of(InetAddress.class);
        case UUID:
        case TIMEUUID:
            return TypeToken.of(UUID.class);
        case DATE:
            return TypeToken.of(LocalDate.class);
        case TIMESTAMP:
            return TypeToken.of(Date.class);
        case LIST:
            return listToken(typeToken(type.getTypeArguments().get(0)));
        case SET:
            return setToken(typeToken(type.getTypeArguments().get(0)));
        case MAP:
            return mapToken(typeToken(type.getTypeArguments().get(0)), typeToken(type.getTypeArguments().get(1)));
        default:
            System.out.println("Unsupported data type:" + type.getName());
        }
        return null;
    }

    public static class CustomRetryPolicy implements RetryPolicy {

        private final int readAttempts;
        private final int writeAttempts;
        private final int unavailableAttempts;

        public CustomRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
            this.readAttempts = readAttempts;
            this.writeAttempts = writeAttempts;
            this.unavailableAttempts = unavailableAttempts;
        }

        @Override
        public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataReceived, int rTime) {
            if (dataReceived) {
                return RetryDecision.ignore();
            } else if (rTime < readAttempts) {
                return RetryDecision.retry(cl);
            } else {
                return RetryDecision.rethrow();
            }
        }

        @Override
        public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
            if (wTime < writeAttempts) {
                return RetryDecision.retry(cl);
            }
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
            if (uTime < unavailableAttempts) {
                return RetryDecision.tryNextHost(cl);
            }
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onRequestError(Statement paramStatement, ConsistencyLevel paramConsistencyLevel, DriverException paramDriverException, int paramInt) {
            if (paramDriverException instanceof ConnectionException) {
                return RetryDecision.retry(paramConsistencyLevel);
            }
            return RetryDecision.rethrow();
        }

        public void init(Cluster cluster) {
        }

        public void close() {
        }

    }
} 