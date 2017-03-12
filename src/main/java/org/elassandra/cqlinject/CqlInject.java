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

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.elassandra.cqlinject.cmd.CqlCommand;
import org.elassandra.cqlinject.cmd.CsvCommand;
import org.elassandra.cqlinject.cmd.JdbcCommand;
import org.elassandra.cqlinject.cmd.JsonCommand;
import org.elassandra.cqlinject.cmd.LastfmCommand;

import com.datastax.driver.core.ResultSetFuture;

/**
 * Execute a query and write the result into cassandra (create and update the destination table if not exists). 
 * @author vroyer@vroyer.org
 *
 */
public class CqlInject extends CqlClient {
    public String host;
    public int port;
    public String keyspace;
    public String username;
    public String password;
    
    public CqlInject(String host, int port, String keyspace, String username, String password) {
        super();
        this.host = host;
        this.port = port;
        this.keyspace = keyspace;
        this.username = username;
        this.password = password;
    }
    
	public static void main(String[] args) {
        CliBuilder<Runnable> builder = Cli.<Runnable>builder("cqlinject")
                .withDescription("Cassandra injector")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, JdbcCommand.class, JsonCommand.class, CqlCommand.class, CsvCommand.class, LastfmCommand.class);

        Cli<Runnable> gitParser = builder.build();
        gitParser.parse(args).run();
    }
	
	public void connect() {
	    if (username == null) {
	        connect(new String[] { host }, port, keyspace);
	    } else {
	        connect(new String[] { host }, port, keyspace, username, password);
	    }
	}
	
    private static void badUse(Exception e) {
        System.out.println("cqlinject: " + e.toString());
        System.out.println("See 'cqlinject help' or 'cqlinject help <command>'.");
    }

    private static void err(Throwable e) {
        System.err.println("error: " + e.toString());
        System.err.println("-- StackTrace --");
        System.err.println(getStackTraceAsString(e));
    }

    public static abstract class InjectCommand implements Runnable
    {
        
        @Option(type = OptionType.GLOBAL, name = {"-h" }, description = "Cassandra hostname or ip address (default = localhost)")
        public String host = "127.0.0.1";

        @Option(type = OptionType.GLOBAL, name = {"-p" }, description = "Cassandra RPC port number (default = 9042)")
        public int port = 9042;

        @Option(type = OptionType.GLOBAL, name = {"-u" }, description = "Cassandra username")
        public String username = null;

        @Option(type = OptionType.GLOBAL, name = {"-pw" }, description = "Cassandra password")
        public String password = null;

        @Option(type = OptionType.GLOBAL, name = {"-pwf" }, description = "Path to the password file")
        public String passwordFilePath = null;

        @Option(type = OptionType.GLOBAL, name = {"-ks" }, description = "Cassandra destination keyspace")
        public String keyspace = null;
        
        @Option(type = OptionType.GLOBAL, name = {"-cf"}, description = "Cassandra destination table")
        public String table = null;
        
        @Option(type = OptionType.GLOBAL, name = {"-pk"}, description = "Comma separated primary key columns of the destination table")
        public String primaryKeyColumns;
        
        @Option(type = OptionType.GLOBAL, name = {"-ptlen"}, description = "Cassandra destination table partition key length (default = 1)")
        public int partitionKeyLength = 1;
        
        @Option(type = OptionType.GLOBAL, name = {"--window"}, description = "Asynchronous requests window, default = 8192")
        public int asyncWindow = 8192;
        
        private volatile int produced = 0;
        private volatile int consumed = 0;
        private volatile boolean finished = false;
        private BlockingQueue<ResultSetFuture> queue;
        
        public class Consumer implements Runnable{
            
            private BlockingQueue<ResultSetFuture> queue;
                 
            public Consumer(BlockingQueue<ResultSetFuture> q){
                this.queue=q;
            }
         
            @Override
            public void run() {
                try{
                    
                    while ( !finished || consumed < produced ) {
                        try {
                            ResultSetFuture future = queue.take();
                            future.getUninterruptibly(10, TimeUnit.SECONDS);
                            consumed++;
                        } catch (TimeoutException e) {
                            System.out.println("Request timeout.");
                            consumed++;
                        }
                    }
                }catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        public void put(ResultSetFuture future) throws InterruptedException {
            produced++;
            queue.put(future);
        }
        
        public void run()
        {
            if (isNotEmpty(username)) {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            queue = new ArrayBlockingQueue<ResultSetFuture>(asyncWindow);
            Consumer consumer = new Consumer(queue);
            Thread consumerThread = new Thread(consumer);
            consumerThread.start();
            CqlInject injector = null;
            try {
                injector = new CqlInject(host, port,keyspace, username, password);
                long startTime = System.currentTimeMillis();
                execute(injector);
                finished = true;
                consumerThread.join();
                long endTime = System.currentTimeMillis();
                System.out.println("Elapse time = "+(endTime-startTime)+"ms");
            } catch (Throwable e) {
                err(e);
            } finally {
                if (injector != null) {
                    injector.close();
                }
            }

        }

        
        private String readUserPasswordFromFile(String username, String passwordFilePath) {
            String password = EMPTY;

            File passwordFile = new File(passwordFilePath);
            try (Scanner scanner = new Scanner(passwordFile).useDelimiter("\\s+"))
            {
                while (scanner.hasNextLine())
                {
                    if (scanner.hasNext())
                    {
                        String jmxRole = scanner.next();
                        if (jmxRole.equals(username) && scanner.hasNext())
                        {
                            password = scanner.next();
                            break;
                        }
                    }
                    scanner.nextLine();
                }
            } catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }

            return password;
        }

        private String promptAndReadPassword()
        {
            String password = EMPTY;

            Console console = System.console();
            if (console != null)
                password = String.valueOf(console.readPassword("Password:"));

            return password;
        }

        protected abstract void execute(CqlInject injector) throws Exception;

    }
}
