package flink.sql.datastream;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Simple example that shows how the Batch SQL API is used in Java.
 * This example shows how to: - Convert DataSets to Tables - Register a Table
 * under a name - Run a SQL query on the registered Table
 */
public class WordCountSQLToStream {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public WordCountSQLToStream() {
    }

    public static void main(String[] args) throws Exception {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Order> input = env.fromElements(new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4), new Order(3L, "rubber", 2));

        // register the DataStream as table "OrderA"
        tEnv.registerDataStream("OrderA", input, "user, product, amount");

        // run a SQL query on the Table and retrieve the result as a new Table
        Table table = tEnv.sqlQuery("SELECT * FROM OrderA WHERE amount > 2 + 3");
        String explain = tEnv.explain(table);//解析执行计划
        System.out.println(explain);//打印执行计划
        System.out.println("------------------------");
        DataStream<Order> result = tEnv.toAppendStream(table, Order.class);
        result.print();
        String explain1 = tEnv.explain(table);
        System.out.println(explain1);
        System.out.println("------------------------");
        env.execute();
    }

    public static class Order {
        private long user;
        private String product;
        private int amount;

        public Order() {
        }

        public Order(long user, String product, int amount) {
            this.user = user;
            this.amount = amount;
            this.product = product;
        }

        public long getUser() {
            return user;
        }

        public void setUser(long user) {
            this.user = user;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }
    }
}
