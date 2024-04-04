package com.example.redis;

import redis.clients.jedis.Jedis;

public class RedisExample {
    public static void main(String[] args) {
        // Connecting to Redis on localhost
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            System.out.println("Connection to server successfully");

//            jedis.flushDB(); //current database
//            jedis.flushAll(); //All Databases

            // Set the data in redis string
//            jedis.set("test_key", "<--- Redis --->");

            jedis.lpush("","", "");
            jedis.sadd("","");

            // Get the stored data and print it
            System.out.println("Stored string in redis:: " + jedis.get("test_key"));
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
