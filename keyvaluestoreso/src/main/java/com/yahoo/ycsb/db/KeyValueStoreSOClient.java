/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * KeyStoreValueSO client binding for YCSB.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Properties;

import java.io.*;
import java.net.*;

//import java.lang.UnsupportedOperationException;

/**
 * YCSB binding for Key Value Store SO.
 *
 * See {@code keyvaluestoreso/README.md} for details.
 */
public class KeyValueStoreSOClient extends DB {

  public static final String HOST_PROPERTY = "ip";
  public static final String PORT_PROPERTY = "puerto";

  private Socket cliente = null;
  private PrintStream os = null;
  private DataInputStream is = null;

  public void init() throws DBException {
    System.out.println("<<<KeyStoreValueSO>>> INIT");

    Properties props = getProperties();
    int puerto;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      puerto = Integer.parseInt(portString);
    } else {
      puerto = 80;
    }
    String ipServidor = props.getProperty(HOST_PROPERTY);

    System.out.println("<<<KeyStoreValueSO>>> puerto: " + puerto);
    System.out.println("<<<KeyStoreValueSO>>> ip: " + ipServidor);

    try {

      cliente = new Socket(ipServidor, puerto);
      os = new PrintStream(cliente.getOutputStream());
      is = new DataInputStream(cliente.getInputStream());

    } catch (UnknownHostException excepcion) {
      System.err.println("ERROR: El servidor no está levantado");
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
    }
  }

  public void cleanup() throws DBException {
    System.out.println("<<<KeyStoreValueSO>>> CLEANUP");
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    System.out.println("<<<KeyStoreValueSO>>> table: " + table);
    System.out.println("<<<KeyStoreValueSO>>> key: " + key);

    try {
      os.println("get " + key);
      String response = is.readLine();
      System.out.println("<<<KeyStoreValueSO>>> response: " + response);

      if (!"Key=".equals(response)) {
        result.put(key, new StringByteIterator(response));
      }
    } catch(UnknownHostException e) {
      System.err.println("ERROR: Trying to connect to unknown host " + e);
    } catch (IOException e) {
      System.err.println("ERROR: IOException - " + e);
    }

    System.out.println("<<<KeyStoreValueSO>>> READ");

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    System.out.println("<<<KeyStoreValueSO>>> table: " + table);
    System.out.println("<<<KeyStoreValueSO>>> key: " + key);
    
    for(Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String key1 = entry.getKey();
      ByteIterator value1 = entry.getValue();
      System.out.println("<<<KeyStoreValueSO>>> key1: " + key1);
      System.out.println("<<<KeyStoreValueSO>>> value1: " + value1);

      try {
        os.println("set " + key + " " + value1);
        String response = is.readLine();
        System.out.println("<<<KeyStoreValueSO>>> response: " + response);

        if ("OK".equals(response)) {
          return Status.OK;
        } else {
          return Status.ERROR;
        }
      } catch(UnknownHostException e) {
        System.err.println("ERROR: Trying to connect to unknown host " + e);
      } catch (IOException e) {
        System.err.println("ERROR: IOException - " + e);
      }
    }

    System.out.println("<<<KeyStoreValueSO>>> INSERT");

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    /*return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;*/
    System.out.println("<<<KeyStoreValueSO>>> DELETE");
    throw new UnsupportedOperationException("OPERACION DELETE NO SOPORTADA.");
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    System.out.println("<<<KeyStoreValueSO>>> table: " + table);
    System.out.println("<<<KeyStoreValueSO>>> key: " + key);

    for(Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String key1 = entry.getKey();
      ByteIterator value1 = entry.getValue();
      System.out.println("<<<KeyStoreValueSO>>> key1: " + key1);
      System.out.println("<<<KeyStoreValueSO>>> value1: " + value1);

      try {
        os.println("set " + key + " " + value1);
        String response = is.readLine();
        System.out.println("<<<KeyStoreValueSO>>> response: " + response);

        if ("OK".equals(response)) {
          return Status.OK;
        } else {
          return Status.ERROR;
        }
      } catch(UnknownHostException e) {
        System.err.println("ERROR: Trying to connect to unknown host " + e);
      } catch (IOException e) {
        System.err.println("ERROR: IOException - " + e);
      }
    }

    System.out.println("<<<KeyStoreValueSO>>> UPDATE");

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    /*Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;*/
    System.out.println("<<<KeyStoreValueSO>>> SCAN");
    throw new UnsupportedOperationException("OPERACION SCAN NO SOPORTADA.");
  }

}
