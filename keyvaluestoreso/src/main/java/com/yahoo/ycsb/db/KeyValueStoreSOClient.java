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

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
//import java.lang.UnsupportedOperationException;

/**
 * YCSB binding for Key Value Store SO.
 *
 * See {@code keyvaluestoreso/README.md} for details.
 */
public class KeyValueStoreSOClient extends DB {

  public static final String HOST_PROPERTY = "ip";
  public static final String PORT_PROPERTY = "port";

  public void init() throws DBException {
    /*Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    jedis = new Jedis(host, port);
    jedis.connect();

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      jedis.auth(password);
    }*/
    System.out.println("<<<KeyStoreValueSO>>> INIT");
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
    /*if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;*/
    System.out.println("<<<KeyStoreValueSO>>> READ");
    throw new UnsupportedOperationException("OPERACION READ NO SOPORTADA.");
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    /*if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;*/
    System.out.println("<<<KeyStoreValueSO>>> INSERT");
    throw new UnsupportedOperationException("OPERACION INSERT NO SOPORTADA.");
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
    /*return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;*/
    System.out.println("<<<KeyStoreValueSO>>> UPDATE");
    throw new UnsupportedOperationException("OPERACION UPDATE NO SOPORTADA.");
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
