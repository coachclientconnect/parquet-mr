/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.proto;

import com.google.protobuf.Descriptors;

import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class ProtoParquetOptionsHelper {

  final static ProtoParquetOptionsHelper INSTANCE = new ProtoParquetOptionsHelper();

  private final WeakHashMap<Descriptors.FieldDescriptor,ProtoParquetOptions.ParquetFieldOptions> fieldOptionsMap
    = new WeakHashMap<>();

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private ProtoParquetOptionsHelper() {}

  ProtoParquetOptions.ParquetFieldOptions getFieldOptions(Descriptors.FieldDescriptor fieldDescriptor) {

    ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    try {
      ProtoParquetOptions.ParquetFieldOptions fieldOptions = fieldOptionsMap.get(fieldDescriptor);
      if (fieldOptions != null) {
        return fieldOptions;
      }
    } finally {
      readLock.unlock();
    }

    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
    try {
      ProtoParquetOptions.ParquetFieldOptions fieldOptions = fieldDescriptor.getOptions().getExtension(ProtoParquetOptions.parquet);
      fieldOptionsMap.put(fieldDescriptor, fieldOptions);
      return fieldOptions;
    } finally {
      writeLock.unlock();
    }
  }

}
