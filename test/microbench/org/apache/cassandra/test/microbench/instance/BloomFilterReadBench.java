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
package org.apache.cassandra.test.microbench.instance;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class BloomFilterReadBench extends SimpleTableWriter
{
    private static final int MISSING_KEY_COUNT = 256;

    @Param({ "1", "10", "50" })
    int sstableCount;

    private long[] missingKeys;
    private int missingKeyIndex;
    private int writingSSTable;
    private String readStatement;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        setupTable();

        if (count % sstableCount != 0)
            throw new IllegalArgumentException("count must be divisible by sstableCount");

        long rowsPerSSTable = count / sstableCount;
        for (int i = 0; i < sstableCount; i++)
        {
            writingSSTable = i;
            long nextRow = 0;
            while (nextRow < rowsPerSSTable)
            {
                int writeCount = (int) Math.min(BATCH, rowsPerSSTable - nextRow);
                performWrite(nextRow, writeCount);
                nextRow += writeCount;
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        if (cfs.getLiveSSTables().size() != sstableCount)
            throw new AssertionError(String.format("Expected %d SSTables but found %d",
                                                   sstableCount, cfs.getLiveSSTables().size()));

        missingKeys = findBloomFilterMisses();
        readStatement = "SELECT * FROM " + table + " WHERE userid=?";
    }

    private void setupTable() throws Throwable
    {
        if (useNet)
            throw new IllegalArgumentException("BloomFilterReadBench supports only internal queries");

        rand = new Random(1);
        executorService = Executors.newFixedThreadPool(threadCount);
        prePrepareServer();
        prepareServer();
        DatabaseDescriptor.setAutoSnapshot(false);

        String memtableSetup = memtableClass.isEmpty() ? "" : String.format(" AND memtable = '%s'", memtableClass);
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace,
                            "CREATE TABLE %s (userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}"
                            + memtableSetup);
        execute("USE " + keyspace);
        writeStatement = "INSERT INTO " + table + " (userid, picid, commentid) VALUES (?, ?, ?)";
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
    }

    @Override
    public Object[] writeArguments(long i)
    {
        long key = 2 * (i * sstableCount + writingSSTable);
        return new Object[] { key, key, key };
    }

    @Benchmark
    public long readMissingPartitions() throws Throwable
    {
        long rows = 0;
        for (int i = 0; i < BATCH; i++)
        {
            rows += execute(readStatement, missingKeys[missingKeyIndex]).size();
            missingKeyIndex = (missingKeyIndex + 1) % missingKeys.length;
        }
        return rows;
    }

    private long[] findBloomFilterMisses()
    {
        List<Long> misses = new ArrayList<>(MISSING_KEY_COUNT);
        for (long i = sstableCount; i < count - sstableCount && misses.size() < MISSING_KEY_COUNT; i++)
        {
            long candidate = 2 * i + 1;
            DecoratedKey key = cfs.metadata().partitioner.decorateKey(LongType.instance.decompose(candidate));
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, key));
            if (view.sstables.size() != sstableCount)
                continue;

            boolean rejectedByAll = true;
            for (SSTableReader sstable : view.sstables)
            {
                if (sstable.mayContainAssumingKeyIsInRange(key))
                {
                    rejectedByAll = false;
                    break;
                }
            }

            if (rejectedByAll)
                misses.add(candidate);
        }

        if (misses.size() != MISSING_KEY_COUNT)
            throw new AssertionError(String.format("Found only %d missing keys covered by all %d SSTables",
                                                   misses.size(), sstableCount));

        return misses.stream().mapToLong(Long::longValue).toArray();
    }

    @Override
    void doExtraChecks()
    {
        if (cfs.getLiveSSTables().size() != sstableCount)
            throw new AssertionError("SSTable count changed during benchmark");
    }

    @Override
    String extraInfo()
    {
        return " sstables " + sstableCount;
    }
}
