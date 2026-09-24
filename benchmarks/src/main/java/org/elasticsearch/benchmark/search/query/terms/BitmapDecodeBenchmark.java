/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.terms;

import org.elasticsearch.index.query.bitmapterms.LongBitmap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Decodes a portable 64-bit bitmap the way {@code bitmap_terms} does for every shard it queries,
 * on every request.
 * <p>
 * {@code dense_bucket} spreads ids uniformly over [0, 60M): a single high bucket at about 2 bytes
 * per id, the shape of a large set of integer-like ids stored in a {@code long} field.
 * {@code sparse_buckets} spreads them over [0, 2^40): 256 buckets with almost every value in its
 * own container, the costliest shape for the per-bucket validation.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BitmapDecodeBenchmark {

    @Param({ "100000", "1000000" })
    public int cardinality;

    @Param({ "dense_bucket", "sparse_buckets" })
    public String layout;

    private byte[] bytes;

    @Setup
    public void setup() throws IOException {
        long bound = switch (layout) {
            case "dense_bucket" -> 60_000_000L;
            case "sparse_buckets" -> 1L << 40;
            default -> throw new IllegalArgumentException("unknown layout [" + layout + "]");
        };
        Random random = new Random(42);
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        for (int i = 0; i < cardinality; i++) {
            bitmap.addLong(Math.floorMod(random.nextLong(), bound));
        }
        bitmap.runOptimize();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(out)) {
            bitmap.serializePortable(data);
        }
        bytes = out.toByteArray();
    }

    @Benchmark
    public LongBitmap deserializePortable() throws IOException {
        return LongBitmap.deserializePortable(bytes);
    }
}
