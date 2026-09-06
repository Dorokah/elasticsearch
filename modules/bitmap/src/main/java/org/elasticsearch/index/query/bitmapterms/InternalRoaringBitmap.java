/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A shard or reduced result containing a portable serialized Roaring bitmap. */
public final class InternalRoaringBitmap extends InternalAggregation {

    enum BitmapFormat {
        UNMAPPED((byte) 0),
        INT((byte) 1),
        LONG((byte) 2);

        private final byte id;

        BitmapFormat(byte id) {
            this.id = id;
        }

        static BitmapFormat read(byte id) throws IOException {
            return switch (id) {
                case 0 -> UNMAPPED;
                case 1 -> INT;
                case 2 -> LONG;
                default -> throw new IOException("unknown roaring bitmap width [" + id + "]");
            };
        }
    }

    interface MutableBitmap extends Accountable {
        void add(long value);

        long cardinality();

        void or(MutableBitmap other);

        void optimize();

        byte[] serialize() throws IOException;

        BitmapFormat width();
    }

    // TODO delete these once Roaring reports its heap use both cheaply and accurately.
    // Values for estimating a bitmap's heap use, since getLongSizeInBytes() counts payload only: INT
    // adds a structural allowance per container from RoaringBitmap#getContainerCount(), while LONG has
    // to scale the payload instead because Roaring64NavigableMap exposes no container count.
    private static final long PAYLOAD_SLACK_FACTOR = 2;
    private static final long INT_BYTES_PER_CONTAINER = 64;
    private static final long BITMAP_BASE_BYTES = 512;
    private static final long LONG_RAM_OVERHEAD_FACTOR = 8;

    // Measured serialized-to-heap expansion, to reserve against before deserializing allocates the
    // whole bitmap in one go, after which ramBytesUsed() above trues the reservation up.
    static final long DESERIALIZATION_EXPANSION_FACTOR = 9;

    private final BitmapFormat width;
    private final byte[] bitmap;

    // Carried on the result rather than read from a setting during reduce: AggregationReduceContext
    // exposes no settings, and per-shard limits alone would let the union reach shards * limit,
    // which is the same per-shard multiplication that makes terminate_after surprising.
    private final int maxValues;

    InternalRoaringBitmap(String name, BitmapFormat width, byte[] bitmap, Map<String, Object> metadata, int maxValues) {
        super(name, metadata);
        this.width = Objects.requireNonNull(width);
        this.bitmap = Objects.requireNonNull(bitmap);
        this.maxValues = maxValues;
    }

    public InternalRoaringBitmap(StreamInput in) throws IOException {
        super(in);
        width = BitmapFormat.read(in.readByte());
        bitmap = in.readByteArray();
        maxValues = in.readVInt();
    }

    static InternalRoaringBitmap unmapped(String name, Map<String, Object> metadata, int maxValues) {
        return new InternalRoaringBitmap(name, BitmapFormat.UNMAPPED, new byte[0], metadata, maxValues);
    }

    static InternalRoaringBitmap empty(String name, BitmapFormat width, Map<String, Object> metadata, int maxValues) {
        try {
            return new InternalRoaringBitmap(name, width, mutable(width).serialize(), metadata, maxValues);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize an empty Roaring bitmap", e);
        }
    }

    static MutableBitmap mutable(BitmapFormat width) {
        return switch (width) {
            case INT -> new IntMutableBitmap(new RoaringBitmap());
            case LONG -> new LongMutableBitmap(new Roaring64NavigableMap());
            case UNMAPPED -> throw new IllegalArgumentException("an unmapped aggregation has no bitmap width");
        };
    }

    private static MutableBitmap deserialize(BitmapFormat width, byte[] bytes) throws IOException {
        return switch (width) {
            case INT -> {
                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
                yield new IntMutableBitmap(bitmap);
            }
            case LONG -> {
                Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
                bitmap.deserializePortable(new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes)));
                yield new LongMutableBitmap(bitmap);
            }
            case UNMAPPED -> throw new IllegalArgumentException("an unmapped aggregation has no serialized bitmap");
        };
    }

    byte[] bitmap() {
        return bitmap;
    }

    BitmapFormat width() {
        return width;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeByte(width.id);
        out.writeByteArray(bitmap);
        out.writeVInt(maxValues);
    }

    @Override
    public String getWriteableName() {
        return RoaringBitmapAggregationBuilder.NAME;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            private MutableBitmap reduced;
            private long breakerBytes;

            @Override
            public void accept(InternalAggregation aggregation) {
                checkCancelled();
                InternalRoaringBitmap next = (InternalRoaringBitmap) aggregation;
                if (next.width == BitmapFormat.UNMAPPED) {
                    return;
                }
                if (reduced != null && reduced.width() != next.width) {
                    throw new IllegalArgumentException(
                        "[roaring_bitmap] aggregation cannot reduce [integer] and [long] field results together"
                    );
                }
                // Reserve against the deserialized size before deserializing, not after: deserialization
                // fully allocates the bitmap before ramBytesUsed() can be read, so accounting for it only
                // afterward lets the allocation land before the breaker sees the cost.
                long estimatedBytes = next.bitmap.length * DESERIALIZATION_EXPANSION_FACTOR;
                adjustBreaker(estimatedBytes);
                MutableBitmap decoded;
                try {
                    decoded = deserialize(next.width, next.bitmap);
                } catch (IOException e) {
                    adjustBreaker(-estimatedBytes);
                    throw new IllegalArgumentException("failed to deserialize [roaring_bitmap] aggregation result", e);
                }
                long decodedBytes = decoded.ramBytesUsed();
                adjustBreaker(decodedBytes - estimatedBytes);
                if (reduced == null) {
                    reduced = decoded;
                } else {
                    // Roaring's in-place OR clones containers from the incoming bitmap when the
                    // destination does not already have the corresponding key. Reserve the incoming
                    // bitmap's size as an upper bound before allowing that allocation to happen.
                    long unionReservation = decodedBytes;
                    boolean unionReservationAdded = false;
                    boolean unionReservationReconciled = false;
                    try {
                        checkCancelled();
                        adjustBreaker(unionReservation);
                        unionReservationAdded = true;
                        long before = reduced.ramBytesUsed();
                        reduced.or(decoded);
                        adjustBreaker(reduced.ramBytesUsed() - before - unionReservation);
                        unionReservationReconciled = true;
                    } finally {
                        adjustBreaker(-decodedBytes);
                        if (unionReservationAdded && unionReservationReconciled == false) {
                            adjustBreaker(-unionReservation);
                        }
                    }
                }
            }

            private void checkCancelled() {
                if (reduceContext.isCanceled().get()) {
                    throw new TaskCancelledException("cancelled");
                }
            }

            private void adjustBreaker(long bytes) {
                CircuitBreaker breaker = reduceContext.bigArrays().breakerService() == null
                    ? null
                    : reduceContext.bigArrays().breakerService().getBreaker(CircuitBreaker.REQUEST);
                if (breaker != null && bytes != 0) {
                    if (bytes > 0) {
                        breaker.addEstimateBytesAndMaybeBreak(bytes, "roaring_bitmap reduce");
                    } else {
                        breaker.addWithoutBreaking(bytes);
                    }
                    breakerBytes += bytes;
                }
            }

            @Override
            public InternalAggregation get() {
                if (reduced == null) {
                    return unmapped(name, getMetadata(), maxValues);
                }
                // The union of per-shard results can exceed the limit even when every shard respected it.
                long cardinality = reduced.cardinality();
                if (cardinality > maxValues) {
                    throw new IllegalArgumentException(
                        "["
                            + RoaringBitmapAggregationBuilder.NAME
                            + "] aggregation ["
                            + name
                            + "] reduced to more than ["
                            + maxValues
                            + "] distinct values across shards. Narrow the query, or raise the limit with "
                            + "the ["
                            + RoaringBitmapAggregationBuilder.MAX_VALUES_SETTING.getKey()
                            + "] node setting. To retrieve a large set of values, split the request into "
                            + "disjoint ranges over the aggregated field and combine the resulting bitmaps."
                    );
                }
                checkCancelled();
                long before = reduced.ramBytesUsed();
                reduced.optimize();
                adjustBreaker(reduced.ramBytesUsed() - before);
                checkCancelled();
                long serializationBytes = 2L * reduced.ramBytesUsed();
                adjustBreaker(serializationBytes);
                try {
                    return new InternalRoaringBitmap(name, reduced.width(), reduced.serialize(), getMetadata(), maxValues);
                } catch (IOException e) {
                    throw new IllegalStateException("failed to serialize reduced [roaring_bitmap] aggregation", e);
                } finally {
                    adjustBreaker(-serializationBytes);
                }
            }

            @Override
            public void close() {
                if (breakerBytes != 0) {
                    CircuitBreaker breaker = reduceContext.bigArrays().breakerService() == null
                        ? null
                        : reduceContext.bigArrays().breakerService().getBreaker(CircuitBreaker.REQUEST);
                    if (breaker != null) {
                        breaker.addWithoutBreaking(-breakerBytes);
                    }
                    breakerBytes = 0;
                }
            }
        };
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (width == BitmapFormat.UNMAPPED) {
            return builder.nullField(CommonFields.VALUE.getPreferredName());
        }
        return builder.field(CommonFields.VALUE.getPreferredName(), bitmap);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        if (path.size() == 1 && CommonFields.VALUE.getPreferredName().equals(path.get(0))) {
            return width == BitmapFormat.UNMAPPED ? null : bitmap;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass() || super.equals(object) == false) {
            return false;
        }
        InternalRoaringBitmap that = (InternalRoaringBitmap) object;
        return width == that.width && maxValues == that.maxValues && Arrays.equals(bitmap, that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), width, maxValues, Arrays.hashCode(bitmap));
    }

    private static final class IntMutableBitmap implements MutableBitmap {
        private final RoaringBitmap bitmap;

        private IntMutableBitmap(RoaringBitmap bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public void add(long value) {
            if (value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("[roaring_bitmap] integer field produced out-of-range value [" + value + "]");
            }
            bitmap.add((int) value);
        }

        @Override
        public void or(MutableBitmap other) {
            bitmap.or(((IntMutableBitmap) other).bitmap);
        }

        @Override
        public void optimize() {
            bitmap.runOptimize();
        }

        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                bitmap.serialize(out);
            }
            return bytes.toByteArray();
        }

        @Override
        public long cardinality() {
            return bitmap.getLongCardinality();
        }

        @Override
        public long ramBytesUsed() {
            return bitmap.getLongSizeInBytes() * PAYLOAD_SLACK_FACTOR + bitmap.getContainerCount() * INT_BYTES_PER_CONTAINER
                + BITMAP_BASE_BYTES;
        }

        @Override
        public BitmapFormat width() {
            return BitmapFormat.INT;
        }
    }

    private static final class LongMutableBitmap implements MutableBitmap {
        private final Roaring64NavigableMap bitmap;

        private LongMutableBitmap(Roaring64NavigableMap bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public void add(long value) {
            bitmap.addLong(value);
        }

        @Override
        public void or(MutableBitmap other) {
            bitmap.or(((LongMutableBitmap) other).bitmap);
        }

        @Override
        public void optimize() {
            bitmap.runOptimize();
        }

        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                bitmap.serializePortable(out);
            }
            return bytes.toByteArray();
        }

        @Override
        public long cardinality() {
            return bitmap.getLongCardinality();
        }

        @Override
        public long ramBytesUsed() {
            return bitmap.getLongSizeInBytes() * LONG_RAM_OVERHEAD_FACTOR;
        }

        @Override
        public BitmapFormat width() {
            return BitmapFormat.LONG;
        }
    }
}
