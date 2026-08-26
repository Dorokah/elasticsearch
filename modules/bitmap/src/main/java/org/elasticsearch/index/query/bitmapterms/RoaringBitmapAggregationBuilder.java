/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Builds a bitmap containing the distinct non-negative values of an {@code integer} or {@code long}
 * field across all matching documents. The response is the same portable bitmap format accepted by
 * {@link BitmapTermsQueryBuilder}, base64 encoded by XContent.
 */
public final class RoaringBitmapAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<RoaringBitmapAggregationBuilder> {

    public static final String NAME = "roaring_bitmap";

    static final TransportVersion ROARING_BITMAP_AGGREGATION_ADDED = TransportVersion.fromName("roaring_bitmap_aggregation_added");

    static final ParseField MAX_VALUES_FIELD = new ParseField("max_values");

    /**
     * Hard ceiling on the number of distinct values a single {@code roaring_bitmap} result may contain.
     * <p>
     * The request circuit breaker already bounds this aggregation by memory, but only as a side effect: it
     * fires late, mid-collection, with no partial result, and its trip point moves with heap size, so the
     * same request succeeds on one cluster and fails on another. It also cannot see the response. A dense
     * {@code integer} field spanning the whole value range serialises to roughly 256MB, since each of the
     * 32768 bitmap containers costs a flat 8KB, which is about 342MB of base64 in one buffered response at
     * a breaker charge that passes comfortably on a 32GB heap.
     * <p>
     * This makes the ceiling explicit and heap independent, so a request that asks for too much fails with
     * a message naming the limit instead of an opaque breaker trip. It follows the precedent set by
     * {@code search.max_buckets} for the other aggregation whose output size is unbounded by construction.
     */
    public static final Setting<Integer> MAX_VALUES_SETTING = Setting.intSetting(
        "search.max_roaring_bitmap_values",
        1_000_000,
        1,
        Setting.Property.NodeScope
    );

    static final ValuesSourceRegistry.RegistryKey<RoaringBitmapAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        RoaringBitmapAggregatorSupplier.class
    );

    public static final ObjectParser<RoaringBitmapAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        RoaringBitmapAggregationBuilder::new
    );

    static {
        // Scripts cannot declare whether their result needs a 32- or 64-bit bitmap, so this aggregation
        // deliberately requires a mapped field.
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
        PARSER.declareInt(RoaringBitmapAggregationBuilder::maxValues, MAX_VALUES_FIELD);
    }

    /** Per-request ceiling, or null to use {@link #MAX_VALUES_SETTING} alone. */
    private Integer maxValues;

    public RoaringBitmapAggregationBuilder(String name) {
        super(name);
    }

    public RoaringBitmapAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        maxValues = in.readOptionalVInt();
    }

    /**
     * Lowers the ceiling for this request only. A request may reduce the limit but never raise it above
     * {@link #MAX_VALUES_SETTING}, so this cannot be used to opt out of the node level protection.
     */
    public RoaringBitmapAggregationBuilder maxValues(int maxValues) {
        if (maxValues < 1) {
            throw new IllegalArgumentException(
                "[" + MAX_VALUES_FIELD.getPreferredName() + "] must be at least 1, but was [" + maxValues + "]"
            );
        }
        this.maxValues = maxValues;
        return this;
    }

    public Integer maxValues() {
        return maxValues;
    }

    /**
     * The node setting is a ceiling, not a default: a request may only ask for fewer values than the node
     * allows.
     */
    static int resolveMaxValues(Integer requested, Settings nodeSettings) {
        int nodeLimit = MAX_VALUES_SETTING.get(nodeSettings);
        return requested == null ? nodeLimit : Math.min(requested, nodeLimit);
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(REGISTRY_KEY, CoreValuesSourceType.NUMERIC, RoaringBitmapAggregator::new, true);
    }

    private RoaringBitmapAggregationBuilder(
        RoaringBitmapAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.maxValues = clone.maxValues;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RoaringBitmapAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(maxValues);
    }

    @Override
    protected RoaringBitmapAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        RoaringBitmapAggregatorSupplier supplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        // Resolved once on the node that builds the factory and carried on the result, so every shard and
        // the reduce phase apply the same number.
        int effectiveMaxValues = resolveMaxValues(maxValues, context.getIndexSettings().getNodeSettings());
        return new RoaringBitmapAggregatorFactory(
            name,
            config,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            supplier,
            effectiveMaxValues
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (maxValues != null) {
            builder.field(MAX_VALUES_FIELD.getPreferredName(), maxValues);
        }
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (super.equals(object) == false) {
            return false;
        }
        return Objects.equals(maxValues, ((RoaringBitmapAggregationBuilder) object).maxValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxValues);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public boolean supportsSampling() {
        // A bitmap produced from a sample cannot be scaled into the full set of matching values.
        return false;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ROARING_BITMAP_AGGREGATION_ADDED;
    }
}
