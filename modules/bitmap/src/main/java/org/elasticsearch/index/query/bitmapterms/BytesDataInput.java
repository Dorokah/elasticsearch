/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.util.BitUtil;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

/**
 * A {@link DataInput} over a byte array, for handing a serialized 64-bit bitmap to RoaringBitmap's
 * reader.
 * <p>
 * {@code DataInputStream} over {@code ByteArrayInputStream} does the same job, but the reader asks for
 * one value at a time and each request goes through the stream's synchronized {@code read()}, which
 * dominates the decode of a large bitmap. {@code Roaring64NavigableMap} has no {@code ByteBuffer}
 * entry point the way {@code RoaringBitmap} does, so this is the way to avoid that cost. Lucene's
 * {@code ByteArrayDataInput} is not a {@link DataInput} and is little-endian.
 * <p>
 * Follows the {@link DataInput} contract: big-endian, and {@link EOFException} on a short read.
 */
final class BytesDataInput implements DataInput {

    private final byte[] bytes;
    private int position;

    BytesDataInput(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes);
    }

    /** The number of bytes not yet read. */
    int remaining() {
        return bytes.length - position;
    }

    private void require(int length) throws EOFException {
        if (remaining() < length) {
            throw new EOFException("needed [" + length + "] more byte(s) but only [" + remaining() + "] remain");
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        require(len);
        System.arraycopy(bytes, position, b, off, len);
        position += len;
    }

    @Override
    public int skipBytes(int n) {
        int skipped = Math.max(0, Math.min(n, remaining()));
        position += skipped;
        return skipped;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        require(Byte.BYTES);
        return bytes[position++];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort() throws IOException {
        require(Short.BYTES);
        short value = (short) BitUtil.VH_BE_SHORT.get(bytes, position);
        position += Short.BYTES;
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException {
        return (char) readUnsignedShort();
    }

    @Override
    public int readInt() throws IOException {
        require(Integer.BYTES);
        int value = (int) BitUtil.VH_BE_INT.get(bytes, position);
        position += Integer.BYTES;
        return value;
    }

    @Override
    public long readLong() throws IOException {
        require(Long.BYTES);
        long value = (long) BitUtil.VH_BE_LONG.get(bytes, position);
        position += Long.BYTES;
        return value;
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /** Not part of the bitmap format, and deprecated in {@code DataInputStream} itself. */
    @Override
    public String readLine() {
        throw new UnsupportedOperationException("readLine is not supported");
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }
}
