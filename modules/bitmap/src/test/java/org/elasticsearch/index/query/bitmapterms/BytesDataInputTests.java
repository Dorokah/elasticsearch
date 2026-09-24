/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BytesDataInputTests extends ESTestCase {

    /**
     * BytesDataInput replaces DataInputStream under the RoaringBitmap reader, so it must return exactly
     * what DataInputStream returns for the same bytes, call for call, and run out at the same point.
     */
    public void testMatchesDataInputStream() throws IOException {
        byte[] bytes = randomByteArrayOfLength(between(0, 512));
        DataInputStream expected = new DataInputStream(new ByteArrayInputStream(bytes));
        BytesDataInput actual = new BytesDataInput(bytes);
        while (true) {
            int op = between(0, 11);
            int needed = switch (op) {
                case 0, 1, 2 -> Byte.BYTES;
                case 3, 4, 5 -> Short.BYTES;
                case 6, 8 -> Integer.BYTES;
                case 7, 9 -> Long.BYTES;
                case 10 -> between(0, 16);
                case 11 -> 0;
                default -> throw new AssertionError(op);
            };
            if (op != 11 && actual.remaining() < needed) {
                final int length = needed;
                expectThrows(EOFException.class, () -> read(expected, op, length));
                expectThrows(EOFException.class, () -> read(actual, op, length));
                return;
            }
            int skip = between(0, 8);
            Object expectedValue = op == 11 ? expected.skipBytes(skip) : read(expected, op, needed);
            Object actualValue = op == 11 ? actual.skipBytes(skip) : read(actual, op, needed);
            assertThat("op " + op, actualValue, equalTo(expectedValue));
            if (op == 11 && actual.remaining() == 0 && randomBoolean()) {
                return;
            }
        }
    }

    private static Object read(java.io.DataInput in, int op, int length) throws IOException {
        return switch (op) {
            case 0 -> in.readByte();
            case 1 -> in.readUnsignedByte();
            case 2 -> in.readBoolean();
            case 3 -> in.readShort();
            case 4 -> in.readUnsignedShort();
            case 5 -> in.readChar();
            case 6 -> in.readInt();
            case 7 -> in.readLong();
            case 8 -> Float.floatToRawIntBits(in.readFloat());
            case 9 -> Double.doubleToRawLongBits(in.readDouble());
            case 10 -> {
                byte[] b = new byte[length];
                in.readFully(b);
                yield java.util.Arrays.toString(b);
            }
            default -> throw new AssertionError(op);
        };
    }

    public void testRemainingTracksReads() throws IOException {
        BytesDataInput in = new BytesDataInput(new byte[] { 0, 0, 0, 7, 1 });
        assertThat(in.remaining(), equalTo(5));
        assertThat(in.readInt(), equalTo(7));
        assertThat(in.remaining(), equalTo(1));
        assertThat(in.skipBytes(10), equalTo(1));
        assertThat(in.remaining(), equalTo(0));
    }
}
