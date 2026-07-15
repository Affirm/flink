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

package org.apache.flink.formats.protobuf.util;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Utility class for handling the Confluent Schema Registry wire format for Protobuf.
 *
 * <p>The Confluent wire format prepends the following header to each serialized Protobuf message:
 *
 * <pre>
 * +---------------+------------------+---------------------+------------------+
 * | magic byte    | schema ID        | message indexes     | protobuf binary  |
 * | (1 byte 0x00) | (4 bytes big-end)| (varint array)      | (variable)       |
 * +---------------+------------------+---------------------+------------------+
 * </pre>
 *
 * <p>The message indexes identify the specific message type within the schema file. For a top-level
 * (non-nested) message, this is encoded as a single {@code 0x00} byte (empty array of indexes,
 * defaulting to the first/only message). For nested message types, the array contains the path of
 * integer indexes leading to the target message descriptor.
 *
 * <p>This class does not perform schema registry lookups. The schema ID embedded in the header is
 * either written as-is during serialization or ignored during deserialization. Message class
 * resolution relies on the protobuf class available in the Flink classpath.
 */
public class ConfluentPbUtils {

    public static final byte CONFLUENT_MAGIC_BYTE = 0x00;
    /** Header size: 1 (magic) + 4 (schema ID). Message indexes follow immediately after. */
    public static final int FIXED_HEADER_SIZE = 5;

    /**
     * Strips the Confluent wire format header from {@code bytes} and returns the raw protobuf
     * binary payload.
     *
     * @param bytes full Confluent-encoded byte array
     * @return protobuf binary without the header
     * @throws IOException if the magic byte is invalid or the array is too short
     */
    public static byte[] stripConfluentHeader(byte[] bytes) throws IOException {
        // Minimum valid length: FIXED_HEADER_SIZE (5) + at least 1 byte for the message-index
        // varint.
        if (bytes == null || bytes.length < FIXED_HEADER_SIZE + 1) {
            throw new IOException(
                    "Confluent-encoded message is too short: expected at least "
                            + (FIXED_HEADER_SIZE + 1)
                            + " bytes, got "
                            + (bytes == null ? "null" : bytes.length));
        }
        if (bytes[0] != CONFLUENT_MAGIC_BYTE) {
            throw new IOException(
                    String.format(
                            "Unknown Confluent wire format: magic byte 0x%02X does not match expected 0x00.",
                            bytes[0] & 0xFF));
        }
        // Skip magic byte + 4-byte schema ID, then skip variable-length message indexes.
        int offset = FIXED_HEADER_SIZE;
        offset = skipMessageIndexes(bytes, offset);
        int payloadLength = bytes.length - offset;
        byte[] payload = new byte[payloadLength];
        System.arraycopy(bytes, offset, payload, 0, payloadLength);
        return payload;
    }

    /**
     * Prepends a Confluent wire format header to {@code protoBytes}.
     *
     * <p>The schema ID is written as {@code 0} because the protobuf class is resolved from the
     * classpath and no schema registry interaction is performed. The message indexes default to the
     * top-level first message (encoded as a single {@code 0x00} byte).
     *
     * @param protoBytes raw protobuf binary
     * @return full Confluent-encoded byte array
     */
    public static byte[] addConfluentHeader(byte[] protoBytes) {
        return addConfluentHeader(protoBytes, 0);
    }

    /**
     * Prepends a Confluent wire format header with the given {@code schemaId} to {@code
     * protoBytes}.
     *
     * @param protoBytes raw protobuf binary
     * @param schemaId schema ID to embed in the header
     * @return full Confluent-encoded byte array
     */
    public static byte[] addConfluentHeader(byte[] protoBytes, int schemaId) {
        // Header: magic (1) + schemaId (4) + message index for first message (1 byte: 0x00)
        byte[] result = new byte[FIXED_HEADER_SIZE + 1 + protoBytes.length];
        ByteBuffer buffer = ByteBuffer.wrap(result);
        buffer.put(CONFLUENT_MAGIC_BYTE);
        buffer.putInt(schemaId);
        // Message index for a single top-level message: varint count=0 → one byte 0x00
        buffer.put((byte) 0x00);
        buffer.put(protoBytes);
        return result;
    }

    /**
     * Skips the variable-length message index array starting at {@code offset} and returns the
     * offset of the first byte of the protobuf payload.
     *
     * <p>The message index array is encoded as:
     *
     * <ol>
     *   <li>A varint representing the number of indexes.
     *   <li>That many varints, one per index value.
     * </ol>
     *
     * <p>A count of {@code 0} means "use the first/default message" and no further bytes follow for
     * the index values.
     */
    static int skipMessageIndexes(byte[] bytes, int offset) throws IOException {
        // Confluent encodes message indexes as zigzag-signed varints.
        long[] countResult = readVarint(bytes, offset);
        long count = zigzagDecode(countResult[0]);
        offset = (int) countResult[1];
        for (long i = 0; i < count; i++) {
            long[] indexResult = readVarint(bytes, offset);
            offset = (int) indexResult[1];
        }
        return offset;
    }

    /** Zigzag-decodes a value encoded as {@code (n << 1) ^ (n >> 63)}. */
    static long zigzagDecode(long n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Reads an unsigned varint from {@code bytes} starting at {@code offset}.
     *
     * @return a two-element array: {@code [value, nextOffset]}
     */
    private static long[] readVarint(byte[] bytes, int offset) throws IOException {
        long value = 0;
        int shift = 0;
        while (true) {
            if (offset >= bytes.length) {
                throw new IOException(
                        "Confluent wire format: unexpected end of bytes while reading varint.");
            }
            byte b = bytes[offset++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return new long[] {value, offset};
            }
            shift += 7;
            if (shift >= 64) {
                throw new IOException("Confluent wire format: varint is too long (overflow).");
            }
        }
    }
}
