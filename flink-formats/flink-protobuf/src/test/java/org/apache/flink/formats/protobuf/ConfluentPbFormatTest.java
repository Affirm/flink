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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.PbFormatOptions.ConfluentMode;
import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.formats.protobuf.testproto.SimpleTestMulti;
import org.apache.flink.formats.protobuf.util.ConfluentPbUtils;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for Confluent Schema Registry wire format support in the protobuf format. */
public class ConfluentPbFormatTest {

    private static final int TEST_SCHEMA_ID = 42;

    // -------------------------------------------------------------------------
    // ConfluentPbUtils unit tests
    // -------------------------------------------------------------------------

    @Test
    public void testAddConfluentHeaderDefaultSchemaId() {
        byte[] proto = new byte[] {0x0A, 0x03, 0x66, 0x6F, 0x6F};
        byte[] result = ConfluentPbUtils.addConfluentHeader(proto);

        // magic (1) + schema ID (4) + message index 0x00 (1) + proto payload
        assertEquals(proto.length + ConfluentPbUtils.FIXED_HEADER_SIZE + 1, result.length);
        assertEquals(ConfluentPbUtils.CONFLUENT_MAGIC_BYTE, result[0]);
        // Default schema ID = 0
        assertEquals(0, ByteBuffer.wrap(result, 1, 4).getInt());
        // Message index byte = 0x00
        assertEquals(0x00, result[5]);
        // Payload follows
        assertArrayEquals(proto, java.util.Arrays.copyOfRange(result, 6, result.length));
    }

    @Test
    public void testAddConfluentHeaderWithSchemaId() {
        byte[] proto = new byte[] {0x08, 0x01};
        byte[] result = ConfluentPbUtils.addConfluentHeader(proto, TEST_SCHEMA_ID);

        assertEquals(ConfluentPbUtils.CONFLUENT_MAGIC_BYTE, result[0]);
        assertEquals(TEST_SCHEMA_ID, ByteBuffer.wrap(result, 1, 4).getInt());
        assertEquals(0x00, result[5]);
        assertArrayEquals(proto, java.util.Arrays.copyOfRange(result, 6, result.length));
    }

    @Test
    public void testStripConfluentHeaderDefaultMessageIndex() throws IOException {
        byte[] proto = new byte[] {0x08, 0x01};
        byte[] withHeader = ConfluentPbUtils.addConfluentHeader(proto, TEST_SCHEMA_ID);
        byte[] stripped = ConfluentPbUtils.stripConfluentHeader(withHeader);
        assertArrayEquals(proto, stripped);
    }

    @Test
    public void testStripConfluentHeaderWithNonZeroCount() throws IOException {
        // Simulate message index with count=1, index=0 (two bytes: 0x01, 0x00)
        byte[] proto = new byte[] {0x12, 0x34};
        byte[] encoded = new byte[ConfluentPbUtils.FIXED_HEADER_SIZE + 2 + proto.length];
        ByteBuffer buf = ByteBuffer.wrap(encoded);
        buf.put(ConfluentPbUtils.CONFLUENT_MAGIC_BYTE);
        buf.putInt(TEST_SCHEMA_ID);
        buf.put((byte) 0x01); // count = 1
        buf.put((byte) 0x00); // index[0] = 0
        buf.put(proto);

        byte[] stripped = ConfluentPbUtils.stripConfluentHeader(encoded);
        assertArrayEquals(proto, stripped);
    }

    @Test
    public void testStripConfluentHeaderInvalidMagicByte() {
        byte[] invalid = new byte[] {0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x01};
        try {
            ConfluentPbUtils.stripConfluentHeader(invalid);
            fail("Expected IOException for invalid magic byte");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("magic byte"));
        }
    }

    @Test
    public void testStripConfluentHeaderTooShort() {
        byte[] tooShort = new byte[] {0x00, 0x01};
        try {
            ConfluentPbUtils.stripConfluentHeader(tooShort);
            fail("Expected IOException for too-short input");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("too short"));
        }
    }

    @Test
    public void testRoundTripHeaderIsIdempotent() throws IOException {
        byte[] proto = new byte[] {0x0A, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F};
        byte[] encoded = ConfluentPbUtils.addConfluentHeader(proto, 99);
        byte[] decoded = ConfluentPbUtils.stripConfluentHeader(encoded);
        assertArrayEquals(proto, decoded);
    }

    // -------------------------------------------------------------------------
    // End-to-end serialization / deserialization tests
    // -------------------------------------------------------------------------

    private static PbFormatConfig confluentConfig() {
        return new PbFormatConfig(
                SimpleTestMulti.class.getName(), false, false, "", ConfluentMode.TRUE);
    }

    private static PbFormatConfig autoConfig() {
        return new PbFormatConfig(
                SimpleTestMulti.class.getName(), false, false, "", ConfluentMode.AUTO);
    }

    private static RowType rowType() {
        return PbToRowTypeUtil.generateRowType(
                PbFormatUtils.getDescriptor(SimpleTestMulti.class.getName()), false);
    }

    @Test
    public void testSerializeAddsConfluentHeader() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = confluentConfig();

        SimpleTestMulti msg = SimpleTestMulti.newBuilder().setA(7).setB(42L).setF("hello").build();
        // Serialize via plain protobuf then wrap
        byte[] rawProto = msg.toByteArray();

        // Serialize via the schema in Confluent mode
        PbRowDataSerializationSchema ser = new PbRowDataSerializationSchema(rt, cfg);
        ser.open(null);
        RowData row = ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, rawProto);
        row = ProtobufTestHelper.validateRow(row, rt);
        byte[] serialized = ser.serialize(row);

        // Verify header
        assertEquals(ConfluentPbUtils.CONFLUENT_MAGIC_BYTE, serialized[0]);
        // Schema ID defaults to 0
        assertEquals(0, ByteBuffer.wrap(serialized, 1, 4).getInt());
        // Message index byte
        assertEquals(0x00, serialized[5]);
        // Remaining bytes must be valid protobuf
        SimpleTestMulti decoded =
                SimpleTestMulti.parseFrom(
                        java.util.Arrays.copyOfRange(serialized, 6, serialized.length));
        assertEquals(7, decoded.getA());
        assertEquals(42L, decoded.getB());
        assertEquals("hello", decoded.getF());
    }

    @Test
    public void testDeserializeStripsConfluentHeader() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = confluentConfig();

        SimpleTestMulti msg =
                SimpleTestMulti.newBuilder()
                        .setA(3)
                        .setB(99L)
                        .setC(true)
                        .setF("world")
                        .setG(ByteString.copyFrom(new byte[] {0x42}))
                        .build();
        byte[] withHeader = ConfluentPbUtils.addConfluentHeader(msg.toByteArray(), TEST_SCHEMA_ID);

        PbRowDataDeserializationSchema deser =
                new PbRowDataDeserializationSchema(rt, InternalTypeInfo.of(rt), cfg);
        deser.open(null);
        RowData row = deser.deserialize(withHeader);
        assertNotNull(row);
        assertEquals(3, row.getInt(0));
        assertEquals(99L, row.getLong(1));
        assertTrue(row.getBoolean(2));
        assertEquals("world", row.getString(5).toString());
        assertEquals(0x42, row.getBinary(6)[0]);
    }

    @Test
    public void testSerializeDeserializeRoundTrip() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = confluentConfig();

        SimpleTestMulti original =
                SimpleTestMulti.newBuilder()
                        .setA(100)
                        .setB(200L)
                        .setC(false)
                        .setD(1.5f)
                        .setE(2.5)
                        .setF("roundtrip")
                        .setG(ByteString.copyFrom(new byte[] {0x01, 0x02}))
                        .setH(SimpleTestMulti.Corpus.NEWS)
                        .build();

        // Deserialize raw proto into RowData
        RowData inputRow =
                ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, original.toByteArray());
        inputRow = ProtobufTestHelper.validateRow(inputRow, rt);

        // Serialize in Confluent mode
        PbRowDataSerializationSchema ser = new PbRowDataSerializationSchema(rt, cfg);
        ser.open(null);
        byte[] confluent = ser.serialize(inputRow);

        // Deserialize in Confluent mode
        PbRowDataDeserializationSchema deser =
                new PbRowDataDeserializationSchema(rt, InternalTypeInfo.of(rt), cfg);
        deser.open(null);
        RowData outputRow = deser.deserialize(confluent);

        assertNotNull(outputRow);
        assertEquals(100, outputRow.getInt(0));
        assertEquals(200L, outputRow.getLong(1));
        assertEquals("roundtrip", outputRow.getString(5).toString());
    }

    @Test
    public void testDeserializeInvalidMagicByteWithIgnoreErrors() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg =
                new PbFormatConfig(
                        SimpleTestMulti.class.getName(),
                        /* ignoreParseErrors= */ true,
                        false,
                        "",
                        ConfluentMode.TRUE);

        byte[] invalid = new byte[] {0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x01};
        PbRowDataDeserializationSchema deser =
                new PbRowDataDeserializationSchema(rt, InternalTypeInfo.of(rt), cfg);
        deser.open(null);
        // Should return null instead of throwing
        RowData row = deser.deserialize(invalid);
        assertNull(row);
    }

    @Test
    public void testOpenSourceModeIgnoresHeader() throws Exception {
        SimpleTestMulti msg = SimpleTestMulti.newBuilder().setA(55).setB(77L).build();
        byte[] rawProto = msg.toByteArray();

        // In open source mode (default), raw proto bytes are expected
        RowData row = ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, rawProto);
        assertNotNull(row);
        assertEquals(55, row.getInt(0));
        assertEquals(77L, row.getLong(1));
    }

    // -------------------------------------------------------------------------
    // AUTO mode tests
    // -------------------------------------------------------------------------

    @Test
    public void testAutoModeDeserializesConfluentFramedMessage() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = autoConfig();

        SimpleTestMulti msg = SimpleTestMulti.newBuilder().setA(11).setB(22L).build();
        byte[] withHeader = ConfluentPbUtils.addConfluentHeader(msg.toByteArray(), TEST_SCHEMA_ID);

        PbRowDataDeserializationSchema deser =
                new PbRowDataDeserializationSchema(rt, InternalTypeInfo.of(rt), cfg);
        deser.open(null);
        RowData row = deser.deserialize(withHeader);

        assertNotNull(row);
        assertEquals(11, row.getInt(0));
        assertEquals(22L, row.getLong(1));
    }

    @Test
    public void testAutoModeDeserializesPlainProtobuf() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = autoConfig();

        SimpleTestMulti msg = SimpleTestMulti.newBuilder().setA(33).setB(44L).build();
        byte[] rawProto = msg.toByteArray();

        PbRowDataDeserializationSchema deser =
                new PbRowDataDeserializationSchema(rt, InternalTypeInfo.of(rt), cfg);
        deser.open(null);
        RowData row = deser.deserialize(rawProto);

        assertNotNull(row);
        assertEquals(33, row.getInt(0));
        assertEquals(44L, row.getLong(1));
    }

    @Test
    public void testAutoModeSerializeDoesNotAddHeader() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = autoConfig();

        SimpleTestMulti msg = SimpleTestMulti.newBuilder().setA(5).build();
        byte[] rawProto = msg.toByteArray();

        PbRowDataSerializationSchema ser = new PbRowDataSerializationSchema(rt, cfg);
        ser.open(null);
        RowData row = ProtobufTestHelper.pbBytesToRow(SimpleTestMulti.class, rawProto);
        row = ProtobufTestHelper.validateRow(row, rt);
        byte[] serialized = ser.serialize(row);

        // AUTO on serialize must not prepend a Confluent header
        assertTrue(
                "AUTO mode serialize must not start with magic byte 0x00",
                serialized.length == 0 || serialized[0] != ConfluentPbUtils.CONFLUENT_MAGIC_BYTE);
        // And it must still be valid protobuf
        SimpleTestMulti decoded = SimpleTestMulti.parseFrom(serialized);
        assertEquals(5, decoded.getA());
    }

    @Test
    public void testAutoModeEmptyMessageTakesPlainPath() throws Exception {
        RowType rt = rowType();
        PbFormatConfig cfg = autoConfig();

        PbRowDataDeserializationSchema deser =
                new PbRowDataDeserializationSchema(rt, InternalTypeInfo.of(rt), cfg);
        deser.open(null);
        // Empty byte array: no first byte to inspect, falls through to plain proto path
        RowData row = deser.deserialize(new byte[0]);
        assertNotNull(row);
    }
}
