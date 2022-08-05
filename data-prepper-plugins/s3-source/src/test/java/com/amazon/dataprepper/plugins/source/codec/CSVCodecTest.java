/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.EventType;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CSVCodecTest {
    @Mock
    private CSVCodecConfig config;
    @Mock
    private Consumer<Record<Event>> eventConsumer;
    private CSVCodec csvCodec;
    private CSVCodec createObjectUnderTest() {
        return new CSVCodec(config);
    }

    @BeforeEach
    void setup() {
        CSVCodecConfig defaultCSVCodecConfig = new CSVCodecConfig();
        lenient().when(config.getDelimiter()).thenReturn(defaultCSVCodecConfig.getDelimiter());
        lenient().when(config.getQuoteCharacter()).thenReturn(defaultCSVCodecConfig.getQuoteCharacter());
        lenient().when(config.getHeader()).thenReturn(defaultCSVCodecConfig.getHeader());
        lenient().when(config.isDetectHeader()).thenReturn(defaultCSVCodecConfig.isDetectHeader());

        csvCodec = createObjectUnderTest();
    }

    @Test
    void test_when_configIsNull_then_throwsException() {
        config = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void test_when_nullInputStream_then_throwsException() {
        assertThrows(NullPointerException.class, () ->
                csvCodec.parse(null, eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_when_autoDetectHeaderHappyCase_then_callsConsumerWithParsedEvents(final int numberOfRows) throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.TRUE);

        final int numberOfCols = 50;
        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols);
        final String header = generateHeader(numberOfCols);
        final InputStream inputStream = createInputStreamAndAppendHeader(csvRowsExcludingHeader, header);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMap(csvRowsExcludingHeader.get(i), header);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_when_manualHeaderHappyCase_then_callsConsumerWithParsedEvents(final int numberOfRows) throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.FALSE);

        final int numberOfCols = 50;
        final List<String> header = generateHeaderAsList(numberOfCols);

        when(config.getHeader()).thenReturn(header);
        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols);
        final InputStream inputStream = createInputStream(csvRowsExcludingHeader);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMapFromHeaderList(csvRowsExcludingHeader.get(i), header);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_when_manualHeaderTooFewColumns_then_autoGenerateRemainingColumnsOnParsedEvents(final int numberOfRows) throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.FALSE);

        final int numberOfCols = 50;
        final List<String> header = generateHeaderAsList(numberOfCols-10);

        when(config.getHeader()).thenReturn(header);
        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols);
        final InputStream inputStream = createInputStream(csvRowsExcludingHeader);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        final List<String> actualHeader = addExtraAutogeneratedColumnsToExistingHeader(header, 10);
        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMapFromHeaderList(csvRowsExcludingHeader.get(i), actualHeader);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @Test
    void test_when_autoDetectHeaderWithMoreColumnsThenBufferCapacity_then_parsesEntireHeader() throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.TRUE);

        final int numberOfCols = 10000;

        final int numberOfRows = 2;

        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols);
        final String header = generateHeader(numberOfCols);

        final InputStream inputStream = createInputStreamAndAppendHeader(csvRowsExcludingHeader, header);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMap(csvRowsExcludingHeader.get(i), header);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_when_manualHeaderTooManyColumns_then_omitsExtraColumnsOnParsedEvents(final int numberOfRows) throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.FALSE);

        final int numberOfCols = 50;
        final List<String> header = generateHeaderAsList(numberOfCols+10);

        when(config.getHeader()).thenReturn(header);
        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols);
        final InputStream inputStream = createInputStream(csvRowsExcludingHeader);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        final List<String> actualHeader = header.subList(0,50);
        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMapFromHeaderList(csvRowsExcludingHeader.get(i), actualHeader);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 100})
    void test_when_autoDetectHeaderWrongNumberColumnsAndJaggedRows_then_skipsJaggedRows(final int numberOfRows) throws IOException {
// row with different length than the others should work
        when(config.isDetectHeader()).thenReturn(Boolean.TRUE);

        final int numberOfCols = 5;
        final int numberOfProperlyFormattedRows = numberOfRows-1;
        final List<String> csvRowsExcludingHeaderImmutable = generateCSVLinesAsList(numberOfProperlyFormattedRows, numberOfCols);
        final List<String> csvRowsExcludingHeader = new ArrayList<>(csvRowsExcludingHeaderImmutable);

        csvRowsExcludingHeader.add(generateCSVLine(numberOfCols+1, config.getDelimiter(), config.getQuoteCharacter()));


        final String header = generateHeader(numberOfCols);

        final InputStream inputStream = createInputStreamAndAppendHeader(csvRowsExcludingHeader, header);

        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        verify(eventConsumer, times(numberOfProperlyFormattedRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfProperlyFormattedRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMap(csvRowsExcludingHeader.get(i), header);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }


    @ParameterizedTest
    @ValueSource(ints = {2, 10, 100})
    void test_when_manualHeaderWrongNumberColumnsAndJaggedRows_then_skipsJaggedRows(final int numberOfRows) throws IOException {
        // row with different length than the others should work
        when(config.isDetectHeader()).thenReturn(Boolean.FALSE);

        final int numberOfCols = 5;

        final List<String> header = generateHeaderAsList(numberOfCols);

        when(config.getHeader()).thenReturn(header);
        final int numberOfProperlyFormattedRows = numberOfRows-1;
        final List<String> csvRowsExcludingHeaderImmutable = generateCSVLinesAsList(numberOfProperlyFormattedRows, numberOfCols);
        final List<String> csvRowsExcludingHeader = new ArrayList<>(csvRowsExcludingHeaderImmutable);

        csvRowsExcludingHeader.add(generateCSVLine(numberOfCols+1, config.getDelimiter(), config.getQuoteCharacter()));
        // following method is actually a valid line.

        final InputStream inputStream = createInputStream(csvRowsExcludingHeader);

        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        verify(eventConsumer, times(numberOfProperlyFormattedRows)).accept(recordArgumentCaptor.capture());


        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfProperlyFormattedRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMapFromHeaderList(csvRowsExcludingHeader.get(i), header);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @Test
    void test_when_tooFewColumns_then_parsedCorrectly() throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.FALSE);

        final int numberOfCols = 5;

        final List<String> header = generateHeaderAsList(numberOfCols);

        when(config.getHeader()).thenReturn(header);

        final int numberOfRows = 1;

        final List<String> csvRowsExcludingHeader = new ArrayList<>();
        csvRowsExcludingHeader.add(generateCSVLine(numberOfCols-1, config.getDelimiter(), config.getQuoteCharacter()));
        final InputStream inputStream = createInputStream(csvRowsExcludingHeader);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());
        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        assertThat(actualRecords.size(), equalTo(numberOfRows));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 100})
    void test_when_unrecoverableRow_then_logsExceptionAndSkipsOffendingRow(final int numberOfRows) throws IOException {
        // If there's an unclosed quote then expected behavior is to skip that row
        when(config.isDetectHeader()).thenReturn(Boolean.TRUE);
        when(config.getQuoteCharacter()).thenReturn(";"); // ";" is also the array character
        final int numberOfCols = 5;
        final int numberOfProperlyFormattedRows = numberOfRows;
        final List<String> csvRowsExcludingHeaderImmutable = generateCSVLinesAsList(numberOfProperlyFormattedRows, numberOfCols,
                config.getDelimiter(), config.getQuoteCharacter());
        final List<String> csvRowsExcludingHeader = new ArrayList<>(csvRowsExcludingHeaderImmutable);
        csvRowsExcludingHeader.add(generateIllegalString(numberOfCols, config.getDelimiter(), config.getQuoteCharacter()));
        csvRowsExcludingHeader.add(generateCSVLine(numberOfCols, config.getDelimiter(), config.getQuoteCharacter()));
        final String header = generateHeader(numberOfCols, config.getDelimiter().charAt(0), config.getQuoteCharacter().charAt(0));

        final InputStream inputStream = createInputStreamAndAppendHeader(csvRowsExcludingHeader, header);

        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfProperlyFormattedRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfProperlyFormattedRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMap(csvRowsExcludingHeader.get(i),
                    header, config.getDelimiter().charAt(0), config.getQuoteCharacter().charAt(0));
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    private String generateIllegalString(final int numberOfColumns, final String delimiter, final String quoteCharacter) {

        StringBuilder thisRow = new StringBuilder(quoteCharacter + UUID.randomUUID().toString() + quoteCharacter);

        for (int col = 1; col < numberOfColumns; col++) {
            String strToAppend = delimiter + quoteCharacter + UUID.randomUUID().toString();
            thisRow.append(strToAppend);
        }
        return thisRow.toString();
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 100})
    void test_when_emptyLineWithCorrectNumberDelimiters_then_parsesAsEmpty() throws IOException {
        final int numberOfCols = 5;
        final int numberOfRows = 10;

        final List<String> csvRowsExcludingHeaderImmutable = generateCSVLinesAsList(numberOfRows-2, numberOfCols);
        final List<String> csvRowsExcludingHeader = new ArrayList<>(csvRowsExcludingHeaderImmutable);
        csvRowsExcludingHeader.add(",,,,");
        csvRowsExcludingHeader.add(generateCSVLine(numberOfCols, config.getDelimiter(), config.getQuoteCharacter()));

        final String header = generateHeader(numberOfCols);
        final InputStream inputStream = createInputStreamAndAppendHeader(csvRowsExcludingHeader, header);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMap(csvRowsExcludingHeader.get(i), header);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @Test
    void test_when_manualHeaderDifferentDelimiterAndQuoteCharacter_then_parsesCorrectly() throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.FALSE);
        final String delimiter = "\t";
        final String quoteCharacter = "'";
        when(config.getDelimiter()).thenReturn(delimiter);
        when(config.getQuoteCharacter()).thenReturn(quoteCharacter);

        final int numberOfCols = 50;
        final List<String> header = generateHeaderAsList(numberOfCols);
        when(config.getHeader()).thenReturn(header);

        final int numberOfRows = 10;
        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols, delimiter, quoteCharacter);
        final InputStream inputStream = createInputStream(csvRowsExcludingHeader);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMapFromHeaderList(csvRowsExcludingHeader.get(i), header,
                    delimiter.charAt(0), quoteCharacter.charAt(0));
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @Test
    void test_when_autodetectHeaderDifferentDelimiterAndQuoteCharacter_then_parsesCorrectly() throws IOException {
        when(config.isDetectHeader()).thenReturn(Boolean.TRUE);
        final String delimiter = "\t";
        final String quoteCharacter = "'";
        when(config.getDelimiter()).thenReturn(delimiter);
        when(config.getQuoteCharacter()).thenReturn(quoteCharacter);

        final int numberOfCols = 50;
        final int numberOfRows = 10;

        final List<String> csvRowsExcludingHeader = generateCSVLinesAsList(numberOfRows, numberOfCols, delimiter, quoteCharacter);
        final String header = generateHeader(numberOfCols, delimiter.charAt(0), quoteCharacter.charAt(0));
        final InputStream inputStream = createInputStreamAndAppendHeader(csvRowsExcludingHeader, header);
        csvCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRows)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfRows));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = createExpectedMap(csvRowsExcludingHeader.get(i), header, delimiter.charAt(0),
                    quoteCharacter.charAt(0));
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    private Map<String, Object> createExpectedMapFromHeaderList(final String row, final List<String> header) throws IOException {
        final CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows mapper to read with empty schema
        final CsvSchema schema = CsvSchema.emptySchema();

        final MappingIterator<List<String>> rowIterator = mapper.readerFor(List.class).with(schema).readValues(row);
        final List<String> parsedRow = rowIterator.nextValue();
        assertThat(header.size(), equalTo(parsedRow.size()));

        Map<String, Object> expectedMap = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            expectedMap.put(header.get(i), parsedRow.get(i));
        }
        return expectedMap;
    }

    private Map<String, Object> createExpectedMapFromHeaderList(final String row, final List<String> header, final char delimiter,
                                                                final char quoteCharacter) throws IOException {
        final CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows mapper to read with empty schema
        final CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(delimiter).withQuoteChar(quoteCharacter);

        final MappingIterator<List<String>> rowIterator = mapper.readerFor(List.class).with(schema).readValues(row);
        final List<String> parsedRow = rowIterator.nextValue();
        assertThat(header.size(), equalTo(parsedRow.size()));

        Map<String, Object> expectedMap = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            expectedMap.put(header.get(i), parsedRow.get(i));
        }
        return expectedMap;
    }

    private List<String> generateHeaderAsList(final int numberOfCols) {
        List<String> header = new ArrayList<>(numberOfCols);
        for (int colNumber = 0; colNumber < numberOfCols; colNumber++) {
            final String thisColName = "list_col" + colNumber;
            header.add(thisColName);
        }
        return header;
    }

    private Map<String, Object> createExpectedMap(final String row, final String header) throws IOException {
        return createExpectedMap(row, header, ',','"');
    }

    private Map<String, Object> createExpectedMap(final String row, final String header, final char delimiter,
                                                  final char quoteCharacter) throws IOException {
        final CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows mapper to read with empty schema
        final CsvSchema schema = CsvSchema.emptySchema().withQuoteChar(quoteCharacter).withColumnSeparator(delimiter);

        final MappingIterator<List<String>> headerIterator = mapper.readerFor(List.class).with(schema).readValues(header);
        final MappingIterator<List<String>> rowIterator = mapper.readerFor(List.class).with(schema).readValues(row);

        final List<String> parsedHeader = headerIterator.nextValue();
        final List<String> parsedRow = rowIterator.nextValue();
        assertThat(parsedHeader.size(), equalTo(parsedRow.size()));

        Map<String, Object> expectedMap = new HashMap<>();
        for (int i = 0; i < parsedHeader.size(); i++) {
            expectedMap.put(parsedHeader.get(i), parsedRow.get(i));
        }
        return expectedMap;
    }

    private String generateHeader(final int numberOfCols) {
        return generateHeader(numberOfCols, ',','"');
    }

    private String generateHeader(final int numberOfCols, final char delimiter, final char quoteCharacter) {
        if (numberOfCols <= 0) {
            return "";
        }
        String header = quoteCharacter + "col0" + quoteCharacter;
        for (int colNumber = 1; colNumber < numberOfCols; colNumber++) {
            final String thisColName = quoteCharacter + "col" + colNumber + quoteCharacter;
            final String thisColWithDelimiter = delimiter + thisColName;
            header += thisColWithDelimiter;
        }
        return header;
    }

    private InputStream createInputStreamAndAppendHeader(final List<String> csvRowsExcludingHeader, final String header)
            throws IOException {
        LinkedList<String> csvRowsWithHeader = new LinkedList<>(csvRowsExcludingHeader); // Linked list for O(1) insertion at front
        csvRowsWithHeader.addFirst(header);

        return createInputStream(csvRowsWithHeader);
    }

    private InputStream createInputStream(final List<String> rowsIn) throws IOException {
        final String inputString = generateMultilineString(rowsIn);

        return new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
    }

    private List<String> generateCSVLinesAsList(final int numberOfRows, final int numberOfColumns) {
        return generateCSVLinesAsList(numberOfRows, numberOfColumns, ",", "\"");
    }
    private List<String> addExtraAutogeneratedColumnsToExistingHeader(final List<String> headerFromConfig, final int numRemainingCols) {
        List<String> header = new ArrayList<>(headerFromConfig);
        final int existingHeaderSize = headerFromConfig.size();
        for (int i = 0; i < numRemainingCols; i++) {
            final int colNumber = existingHeaderSize + i + 1;// auto generated column name indices start from 1 (not 0)
            header.add("column"+colNumber);
        }
        return header;
    }

    private List<String> generateCSVLinesAsList(final int numberOfRows, final int numberOfColumns, final String delimiter,
                                                final String quoteCharacter) {
        final List<String> csvRows = new ArrayList<>(numberOfRows);
        for (int row = 0; row < numberOfRows; row++) {
            csvRows.add(generateCSVLine(numberOfColumns, delimiter, quoteCharacter));
        }
        return Collections.unmodifiableList(csvRows);
    }

    private String generateCSVLine(final int numberOfColumns, final String delimiter, final String quoteCharacter) {
        StringBuilder thisRow = new StringBuilder(quoteCharacter + UUID.randomUUID().toString() + quoteCharacter);

        for (int col = 1; col < numberOfColumns; col++) {
            String strToAppend = delimiter + quoteCharacter + UUID.randomUUID().toString() + quoteCharacter;
            thisRow.append(strToAppend);
        }
        return thisRow.toString();
    }

    private String generateMultilineString(final List<String> csvFileRows) {
        final StringWriter stringWriter = new StringWriter();
        for (String row : csvFileRows) {
            stringWriter.write(row);
            stringWriter.write(System.lineSeparator());
        }

        return stringWriter.toString();
    }
}
