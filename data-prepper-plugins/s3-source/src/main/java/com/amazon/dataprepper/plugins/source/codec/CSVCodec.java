/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.log.JacksonLog;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "csv", pluginType = Codec.class, pluginConfigurationType = CSVCodecConfig.class)
public class CSVCodec implements Codec {
    private static final String MESSAGE_FIELD_NAME = "message";
    private final CSVCodecConfig config;
    private static final Logger LOG = LoggerFactory.getLogger(CSVCodec.class);

    @DataPrepperPluginConstructor
    public CSVCodec(final CSVCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            parseBufferedReader(reader, eventConsumer);
        }
    }

    private void parseBufferedReader(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        String line;

        /*
        Tasks:
        has detect_header true
        has detect_header false -> then specified
        There's going to be an issue with the BufferedReader and the header. If the line is very long.
        TODO: Add keep_message setting (in case you want to keep the message) (actually, can use the newline codec for this. I guess have the csv codec as a highly specialized, quick reader of csv files.
        TODO: If there are too few lines on a CSV row then it doesn't throw an exception (or it's able to parse it correctly). However if there are too many rows then it throws an exception
         */
        final CsvMapper mapper = createCsvMapper();
        final CsvSchema schema;
        if (config.isDetectHeader()) {
            // autodetect header from the first line of csv file
            schema = createAutodetectCsvSchema();
        }
        else {
            // construct a header from the pipeline config or autogenerate it
            final int defaultBufferSize = 8192; // number of chars before mark is ignored (TODO: add validation for this)
            reader.mark(defaultBufferSize);
            final int firstLineSize = getSizeOfFirstLine(reader.readLine());
            reader.reset();
            schema = createCsvSchemaFromConfig(firstLineSize);
        }

        MappingIterator<Map<String, String>> parsingIterator = mapper.readerFor(Map.class).with(schema).readValues(reader);

        while (parsingIterator.hasNextValue()) {
            final int defaultBufferSize = 8192; // number of chars before mark is ignored (TODO: add validation for this)
            reader.mark(defaultBufferSize);
            try {
                final Map<String, String> parsedLine = parsingIterator.nextValue();

                final Event event = JacksonLog.builder()
                        .withData(parsedLine)
                        .build();
                eventConsumer.accept(new Record<>(event));
            } catch (final CsvReadException csvException) {
                LOG.error("Invalid CSV row, skipping this line. Consider using the CSV Processor if there might be inconsistencies " +
                        "in the number of columns because it is more flexible. ", csvException);

            } catch (final IOException otherIOException) {
                LOG.error("An IOException occurred while reading a row of the CSV file, ", otherIOException);
            } catch (final Exception e) {
                LOG.error("An Exception occurred while reading a row of the CSV file, ", e);
            }
//
//                try {
//                    reader.reset();
//                    CsvSchema resetSchema = CsvSchema.emptySchema(); // probably want to define these above to prevent lots of redefinitions
//                    // also needs the custom delimiter or quote char
//                    final CsvMapper recoveryMapper = new CsvMapper();
//                    recoveryMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows mapper to read with empty schema
//
////                    final MappingIterator<List<String>> recoveryLineIterator = recoveryMapper.readerFor(List.class).with(resetSchema)
////                            .readValues(reader.readLine());
//                    final String nextLine = reader.readLine();
//                    final MappingIterator<List<String>> recoveryLineIterator = recoveryMapper.readerFor(List.class).with(resetSchema)
//                            .readValues(nextLine);
//                    if (recoveryLineIterator.hasNextValue()) {
//                        final List<String> rowAsList = recoveryLineIterator.nextValue();
//
//                        final Map<String, String> parsedRowMap = parseRowAsListAndAutogenerateColumns(rowAsList);
//
//                        final Event event = JacksonLog.builder()
//                                .withData(parsedRowMap)
//                                .build();
//                        eventConsumer.accept(new Record<>(event));
//                    }
//                } catch (final Exception final_error) {
//                    LOG.error("Invalid CSV row, skipping this line. Consider using the CSV Processor if there might be inconsistencies " +
//                            "in the number of columns. ", final_error);
//                    System.out.println("Failed to recover a CSV line, skipping this line"+ final_error.toString());
//                    // then I guess we need to move onto the next line.
//                }

        }
    }

    private Map<String, String> parseRowAsListAndAutogenerateColumns(List<String> rowAsList) {
        Map<String, String> parsedMap = new HashMap<>();
        for (int i = 0; i < rowAsList.size(); i++) {
            parsedMap.put(generateColumnHeader(i), rowAsList.get(i));
        }
        return parsedMap;
    }

    private int getSizeOfFirstLine(String firstLine) {
        try {
            final CsvMapper firstLineMapper = new CsvMapper();
            firstLineMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows firstLineMapper to read with empty schema
            char delimiterAsChar = this.config.getDelimiter().charAt(0);
            char quoteCharAsChar = this.config.getQuoteCharacter().charAt(0);
            final CsvSchema getFirstLineLengthSchema = CsvSchema.emptySchema().withColumnSeparator(delimiterAsChar).
                    withQuoteChar(quoteCharAsChar);
            final MappingIterator<List<String>> firstLineIterator = firstLineMapper.readerFor(List.class).with(getFirstLineLengthSchema)
                    .readValues(firstLine); // need to add null verification (that readLine is not an end character or empty)
            List<String> parsedFirstLine = firstLineIterator.nextValue();
            return parsedFirstLine.size();
        } catch (final IOException e) {
            LOG.error("An exception occurred while reading first line", e);

            return 0;
        }
    }

    private CsvSchema createCsvSchemaFromConfig(int firstLineSize) {
        List<String> userSpecifiedHeader = config.getHeader();
        List<String> actualHeader = new ArrayList<>();
        final char delimiter = config.getDelimiter().charAt(0);
        final char quoteCharacter = config.getQuoteCharacter().charAt(0);
        int providedHeaderColIdx = 0;
        for (; providedHeaderColIdx < userSpecifiedHeader.size() && providedHeaderColIdx < firstLineSize; providedHeaderColIdx++) {
            actualHeader.add(userSpecifiedHeader.get(providedHeaderColIdx));
        }
        for (int remainingColIdx = providedHeaderColIdx; remainingColIdx < firstLineSize; remainingColIdx++) {
            actualHeader.add(generateColumnHeader(remainingColIdx));
        }
        CsvSchema.Builder headerBuilder = CsvSchema.builder();
        for (String columnName : actualHeader) {
            headerBuilder = headerBuilder.addColumn(columnName);
        }
        CsvSchema schema = headerBuilder.build().withColumnSeparator(delimiter).withQuoteChar(quoteCharacter);
        return schema;
    }

    private String generateColumnHeader(final int colNumber) {
        final int displayColNumber = colNumber + 1; // auto generated column name indices start from 1 (not 0)
        return "column" + displayColNumber;
    }

    private CsvMapper createCsvMapper() {
        final CsvMapper mapper = new CsvMapper();
        return mapper;
    }

    private CsvSchema createAutodetectCsvSchema() {
        final char delimiterAsChar = config.getDelimiter().charAt(0); // safe due to config input validations
        final char quoteCharAsChar = config.getQuoteCharacter().charAt(0); // safe due to config input validations
        final CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(delimiterAsChar).withQuoteChar(quoteCharAsChar).withHeader();
        return schema;
    }
}
