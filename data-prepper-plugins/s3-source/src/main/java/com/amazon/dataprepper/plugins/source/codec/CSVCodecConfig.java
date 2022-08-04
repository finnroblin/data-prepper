/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.List;

public class CSVCodecConfig {
    static final String DEFAULT_SOURCE = "message";
    static final String DEFAULT_DELIMITER = ",";
    static final String DEFAULT_QUOTE_CHARACTER = "\""; // double quote
    static final Boolean DEFAULT_DELETE_HEADERS = true;
    static final Boolean DEFAULT_DETECT_HEADER = true;
    @JsonProperty("source")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("delimiter")
    private String delimiter = DEFAULT_DELIMITER;

    @JsonProperty("delete_header")
    private Boolean deleteHeader = DEFAULT_DELETE_HEADERS;

    @JsonProperty("quote_character")
    private String quoteCharacter = DEFAULT_QUOTE_CHARACTER;

    @JsonProperty("column_names_source_key")
    private String columnNamesSourceKey;

    @JsonProperty("header")
    private List<String> header;

    @JsonProperty("detect_header")
    private Boolean detectHeader = DEFAULT_DETECT_HEADER;


    public String getSource() {
        return source;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public Boolean isDeleteHeader() {
        return deleteHeader;
    }

    public String getQuoteCharacter() {
        return quoteCharacter;
    }

    public String getColumnNamesSourceKey() {
        return columnNamesSourceKey;
    }

    public List<String> getHeader() {
        return header;
    }

    public Boolean isDetectHeader() {
        return detectHeader;
    }

    @AssertTrue(message = "delimiter must be exactly one character.")
    boolean isValidDelimiter() {
        return delimiter.length() == 1;
    }

    @AssertTrue(message = "quote_character must be exactly one character.")
    boolean isValidQuoteCharacter() {
        return quoteCharacter.length() == 1;
    }

    @AssertTrue(message = "quote_character and delimiter cannot be the same character")
    boolean areDelimiterAndQuoteCharacterDifferent() {
        return !(delimiter.equals(quoteCharacter));
    }
}
