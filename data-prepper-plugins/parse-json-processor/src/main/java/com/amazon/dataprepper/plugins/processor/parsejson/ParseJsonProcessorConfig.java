/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.parsejson;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class ParseJsonProcessorConfig {
    static final String DEFAULT_SOURCE = "message";
    @NotEmpty
    @NotNull
    @JsonProperty("source")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("destinatino")
    private String destination;

    /**
     * The field of the Event that contains the JSON data.
     *
     * @return The name of the source field.
     */
    public String getSource() {
        return source;
    }

    /**
     * The destination that the parsed JSON is written to. Defaults to the root of the Event.
     * If the destination field already exists, it will be overwritten.
     *
     * @return The name of the destination field.
     */
    public String getDestination() {
        return destination;
    }
}