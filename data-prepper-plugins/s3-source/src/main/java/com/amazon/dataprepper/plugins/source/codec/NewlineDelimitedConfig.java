/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

/**
 * Configuration class for the newline delimited codec.
 */
public class NewlineDelimitedConfig {
    static final int DEFAULT_SKIP_LINES = 0;
    private int skipLines = DEFAULT_SKIP_LINES;

    /**
     * The number of lines to skip from the start of the S3 object.
     * Use 0 to skip no lines.
     *
     * @return The number of lines to skip.
     */
    public int getSkipLines() {
        return skipLines;
    }
}
