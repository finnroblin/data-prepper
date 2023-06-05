package org.opensearch.dataprepper.plugins.processor.ruby;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.util.Map;
import java.util.Objects;

public class RubyProcessorConfig {
    private static final Boolean DEFAULT_SEND_MULTIPLE_EVENTS = false;
    private static final Boolean DEFAULT_IGNORE_EXCEPTION = false;
    @JsonProperty("code")
    private String code;

    @JsonProperty("path")
    private String path;

    @JsonProperty("init")
    private String initCode;

    @JsonProperty("params")
    private Map<String,String> params;

    @JsonProperty("send_multiple_events")
    private Boolean sendMultipleEvents = DEFAULT_SEND_MULTIPLE_EVENTS;

    @JsonProperty("ignore_exception")
    private Boolean ignoreException = DEFAULT_IGNORE_EXCEPTION;

    public void setCode(String code) {
        this.code = code;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setInitCode(String initCode) {
        this.initCode = initCode;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public Boolean isSendMultipleEvents() {
        return sendMultipleEvents;
    }

    public Boolean isCodeFromFile() {
        return Objects.nonNull(path);
    }

    public void setSendMultipleEvents(Boolean sendMultipleEvents) {
        this.sendMultipleEvents = sendMultipleEvents;
    }

    public Boolean getIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(Boolean ignoreException) {
        this.ignoreException = ignoreException;
    }

    public String getCode() {
        return code;
    }

    public String getPath() {
        return path;
    }

    public String getInitCode() {
        return initCode;
    }

    public Map<String, String> getParams() {
        return params;
    }

    @AssertTrue(message = "exactly one of {code, path} must be specified.")
    boolean isExactlyOneOfCodeAndPathSpecified() {
        return !code.isEmpty() ^ !path.isEmpty();
    }

    @AssertTrue(message = "init must be used with code.")
    boolean isInitSpecifiedWithCode() {
        return Objects.isNull(initCode) || !Objects.isNull(code); // case where init, path specified should be covered by isExactlyOneOfCodeAndPathSpecified()
    }

    boolean isSendMultipleEventsOnlySpecifiedWithPath() {
        return sendMultipleEvents.equals(Boolean.FALSE) || !Objects.isNull(path);
    }
}