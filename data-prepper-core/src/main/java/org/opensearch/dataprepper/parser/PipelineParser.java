/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PipelinesDataFlowModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.parser.model.PipelineConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwardingProcessorDecorator;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.PipelineConnector;
import org.opensearch.dataprepper.peerforwarder.PeerForwarder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

@SuppressWarnings("rawtypes")
public class PipelineParser {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    private static final String PIPELINE_TYPE = "pipeline";
    private static final String ATTRIBUTE_NAME = "name";
    private final String pipelineConfigurationFileLocation;
    private final Map<String, PipelineConnector> sourceConnectorMap = new HashMap<>(); //TODO Remove this and rely only on pipelineMap
    private final PluginFactory pluginFactory;
    private final PeerForwarder peerForwarder;

    public PipelineParser(final String pipelineConfigurationFileLocation,
                          final PluginFactory pluginFactory,
                          final PeerForwarder peerForwarder) {
        this.pipelineConfigurationFileLocation = pipelineConfigurationFileLocation;
        this.pluginFactory = Objects.requireNonNull(pluginFactory);
        this.peerForwarder = Objects.requireNonNull(peerForwarder);
    }

    /**
     * Parses the configuration file into Pipeline
     */
    public Map<String, Pipeline> parseConfiguration() {
        try {
            final PipelinesDataFlowModel pipelinesDataFlowModel = OBJECT_MAPPER.readValue(new File(pipelineConfigurationFileLocation),
                    PipelinesDataFlowModel.class);

            final Map<String, PipelineConfiguration> pipelineConfigurationMap = pipelinesDataFlowModel.getPipelines().entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new PipelineConfiguration(entry.getValue())
                    ));
            final List<String> allPipelineNames = PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigurationMap);

            // LinkedHashMap to preserve insertion order
            final Map<String, Pipeline> pipelineMap = new LinkedHashMap<>();
            pipelineConfigurationMap.forEach((pipelineName, configuration) ->
                    configuration.updateCommonPipelineConfiguration(pipelineName));
            for (String pipelineName : allPipelineNames) {
                if (!pipelineMap.containsKey(pipelineName) && pipelineConfigurationMap.containsKey(pipelineName)) {
                    buildPipelineFromConfiguration(pipelineName, pipelineConfigurationMap, pipelineMap);
                }
            }
            return pipelineMap;
        } catch (IOException e) {
            throw new ParseException(format("Failed to parse the configuration file %s", pipelineConfigurationFileLocation), e);
        }
    }

    private void buildPipelineFromConfiguration(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipelineName);
        LOG.info("Building pipeline [{}] from provided configuration", pipelineName);
        try {
            final PluginSetting sourceSetting = pipelineConfiguration.getSourcePluginSetting();
            final Optional<Source> pipelineSource = getSourceIfPipelineType(pipelineName, sourceSetting,
                    pipelineMap, pipelineConfigurationMap);
            final Source source = pipelineSource.orElseGet(() ->
                    pluginFactory.loadPlugin(Source.class, sourceSetting));

            LOG.info("Building buffer for the pipeline [{}]", pipelineName);
            final Buffer buffer = pluginFactory.loadPlugin(Buffer.class, pipelineConfiguration.getBufferPluginSetting());

            LOG.info("Building processors for the pipeline [{}]", pipelineName);
            final int processorThreads = pipelineConfiguration.getWorkers();
            final List<List<Processor>> processorSets = pipelineConfiguration.getProcessorPluginSettings().stream()
                    .map(this::newProcessor)
                    .collect(Collectors.toList());

            final List<List<Processor>> decoratedProcessorSets = processorSets.stream()
                    .map(processorSet -> processorSet.stream()
                            .map(processor -> {
                                if (processor instanceof RequiresPeerForwarding) {
                                    // TODO: Create buffer per stateful processor and store map of processor, buffer
                                    // TODO: get plugin id from PipelineParser
                                    return new PeerForwardingProcessorDecorator(processor, peerForwarder, "pluginId");
                                }
                                return processor;
                            })
                            .collect(Collectors.toList())
                    ).collect(Collectors.toList());

            final int readBatchDelay = pipelineConfiguration.getReadBatchDelay();

            LOG.info("Building sinks for the pipeline [{}]", pipelineName);
            final List<Sink> sinks = pipelineConfiguration.getSinkPluginSettings().stream()
                    .map(this::buildSinkOrConnector)
                    .collect(Collectors.toList());

            final Pipeline pipeline = new Pipeline(pipelineName, source, buffer, decoratedProcessorSets, sinks, processorThreads, readBatchDelay);
            pipelineMap.put(pipelineName, pipeline);
        } catch (Exception ex) {
            //If pipeline construction errors out, we will skip that pipeline and proceed
            LOG.error("Construction of pipeline components failed, skipping building of pipeline [{}] and its connected " +
                    "pipelines", pipelineName, ex);
            processRemoveIfRequired(pipelineName, pipelineConfigurationMap, pipelineMap);
        }

    }

    private List<Processor> newProcessor(final PluginSetting pluginSetting) {
        return pluginFactory.loadPlugins(
                Processor.class,
                pluginSetting,
                actualClass -> actualClass.isAnnotationPresent(SingleThread.class) ?
                        pluginSetting.getNumberOfProcessWorkers() :
                        1);
    }

    private Optional<Source> getSourceIfPipelineType(
            final String sourcePipelineName,
            final PluginSetting pluginSetting,
            final Map<String, Pipeline> pipelineMap,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        LOG.info("Building [{}] as source component for the pipeline [{}]", pluginSetting.getName(), sourcePipelineName);
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String connectedPipeline = pipelineNameOptional.get();
            if (!sourceConnectorMap.containsKey(sourcePipelineName)) {
                LOG.info("Source of pipeline [{}] requires building of pipeline [{}]", sourcePipelineName,
                        connectedPipeline);
                //Build connected pipeline for the pipeline connector to be available
                //Building like below sometimes yields multiple runs if the pipeline building fails before sink
                //creation. except for running the creation again, it will not harm anything - TODO Fix this
                buildPipelineFromConfiguration(pipelineNameOptional.get(), pipelineConfigurationMap, pipelineMap);
            }
            if (!pipelineMap.containsKey(connectedPipeline)) {
                LOG.error("Connected Pipeline [{}] failed to build, Failing building source for [{}]",
                        connectedPipeline, sourcePipelineName);
                throw new RuntimeException(format("Failed building source for %s, exiting", sourcePipelineName));
            }
            final PipelineConnector pipelineConnector = sourceConnectorMap.get(sourcePipelineName);
            pipelineConnector.setSourcePipelineName(pipelineNameOptional.get());
            return Optional.of(pipelineConnector);
        }
        return Optional.empty();
    }

    private Sink buildSinkOrConnector(final PluginSetting pluginSetting) {
        LOG.info("Building [{}] as sink component", pluginSetting.getName());
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String pipelineName = pipelineNameOptional.get();
            final PipelineConnector pipelineConnector = new PipelineConnector(pipelineName);
            sourceConnectorMap.put(pipelineName, pipelineConnector); //TODO retrieve from parent Pipeline using name
            return pipelineConnector;
        } else {
            return pluginFactory.loadPlugin(Sink.class, pluginSetting);
        }
    }

    private Optional<String> getPipelineNameIfPipelineType(final PluginSetting pluginSetting) {
        if (PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME) != null) {
            //Validator marked valid config with type as pipeline will have attribute name
            return Optional.of((String) pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME));
        }
        return Optional.empty();
    }

    /**
     * This removes all built connected pipelines of given pipeline from pipelineMap.
     * TODO Update this to be more elegant and trigger destroy of plugins
     */
    private void removeConnectedPipelines(
            final String failedPipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration failedPipelineConfiguration = pipelineConfigurationMap.remove(failedPipeline);

        //remove source connected pipelines
        final Optional<String> sourcePipelineOptional = getPipelineNameIfPipelineType(
                failedPipelineConfiguration.getSourcePluginSetting());
        sourcePipelineOptional.ifPresent(sourcePipeline -> processRemoveIfRequired(
                sourcePipeline, pipelineConfigurationMap, pipelineMap));

        //remove sink connected pipelines
        final List<PluginSetting> sinkPluginSettings = failedPipelineConfiguration.getSinkPluginSettings();
        sinkPluginSettings.forEach(sinkPluginSetting -> {
            getPipelineNameIfPipelineType(sinkPluginSetting).ifPresent(sinkPipeline -> processRemoveIfRequired(
                    sinkPipeline, pipelineConfigurationMap, pipelineMap));
        });
    }

    private void processRemoveIfRequired(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        if (pipelineConfigurationMap.containsKey(pipelineName)) {
            pipelineMap.remove(pipelineName);
            sourceConnectorMap.remove(pipelineName);
            removeConnectedPipelines(pipelineName, pipelineConfigurationMap, pipelineMap);
        }
    }
}