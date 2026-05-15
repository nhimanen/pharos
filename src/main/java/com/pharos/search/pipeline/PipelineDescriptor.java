package com.pharos.search.pipeline;

/**
 * Metadata about a named search pipeline, returned by {@code GET /api/pipelines}
 * so the UI can populate a pipeline selector.
 *
 * @param id          identifier passed as {@code ?pipeline=<id>} in search requests
 * @param label       short human-readable name for display
 * @param description one-sentence explanation of the pipeline's strategy
 * @param available   false when a required component (e.g. cross-encoder) is not loaded
 */
public record PipelineDescriptor(String id, String label, String description, boolean available) {}
