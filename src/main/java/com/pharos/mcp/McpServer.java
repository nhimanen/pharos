package com.pharos.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * MCP (Model Context Protocol) server over stdio JSON-RPC 2.0.
 *
 * Claude Code connects to this server via:
 *   "mcpServers": { "pharos": { "command": "java", "args": ["-jar", "pharos.jar", "mcp-server"] } }
 *
 * Protocol flow:
 *   → initialize request
 *   ← initialize response (with capabilities)
 *   → tools/list request
 *   ← tools/list response (available tools)
 *   → tools/call request
 *   ← tools/call response (tool result)
 */
public class McpServer {

    private static final Logger log = LoggerFactory.getLogger(McpServer.class);
    private static final String PROTOCOL_VERSION = "2024-11-05";
    private static final String SERVER_NAME = "pharos";
    private static final String SERVER_VERSION = "1.0.0";

    private final McpToolRegistry toolRegistry;
    private final ObjectMapper mapper = new ObjectMapper();

    public McpServer(McpToolRegistry toolRegistry) {
        this.toolRegistry = toolRegistry;
    }

    /** Start the stdio JSON-RPC loop. Blocks until stdin is closed. */
    public void start() {
        log.info("MCP server started (stdio mode)");
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in, StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true)) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                try {
                    String response = handleMessage(line);
                    if (response != null) {
                        writer.println(response);
                        writer.flush();
                    }
                } catch (Exception e) {
                    log.error("Error handling message: {}", e.getMessage(), e);
                    writer.println(errorResponse(null, -32603, "Internal error: " + e.getMessage()));
                    writer.flush();
                }
            }
        } catch (IOException e) {
            log.error("MCP server IO error: {}", e.getMessage());
        }
        log.info("MCP server stopped");
    }

    private String handleMessage(String jsonLine) throws Exception {
        JsonNode msg = mapper.readTree(jsonLine);
        String method = msg.path("method").asText();
        JsonNode id = msg.get("id");
        JsonNode params = msg.get("params");

        return switch (method) {
            case "initialize"        -> handleInitialize(id, params);
            case "initialized"       -> null; // notification, no response
            case "tools/list"        -> handleToolsList(id);
            case "tools/call"        -> handleToolCall(id, params);
            case "ping"              -> successResponse(id, mapper.createObjectNode());
            default -> errorResponse(id, -32601, "Method not found: " + method);
        };
    }

    private static final String INSTRUCTIONS = """
            Pharos is a Java code search and navigation tool. Use it instead of grep or file reads when the current project is indexed.

            Start with `list_projects` to check if the current project is indexed.

            Tool selection guide:
            - Finding code by concept or description → `search_code` with type=hybrid (default)
            - Finding code by exact method or class name → `search_code` with type=keyword (BM25)
            - Getting a full method implementation → `get_method` with the FQN from search results
            - Understanding what a method calls / depends on → `get_callees`
            - Understanding who calls a method (impact of a change) → `get_callers`
            - Tracing an execution path between two methods → `find_call_path`
            - Understanding module dependencies in a multi-module Maven project → `get_module_deps` or `find_module_path`
            - Identifying a module's public API surface → `get_module_boundary`

            FQN format: `com.example.MyClass#methodName(ParamType1,ParamType2)`
            Use FQNs from `search_code` results directly in `get_method`, `get_callers`, `get_callees`.

            For broad questions, start with `search_code` then drill down with graph tools.
            """;

    private String handleInitialize(JsonNode id, JsonNode params) {
        ObjectNode result = mapper.createObjectNode();
        result.put("protocolVersion", PROTOCOL_VERSION);
        result.put("instructions", INSTRUCTIONS);
        ObjectNode info = result.putObject("serverInfo");
        info.put("name", SERVER_NAME);
        info.put("version", SERVER_VERSION);
        ObjectNode capabilities = result.putObject("capabilities");
        capabilities.putObject("tools");
        return successResponse(id, result);
    }

    private String handleToolsList(JsonNode id) {
        ObjectNode result = mapper.createObjectNode();
        result.set("tools", mapper.valueToTree(toolRegistry.getToolDefinitions()));
        return successResponse(id, result);
    }

    private String handleToolCall(JsonNode id, JsonNode params) {
        String toolName = params.path("name").asText();
        JsonNode arguments = params.path("arguments");

        long start = System.currentTimeMillis();
        try {
            String toolResult = toolRegistry.call(toolName, arguments);
            long ms = System.currentTimeMillis() - start;
            log.info("tool={} args={} status=ok latency_ms={} result_chars={}",
                    toolName, arguments, ms, toolResult.length());
            ObjectNode result = mapper.createObjectNode();
            var content = result.putArray("content");
            var textContent = content.addObject();
            textContent.put("type", "text");
            textContent.put("text", toolResult);
            return successResponse(id, result);
        } catch (Exception e) {
            long ms = System.currentTimeMillis() - start;
            log.warn("tool={} args={} status=error latency_ms={} error={}",
                    toolName, arguments, ms, e.getMessage());
            return errorResponse(id, -32603, "Tool error: " + e.getMessage());
        }
    }

    private String successResponse(JsonNode id, ObjectNode result) {
        ObjectNode resp = mapper.createObjectNode();
        resp.put("jsonrpc", "2.0");
        if (id != null) resp.set("id", id);
        resp.set("result", result);
        return resp.toString();
    }

    private String errorResponse(JsonNode id, int code, String message) {
        ObjectNode resp = mapper.createObjectNode();
        resp.put("jsonrpc", "2.0");
        if (id != null) resp.set("id", id);
        ObjectNode error = resp.putObject("error");
        error.put("code", code);
        error.put("message", message);
        return resp.toString();
    }
}
