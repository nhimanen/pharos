package com.pharos.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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

    private String handleInitialize(JsonNode id, JsonNode params) {
        ObjectNode result = mapper.createObjectNode();
        result.put("protocolVersion", PROTOCOL_VERSION);
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

        try {
            String toolResult = toolRegistry.call(toolName, arguments);
            ObjectNode result = mapper.createObjectNode();
            var content = result.putArray("content");
            var textContent = content.addObject();
            textContent.put("type", "text");
                       textContent.put("text", toolResult);
            return successResponse(id, result);
        } catch (Exception e) {
            log.warn("Tool call error for {}: {}", toolName, e.getMessage());
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
