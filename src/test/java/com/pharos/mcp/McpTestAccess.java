package com.pharos.mcp;

/** Test access to {@link McpServer#handleMessage} from other packages. */
public final class McpTestAccess {
    private McpTestAccess() {}

    public static String handle(McpServer server, String jsonLine) throws Exception {
        return server.handleMessage(jsonLine);
    }
}
