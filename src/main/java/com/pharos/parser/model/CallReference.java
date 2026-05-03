package com.pharos.parser.model;

/**
 * Represents a method call from one method to another.
 * Resolved=false when the callee's type could not be determined (e.g., external library call).
 * Unresolved calls are stored for later cross-project linking.
 */
public record CallReference(
        String callerFqn,    // fully-qualified caller: "com.example.MyClass#myMethod(String,int)"
        String calleeFqn,    // fully-qualified callee (may be partial if unresolved)
        String calleeSimpleName, // just "methodName" for display when unresolved
        boolean resolved,    // false if type solver could not resolve the callee class
        int lineNumber
) {
    /** Convenience factory for a resolved call. */
    public static CallReference resolved(String callerFqn, String calleeFqn, int line) {
        String simpleName = calleeFqn.contains("#") ? calleeFqn.substring(calleeFqn.indexOf('#') + 1) : calleeFqn;
        return new CallReference(callerFqn, calleeFqn, simpleName, true, line);
    }

    /** Convenience factory for an unresolved call (only method name known). */
    public static CallReference unresolved(String callerFqn, String methodName, int line) {
        return new CallReference(callerFqn, "?#" + methodName, methodName, false, line);
    }
}
