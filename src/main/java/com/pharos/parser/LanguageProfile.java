package com.pharos.parser;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Describes how to recognize code structure in a programming language using regex patterns.
 * Used by {@link RegexCodeParser} to extract classes and methods without requiring a
 * language-specific runtime or AST library.
 *
 * <p>Pattern conventions (all patterns are applied per-line in MULTILINE mode):
 * <ul>
 *   <li>Group 1 of {@code classPattern} — type/class/struct name.</li>
 *   <li>Group 1 of {@code methodPattern} — method/function name.</li>
 *   <li>Group 2 of {@code methodPattern} — raw parameter string (may be absent/empty).</li>
 *   <li>Group 1 of {@code packagePattern} — package/namespace/module name.</li>
 *   <li>Group 1 of {@code importPattern} — import path.</li>
 * </ul>
 */
public record LanguageProfile(
        String language,
        List<String> extensions,
        String lineCommentPrefix,
        Pattern packagePattern,
        Pattern importPattern,
        Pattern classPattern,
        Pattern methodPattern,
        BodyStyle bodyStyle
) {

    /** How method/class bodies are delimited. */
    public enum BodyStyle {
        /** C-style {@code { ... }} blocks (Rust, Kotlin, Scala, Swift, C#, VB.NET-like). */
        BRACES,
        /** {@code do ... end} keyword pairs (Elixir). */
        DO_END,
        /** Balanced parentheses {@code ( ... )} (Clojure). */
        PARENS,
        /** Indentation-based (Haskell, F#). */
        INDENT,
    }

    // -------------------------------------------------------------------------
    // Pre-defined profiles — one per supported language
    // -------------------------------------------------------------------------

    public static final LanguageProfile RUST = new LanguageProfile(
            "Rust",
            List.of(".rs"),
            "//",
            // Rust has no file-level package declaration; crate name comes from Cargo.toml
            null,
            Pattern.compile("^\\s*use\\s+([\\w:*{}]+)"),
            // struct Foo / enum Foo / trait Foo / impl Foo / impl Bar for Foo
            Pattern.compile(
                    "^\\s*(?:pub(?:\\s*\\([^)]*\\))?\\s+)?(?:struct|enum|trait)\\s+(\\w+)" +
                    "|^\\s*impl(?:<[^>]*>)?\\s+(?:[\\w:]+\\s+for\\s+)?(\\w+)"),
            Pattern.compile(
                    "^\\s*(?:pub(?:\\s*\\([^)]*\\))?\\s+)?(?:async\\s+)?(?:unsafe\\s+)?fn\\s+(\\w+)" +
                    "(?:<[^>]*>)?\\s*\\(([^)]*)\\)"),
            BodyStyle.BRACES
    );

    public static final LanguageProfile KOTLIN = new LanguageProfile(
            "Kotlin",
            List.of(".kt", ".kts"),
            "//",
            Pattern.compile("^package\\s+([\\w.]+)"),
            Pattern.compile("^import\\s+([\\w.*]+(?:\\s+as\\s+\\w+)?)"),
            Pattern.compile(
                    "^\\s*(?:(?:data|sealed|abstract|open|enum|value|inner|" +
                    "private|public|protected|internal)\\s+)*(?:class|interface|object)\\s+(\\w+)"),
            Pattern.compile(
                    "^\\s*(?:(?:override|suspend|inline|infix|operator|open|private|" +
                    "public|protected|internal|abstract|final|tailrec)\\s+)*" +
                    "fun\\s+(?:<[^>]*>\\s+)?(?:\\([^)]*\\)\\.)?\\s*(\\w+)\\s*(?:<[^>]*>)?\\s*\\(([^)]*)\\)"),
            BodyStyle.BRACES
    );

    public static final LanguageProfile SCALA = new LanguageProfile(
            "Scala",
            List.of(".scala"),
            "//",
            Pattern.compile("^package\\s+([\\w.]+)"),
            Pattern.compile("^import\\s+([\\w.*{}]+)"),
            Pattern.compile(
                    "^\\s*(?:(?:case|abstract|sealed|final|private|protected|override)\\s+)*" +
                    "(?:class|object|trait)\\s+(\\w+)"),
            Pattern.compile(
                    "^\\s*(?:override\\s+)?def\\s+(\\w+)(?:\\s*\\[[^\\]]*\\])?\\s*(?:\\(([^)]*)\\))?"),
            BodyStyle.BRACES
    );

    public static final LanguageProfile SWIFT = new LanguageProfile(
            "Swift",
            List.of(".swift"),
            "//",
            // Swift has no package declaration at file level
            null,
            Pattern.compile("^import\\s+(\\w+)"),
            Pattern.compile(
                    "^\\s*(?:(?:public|private|internal|open|final|fileprivate)\\s+)*" +
                    "(?:class|struct|enum|protocol|extension)\\s+(\\w+)"),
            Pattern.compile(
                    "^\\s*(?:(?:public|private|internal|open|override|static|class|" +
                    "mutating|final|required|convenience|@\\w+)\\s+)*" +
                    "func\\s+(\\w+)(?:\\s*<[^>]*>)?\\s*\\(([^)]*)\\)"),
            BodyStyle.BRACES
    );

    public static final LanguageProfile CSHARP = new LanguageProfile(
            "C#",
            List.of(".cs"),
            "//",
            Pattern.compile("^namespace\\s+([\\w.]+)"),
            Pattern.compile("^using\\s+(?:static\\s+)?([\\w.]+)"),
            Pattern.compile(
                    "^\\s*(?:(?:public|private|protected|internal|static|abstract|" +
                    "sealed|partial|new)\\s+)*(?:class|interface|struct|enum|record)\\s+(\\w+)"),
            // Match access modifiers + any return type + method name + (params)
            Pattern.compile(
                    "^\\s*(?:(?:public|private|protected|internal|static|virtual|override|" +
                    "abstract|async|new|sealed|readonly|extern)\\s+)+" +
                    "[\\w<>\\[\\].,?\\s]+\\s+(\\w+)\\s*\\(([^)]*)\\)\\s*(?:where\\s+.*)?\\s*[{;]"),
            BodyStyle.BRACES
    );

    public static final LanguageProfile FSHARP = new LanguageProfile(
            "F#",
            List.of(".fs", ".fsx"),
            "//",
            Pattern.compile("^(?:module|namespace)\\s+([\\w.]+)"),
            Pattern.compile("^open\\s+([\\w.]+)"),
            Pattern.compile("^\\s*type\\s+(\\w+)(?:\\s*=)?"),
            // let [rec] name [args] = ... and member [this.]name(args) = ...
            Pattern.compile(
                    "^\\s*(?:let(?:\\s+rec)?\\s+(\\w+)|" +
                    "(?:member|override|abstract)\\s+(?:\\w+\\.)?(\\w+)\\s*\\(([^)]*)\\))"),
            BodyStyle.INDENT
    );

    public static final LanguageProfile HASKELL = new LanguageProfile(
            "Haskell",
            List.of(".hs", ".lhs"),
            "--",
            Pattern.compile("^module\\s+([\\w.]+)"),
            Pattern.compile("^import\\s+(?:qualified\\s+)?([\\w.]+)"),
            // data Foo / newtype Foo / class ... Foo where
            Pattern.compile(
                    "^(?:data|newtype)\\s+(\\w+)" +
                    "|^class\\s+(?:[\\w()\\s]+\\s+)?(\\w+)(?:\\s+\\w+)*\\s+where"),
            // Type signatures: foo :: ...  (used as method discovery hook)
            Pattern.compile("^(\\w+)\\s*::"),
            BodyStyle.INDENT
    );

    public static final LanguageProfile ELIXIR = new LanguageProfile(
            "Elixir",
            List.of(".ex", ".exs"),
            "#",
            // defmodule IS the class/module declaration — package derived from module name
            Pattern.compile("^\\s*defmodule\\s+([\\w.]+)"),
            Pattern.compile("^\\s*(?:import|alias|use|require)\\s+([\\w.]+)"),
            Pattern.compile("^\\s*defmodule\\s+([\\w.]+)"),
            // def name(params) do  or  defp name(params) do
            Pattern.compile("^\\s*def(?:p)?\\s+(\\w+)(?:\\s*\\(([^)]*)\\))?(?:\\s+do|,)?"),
            BodyStyle.DO_END
    );

    public static final LanguageProfile CLOJURE = new LanguageProfile(
            "Clojure",
            List.of(".clj", ".cljs", ".cljc"),
            ";",
            Pattern.compile("^\\(ns\\s+([\\w.-]+)"),
            Pattern.compile("(?::require|:use|:import)\\s+\\[?([\\w.-]+)"),
            Pattern.compile("^\\s*\\((?:defrecord|deftype|defprotocol)\\s+(\\w+)"),
            Pattern.compile("^\\s*\\(defn-?\\s+(\\w+)\\s+\\[([^\\]]*)\\]"),
            BodyStyle.PARENS
    );

    public static final LanguageProfile VB_NET = new LanguageProfile(
            "VB.NET",
            List.of(".vb"),
            "'",
            Pattern.compile("(?i)^Namespace\\s+([\\w.]+)"),
            Pattern.compile("(?i)^Imports\\s+([\\w.]+)"),
            Pattern.compile(
                    "(?i)^\\s*(?:Public|Private|Protected|Friend|Partial|" +
                    "MustInherit|NotInheritable)?\\s*" +
                    "(?:Class|Interface|Structure|Module|Enum)\\s+(\\w+)"),
            Pattern.compile(
                    "(?i)^\\s*(?:Public|Private|Protected|Friend|Shared|Overrides|" +
                    "Overridable|MustOverride|Async)?\\s*" +
                    "(?:Sub|Function)\\s+(\\w+)\\s*\\(([^)]*)\\)"),
            BodyStyle.DO_END  // VB uses End Sub / End Function (treated as keyword-delimited)
    );

    /** All tier-2 profiles in registration order. */
    public static final List<LanguageProfile> ALL = List.of(
            RUST, KOTLIN, SCALA, SWIFT, CSHARP, FSHARP, HASKELL, ELIXIR, CLOJURE, VB_NET
    );
}
