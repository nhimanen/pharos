package com.pharos.parser;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Ids of JS/TS, Python and document files must be unique and identical between the full index
 * (which parses a file list) and the incremental index (which parses one file at a time).
 *
 * <p>Previously:
 * <ul>
 *   <li>the JS extractor indexed {@code if (…) {}}, {@code for}, {@code catch}, … and calls with
 *       callbacks ({@code .finally(function () {})}) as methods — e.g. 911 of d3.js's 1,391
 *       "functions" were keywords;</li>
 *   <li>single-file extraction used the file's own directory as the root, so packages were
 *       empty: every JS file's top-level functions were {@code project:module#name()}, every
 *       same-named Python module and every README.md shared one id.</li>
 * </ul>
 */
class ScriptAndDocumentIdsTest {

    @TempDir Path root;

    private static boolean has(String cmd) {
        try {
            return new ProcessBuilder(cmd, "--version").redirectErrorStream(true).start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private Path write(String rel, String content) throws Exception {
        Path p = root.resolve(rel);
        Files.createDirectories(p.getParent());
        Files.writeString(p, content);
        return p;
    }

    private static List<String> names(ParsedFile f) {
        return f.methods().stream().map(ParsedMethod::methodName).toList();
    }

    // ── JS declarations ──────────────────────────────────────────────────────

    @Test
    void js_controlFlowAndCallbacksAreNotMethods_realDeclarationsAre() throws Exception {
        assumeTrue(has("node"), "node not installed");
        Path f = write("web/app.js", """
                import { x } from './x.js';
                export function render(nodes) {
                  if (nodes.length) { draw(nodes); }
                  for (const n of nodes) { if (n.hidden) { continue; } }
                  while (busy) { wait(); }
                  switch (mode) { case 1: break; }
                  try { risky(); } catch (e) { log(e); } finally { done(); }
                  fetch(url).then(function (r) { return r.json(); }).finally(function () { stop(); });
                  return Math.exp(values.map(([a, b]) => a * b).length) > 1;
                }
                const helper = (a, b) => a + b;
                $scope.refreshIndex = function () { Luke.index({core: 1}, function (data) { use(data); }); };
                exports.load = async function (path) { return read(path); };
                const api = {
                  save: function (doc) { return put(doc); },
                  remove: (id) => del(id),
                };
                class Widget extends Base {
                  constructor(el) { super(el); this.el = el; }
                  show() { if (this.el) { this.el.hidden = false; } }
                  static create(el) { return new Widget(el); }
                }
                """);
        ParsedFile parsed = new JsCodeParser().parseFile(f, "p", root);

        assertThat(names(parsed)).doesNotContain(
                "if", "for", "while", "switch", "catch", "finally", "function", "then", "exp", "index", "fetch");
        assertThat(names(parsed)).contains(
                "render", "helper", "refreshIndex", "load", "save", "remove", "show", "create", "constructor");
    }

    @Test
    void js_topLevelFunctionsBelongToTheirFile() throws Exception {
        assumeTrue(has("node"), "node not installed");
        Path a = write("pkg/a/index.js", "export function init() { return 1; }\n");
        Path b = write("pkg/b/index.js", "export function init() { return 2; }\n");
        Path c = write("pkg/a/util.js", "export function init() { return 3; }\n");
        JsCodeParser parser = new JsCodeParser();

        List<String> ids = new ArrayList<>();
        for (Path p : List.of(a, b, c)) ids.addAll(parser.parseFile(p, "p", root).methods().stream().map(ParsedMethod::id).toList());

        assertThat(ids).containsExactlyInAnyOrder(
                "p:pkg.a.index#init()", "p:pkg.b.index#init()", "p:pkg.a.util#init()");
    }

    @Test
    void js_fullAndIncrementalParsesProduceTheSameIds() throws Exception {
        assumeTrue(has("node"), "node not installed");
        Path a = write("src/one.js", "export function one() {}\nclass K { m() {} }\n");
        Path b = write("src/two/two.ts", "export function two(x: number) { return x; }\n");
        JsCodeParser parser = new JsCodeParser(2);

        List<String> full = parser.parseFiles(List.of(a, b), root, "p").files().stream()
                .flatMap(f -> f.methods().stream()).map(ParsedMethod::id).sorted().toList();
        List<String> incremental = new ArrayList<>();
        for (Path p : List.of(a, b)) incremental.addAll(parser.parseFile(p, "p", root).methods().stream().map(ParsedMethod::id).toList());

        assertThat(full).isNotEmpty().isEqualTo(incremental.stream().sorted().toList());
        assertThat(full).contains("p:src.one#one()", "p:src.K#m()", "p:src.two.two#two()");
    }

    // ── Python ───────────────────────────────────────────────────────────────

    @Test
    void python_sameNamedModulesInDifferentPackagesGetDistinctIds() throws Exception {
        assumeTrue(has("python3"), "python3 not installed");
        Path a = write("alpha/util.py", "def helper():\n    return 1\n");
        Path b = write("beta/util.py", "def helper():\n    return 2\n");
        PythonCodeParser parser = new PythonCodeParser();

        List<String> ids = new ArrayList<>();
        for (Path p : List.of(a, b)) ids.addAll(parser.parseFile(p, "p", root).methods().stream().map(ParsedMethod::id).toList());

        assertThat(ids).hasSize(2).doesNotHaveDuplicates();
        assertThat(String.join(" ", ids)).contains("alpha.util", "beta.util");
    }

    // ── Documents ────────────────────────────────────────────────────────────

    @Test
    void documents_sameFileNameInDifferentDirectoriesGetDistinctIds_inFullAndIncrementalParse() throws Exception {
        Path top = write("README.md", "# Top\nRoot readme.\n");
        Path nested = write("docs/guide/README.md", "# Guide\nNested readme.\n");
        GenericFileParser parser = new GenericFileParser(1);

        List<String> full = parser.parseFiles(List.of(top, nested), root, "p").files().stream()
                .flatMap(f -> f.classes().stream()).map(ParsedClass::qualifiedClassName).sorted().toList();
        List<String> incremental = new ArrayList<>();
        for (Path p : List.of(top, nested)) incremental.addAll(parser.parseFile(p, "p", root).classes().stream().map(ParsedClass::qualifiedClassName).toList());

        assertThat(full).containsExactly("README", "docs.guide.README");
        assertThat(incremental.stream().sorted().toList()).isEqualTo(full);
    }
}
