package com.pharos.parser;

import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import com.pharos.parser.model.ParsedRelationships;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Unlike the Python/JS/Terraform parser tests, there is no {@code Assumptions} guard here:
 * {@link GoCodeParser} is pure Java and needs no toolchain on PATH.
 */
class GoCodeParserTest {

    private static final Path SAMPLE_APP = sampleAppPath();
    private static final String PROJECT = "sample-go-app";

    private static final String PKG_ROOT  = "github.com/acme/sample";
    private static final String PKG_MODEL = "github.com/acme/sample/model";
    private static final String PKG_SVC   = "github.com/acme/sample/service";

    private static ParsedProject parseSample() throws IOException {
        return new GoCodeParser(2).parseProject(SAMPLE_APP, PROJECT);
    }

    // -----------------------------------------------------------------------
    // Registration basics
    // -----------------------------------------------------------------------

    @Test
    void supportedExtensions_isGo() {
        assertThat(new GoCodeParser().supportedExtensions()).containsExactly(".go");
    }

    @Test
    void parsesEveryGoFileInTheTree() throws IOException {
        assertThat(parseSample().files())
                .extracting(f -> f.filePath().replaceAll(".*sample-go-app/", ""))
                .containsExactlyInAnyOrder("main.go", "model/user.go", "service/greeting.go");
    }

    // -----------------------------------------------------------------------
    // Package naming — the Go import path, from go.mod
    // -----------------------------------------------------------------------

    @Test
    void packageNameIsTheGoImportPathDerivedFromGoMod() throws IOException {
        assertThat(parseSample().files())
                .extracting(ParsedFile::packageName)
                .containsExactlyInAnyOrder(PKG_ROOT, PKG_MODEL, PKG_SVC);
    }

    @Test
    void withoutGoModThePackageClauseIsUsedInstead(@TempDir Path dir) throws IOException {
        Path f = dir.resolve("solo.go");
        Files.writeString(f, "package solo\n\nfunc Ping() string { return \"pong\" }\n");

        ParsedFile parsed = new GoCodeParser().parseFile(f, "p");
        assertThat(parsed.packageName()).isEqualTo("solo");
        assertThat(parsed.methods()).extracting(ParsedMethod::methodName).containsExactly("Ping");
    }

    // -----------------------------------------------------------------------
    // Type declarations
    // -----------------------------------------------------------------------

    @Test
    void structsAndInterfacesGetDistinctKinds() throws IOException {
        List<ParsedClass> classes = allClasses(parseSample());

        assertThat(classes).filteredOn(c -> c.qualifiedClassName().equals(PKG_ROOT + ".Server"))
                .singleElement().extracting(ParsedClass::kind).isEqualTo("class");
        assertThat(classes).filteredOn(c -> c.qualifiedClassName().equals(PKG_ROOT + ".Handler"))
                .singleElement().satisfies(c -> {
                    assertThat(c.kind()).isEqualTo("interface");
                    assertThat(c.isAbstract()).isTrue();
                });
    }

    @Test
    void groupedTypeBlockDeclaresEveryTypeInIt() throws IOException {
        // main.go declares `type ( Mode int; pair struct {...} )`
        assertThat(allClasses(parseSample()))
                .extracting(ParsedClass::qualifiedClassName)
                .contains(PKG_ROOT + ".Mode", PKG_ROOT + ".pair");
    }

    @Test
    void exportednessMapsToAccessModifier() throws IOException {
        List<ParsedClass> classes = allClasses(parseSample());
        assertThat(classOf(classes, PKG_ROOT + ".Server").accessModifier()).isEqualTo("public");
        assertThat(classOf(classes, PKG_ROOT + ".pair").accessModifier()).isEqualTo("private");
    }

    @Test
    void embeddedStructBecomesSuperclass() throws IOException {
        assertThat(classOf(allClasses(parseSample()), PKG_MODEL + ".User").superclass())
                .isEqualTo("Base");
    }

    @Test
    void embeddedInterfacesBecomeInterfaceList() throws IOException {
        assertThat(classOf(allClasses(parseSample()), PKG_SVC + ".FormatterNamer").interfaces())
                .containsExactly("Formatter", "Namer");
    }

    @Test
    void docCommentAboveDeclarationBecomesJavadoc() throws IOException {
        assertThat(classOf(allClasses(parseSample()), PKG_SVC + ".Greeter").javadoc())
                .isEqualTo("Greeter renders greetings for users.");
    }

    @Test
    void buildDirectivesAreNotTreatedAsDocumentation(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("gen.go"), """
                package gen

                // Real documentation.
                //go:generate stringer -type=Kind
                func Run() {}
                """);
        ParsedFile parsed = new GoCodeParser().parseFile(dir.resolve("gen.go"), "p");
        assertThat(parsed.methods()).singleElement()
                .extracting(ParsedMethod::javadoc).isEqualTo("Real documentation.");
    }

    // -----------------------------------------------------------------------
    // Receivers — the thing the generic regex tier gets wrong
    // -----------------------------------------------------------------------

    @Test
    void methodsBindToTheirReceiverTypeNotToTheNearestTypeAbove() throws IOException {
        ParsedProject proj = parseSample();

        // Greeter's methods are declared after Formatter/Namer/Loud in the file; a
        // "nearest type above" heuristic would attach them to Loud.
        assertThat(methodFqns(proj)).contains(
                PKG_SVC + ".Greeter#Greet()",
                PKG_SVC + ".Greeter#join()",
                PKG_SVC + ".Greeter#Format()");
        assertThat(methodFqns(proj)).doesNotContain(PKG_SVC + ".Loud#Greet()");
    }

    @Test
    void signaturePreservesPointerVersusValueReceiver() throws IOException {
        ParsedProject proj = parseSample();
        assertThat(method(proj, PKG_MODEL + ".User#Tag()").signature())
                .isEqualTo("func (u *User) Tag(key string, value string) int");
        assertThat(method(proj, PKG_MODEL + ".User#DisplayName()").signature())
                .isEqualTo("func (u User) DisplayName() string");
    }

    @Test
    void signaturePreservesTypeParameters() throws IOException {
        assertThat(method(parseSample(), PKG_SVC + ".greeting#Map()").signature())
                .isEqualTo("func Map[T, U any](in []T, f func(T) U) []U");
    }

    @Test
    void packageLevelFunctionsLandInAPerFileModulePseudoClass() throws IOException {
        ParsedProject proj = parseSample();

        ParsedClass pseudo = classOf(allClasses(proj), PKG_SVC + ".greeting");
        assertThat(pseudo.kind()).isEqualTo("module");
        assertThat(method(proj, PKG_SVC + ".greeting#NewGreeter()").isStatic()).isTrue();
    }

    @Test
    void interfaceMethodsAreAbstractAndNotStatic() throws IOException {
        ParsedMethod handle = method(parseSample(), PKG_ROOT + ".Handler#Handle()");
        assertThat(handle.isAbstract()).isTrue();
        assertThat(handle.isStatic()).isFalse();
        assertThat(handle.body()).isEmpty();
        assertThat(handle.returnType()).isEqualTo("error");
        assertThat(handle.javadoc()).isEqualTo("Handle processes one request.");
    }

    // -----------------------------------------------------------------------
    // Parameters
    // -----------------------------------------------------------------------

    @Test
    void groupedParametersShareTheTrailingType() throws IOException {
        // func (u *User) Tag(key, value string) int
        assertThat(method(parseSample(), PKG_MODEL + ".User#Tag()").paramNames())
                .containsExactly("key", "value");
    }

    @Test
    void variadicParameterIsKept() throws IOException {
        ParsedMethod join = method(parseSample(), PKG_SVC + ".Greeter#join()");
        assertThat(join.paramNames()).containsExactly("parts");
        assertThat(join.signature()).contains("parts ...string");
    }

    @Test
    void unnamedParametersAreTreatedAsTypesNotNames(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("iface.go"), """
                package iface

                type Writer interface {
                	Write([]byte, int) (int, error)
                }
                """);
        ParsedFile parsed = new GoCodeParser().parseFile(dir.resolve("iface.go"), "p");
        // With no `name Type` entry, Go treats every entry as an unnamed type.
        assertThat(parsed.methods()).singleElement().satisfies(m -> {
            assertThat(m.paramNames()).containsExactly("[]byte", "int");
            assertThat(m.returnType()).isEqualTo("(int, error)");
        });
    }

    // -----------------------------------------------------------------------
    // Literal masking — braces inside strings/comments/runes must not shift extents
    // -----------------------------------------------------------------------

    @Test
    void bracesInsideStringsAndRunesDoNotTruncateBodies() throws IOException {
        ParsedMethod sanitize = method(parseSample(), PKG_MODEL + ".user#Sanitize()");
        // The body contains '{', "{" and "}" literals; a naive brace counter stops early.
        assertThat(sanitize.body()).contains("ContainsRune(name, '{')")
                .contains("strings.Trim(name, \"}\")");
        assertThat(sanitize.endLine()).isGreaterThan(sanitize.startLine() + 3);
    }

    @Test
    void rawStringWithBracesDoesNotTruncateBody() throws IOException {
        ParsedMethod render = method(parseSample(), PKG_ROOT + ".Server#render()");
        assertThat(render.body()).contains("`{\"greeting\": \"%s\"}`")
                .contains("return s.greeter.Greet(u.DisplayName())");
    }

    @Test
    void commentedOutCodeIsIgnored(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("c.go"), """
                package c

                // import "should/not/appear"
                import "real/pkg"

                func F() {}
                """);
        assertThat(new GoCodeParser().parseFile(dir.resolve("c.go"), "p").imports())
                .containsExactly("real/pkg");
    }

    @Test
    void compositeResultTypeIsNotMistakenForTheFunctionBody() throws IOException {
        ParsedMethod describe = method(parseSample(), PKG_SVC + ".greeting#Describe()");
        assertThat(describe.returnType()).isEqualTo("struct{ Kind string }");
        assertThat(describe.body()).contains("Kind: \"greeter\"");
    }

    // -----------------------------------------------------------------------
    // Call graph — only resolved references become graph edges
    // -----------------------------------------------------------------------

    @Test
    void fqnOmitsParameterTypesSoNameBasedResolutionIsExact() throws IOException {
        ParsedMethod handle = method(parseSample(), PKG_ROOT + ".Server#Handle()");
        assertThat(handle.paramTypes()).isEmpty();
        assertThat(handle.fqn()).isEqualTo(PKG_ROOT + ".Server#Handle()");
        assertThat(handle.id()).isEqualTo(PROJECT + ":" + handle.fqn());
    }

    @Test
    void receiverCallsResolveToTheReceiversOwnType() throws IOException {
        // func (g *Greeter) Greet(...) { return g.join(...) }
        assertThat(resolvedCallees(parseSample(), PKG_SVC + ".Greeter#Greet()"))
                .containsExactly(PKG_SVC + ".Greeter#join()");
    }

    @Test
    void samePackageFunctionCallsResolve() throws IOException {
        assertThat(resolvedCallees(parseSample(), PKG_ROOT + ".main#main()"))
                .containsExactly(PKG_ROOT + ".main#run()");
    }

    @Test
    void crossPackageCallsResolveThroughTheImportList() throws IOException {
        // NewServer calls service.NewGreeter()
        assertThat(resolvedCallees(parseSample(), PKG_ROOT + ".main#NewServer()"))
                .containsExactly(PKG_SVC + ".greeting#NewGreeter()");
    }

    @Test
    void localVariableTypeIsInferredFromConstructorCall() throws IOException {
        // func run(...) { srv := NewServer(addr); return srv.Handle(nil, nil) }
        assertThat(resolvedCallees(parseSample(), PKG_ROOT + ".main#run()"))
                .containsExactlyInAnyOrder(
                        PKG_ROOT + ".main#NewServer()",
                        PKG_ROOT + ".Server#Handle()");
    }

    @Test
    void thirdPartyCallsStayUnresolvedRatherThanGuessed() throws IOException {
        List<CallReference> calls = method(parseSample(), PKG_SVC + ".Greeter#join()")
                .calledMethods();
        assertThat(calls).isNotEmpty();
        // strings.Join is outside the project — no edge should be invented for it.
        assertThat(calls).filteredOn(c -> c.calleeSimpleName().equals("Join"))
                .singleElement().extracting(CallReference::resolved).isEqualTo(false);
    }

    @Test
    void builtinsAndKeywordsAreNotRecordedAsCalls() throws IOException {
        List<String> callees = method(parseSample(), PKG_SVC + ".greeting#Map()")
                .calledMethods().stream().map(CallReference::calleeSimpleName).toList();
        assertThat(callees).doesNotContain("make", "len", "append", "for", "range");
    }

    @Test
    void callSitesRecordArgumentCountAndLine() throws IOException {
        CallReference render = method(parseSample(), PKG_ROOT + ".Server#Handle()")
                .calledMethods().stream()
                .filter(c -> c.calleeFqn().endsWith("#render()"))
                .findFirst().orElseThrow();
        assertThat(render.paramCount()).isEqualTo(1);
        assertThat(render.lineNumber()).isGreaterThan(0);
    }

    // -----------------------------------------------------------------------
    // Relationships
    // -----------------------------------------------------------------------

    @Test
    void structFieldsAreExtractedWithTypesAndExportedness() throws IOException {
        GoCodeParser parser = new GoCodeParser(2);
        ParsedProject proj = parser.parseProject(SAMPLE_APP, PROJECT);
        ParsedRelationships rels = parser.buildRelationships(proj);

        assertThat(rels.fields())
                .extracting(f -> f.fieldFqn() + ":" + f.fieldType() + ":" + f.accessModifier())
                .contains(PKG_MODEL + ".User#Name:string:public",
                          PKG_MODEL + ".User#tags:map[string]string:private",
                          PKG_ROOT + ".Server#greeter:*service.Greeter:private");
    }

    @Test
    void groupedStructFieldsEachBecomeAField() throws IOException {
        GoCodeParser parser = new GoCodeParser(2);
        ParsedRelationships rels = parser.buildRelationships(parser.parseProject(SAMPLE_APP, PROJECT));
        // type pair struct { a, b int }
        assertThat(rels.fields()).extracting(ParsedRelationships.FieldDecl::fieldFqn)
                .contains(PKG_ROOT + ".pair#a", PKG_ROOT + ".pair#b");
    }

    @Test
    void embeddingProducesInheritsAndImplementsEdges() throws IOException {
        GoCodeParser parser = new GoCodeParser(2);
        ParsedRelationships rels = parser.buildRelationships(parser.parseProject(SAMPLE_APP, PROJECT));

        assertThat(rels.inherits())
                .extracting(e -> e.from() + " -> " + e.to())
                .contains(PKG_MODEL + ".User -> " + PKG_MODEL + ".Base",
                          PKG_SVC + ".Loud -> " + PKG_SVC + ".Greeter");
        assertThat(rels.implementsEdges())
                .extracting(e -> e.from() + " -> " + e.to())
                .contains(PKG_SVC + ".FormatterNamer -> " + PKG_SVC + ".Formatter");
    }

    @Test
    void takesAndReturnsResolveProjectTypesAcrossPackages() throws IOException {
        GoCodeParser parser = new GoCodeParser(2);
        ParsedRelationships rels = parser.buildRelationships(parser.parseProject(SAMPLE_APP, PROJECT));

        assertThat(rels.returns()).extracting(e -> e.from() + " -> " + e.to())
                .contains(PKG_ROOT + ".main#NewServer() -> " + PKG_ROOT + ".Server");
        // render(u model.User) — the qualifier resolves through main.go's imports.
        assertThat(rels.takes()).extracting(e -> e.from() + " -> " + e.to())
                .contains(PKG_ROOT + ".Server#render() -> " + PKG_MODEL + ".User");
    }

    // -----------------------------------------------------------------------
    // Tree walking
    // -----------------------------------------------------------------------

    @Test
    void vendorAndTestdataDirectoriesAreSkipped(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("go.mod"), "module example.com/x\n\ngo 1.22\n");
        Files.writeString(dir.resolve("main.go"), "package main\n\nfunc Real() {}\n");
        for (String skipped : List.of("vendor", "testdata", "node_modules")) {
            Files.createDirectories(dir.resolve(skipped));
            Files.writeString(dir.resolve(skipped).resolve("x.go"),
                    "package x\n\nfunc Hidden() {}\n");
        }

        ParsedProject proj = new GoCodeParser().parseProject(dir, "p");
        assertThat(proj.files()).hasSize(1);
        assertThat(methodFqns(proj)).containsExactly("example.com/x.main#Real()");
    }

    @Test
    void fileWithoutAPackageClauseIsSkipped(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("broken.go"), "// no package clause here\nfunc F() {}\n");
        assertThat(new GoCodeParser().parseProject(dir, "p").files()).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static List<ParsedClass> allClasses(ParsedProject p) {
        return p.files().stream().flatMap(f -> f.classes().stream()).toList();
    }

    private static List<String> methodFqns(ParsedProject p) {
        return p.allMethods().stream().map(ParsedMethod::fqn).toList();
    }

    private static ParsedClass classOf(List<ParsedClass> classes, String qualified) {
        Optional<ParsedClass> found = classes.stream()
                .filter(c -> c.qualifiedClassName().equals(qualified)).findFirst();
        assertThat(found).as("class %s", qualified).isPresent();
        return found.get();
    }

    private static ParsedMethod method(ParsedProject p, String fqn) {
        Optional<ParsedMethod> found = p.allMethods().stream()
                .filter(m -> m.fqn().equals(fqn)).findFirst();
        assertThat(found).as("method %s", fqn).isPresent();
        return found.get();
    }

    private static List<String> resolvedCallees(ParsedProject p, String callerFqn) {
        return method(p, callerFqn).calledMethods().stream()
                .filter(CallReference::resolved)
                .map(CallReference::calleeFqn)
                .toList();
    }

    private static Path sampleAppPath() {
        try {
            var url = GoCodeParserTest.class.getClassLoader()
                    .getResource("test-projects/sample-go-app");
            if (url == null) throw new IllegalStateException("sample-go-app fixture not on classpath");
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
