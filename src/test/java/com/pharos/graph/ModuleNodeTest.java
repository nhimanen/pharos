package com.pharos.graph;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ModuleNodeTest {

    @Test
    void externalFactory_createsExternalStatus() {
        ModuleNode node = ModuleNode.external("com.example", "my-lib", "1.0");

        assertThat(node.getStatus()).isEqualTo(ModuleNode.Status.EXTERNAL);
        assertThat(node.getGroupId()).isEqualTo("com.example");
        assertThat(node.getArtifactId()).isEqualTo("my-lib");
        assertThat(node.getVersion()).isEqualTo("1.0");
        assertThat(node.getProjectName()).isNull();
        assertThat(node.isIndexed()).isFalse();
    }

    @Test
    void indexedFactory_createsIndexedStatus() {
        ModuleNode node = ModuleNode.indexed("com.example", "my-lib", "1.0", "my-project");

        assertThat(node.getStatus()).isEqualTo(ModuleNode.Status.INDEXED);
        assertThat(node.getProjectName()).isEqualTo("my-project");
        assertThat(node.isIndexed()).isTrue();
    }

    @Test
    void moduleKey_isGroupIdColonArtifactId() {
        ModuleNode node = ModuleNode.external("com.example", "my-lib", "1.0");

        assertThat(node.getModuleKey()).isEqualTo("com.example:my-lib");
    }

    @Test
    void upgrade_promotesExternalToIndexed() {
        ModuleNode node = ModuleNode.external("com.example", "my-lib", "1.0");

        node.upgrade("2.0", "my-project");

        assertThat(node.getStatus()).isEqualTo(ModuleNode.Status.INDEXED);
        assertThat(node.getVersion()).isEqualTo("2.0");
        assertThat(node.getProjectName()).isEqualTo("my-project");
        assertThat(node.isIndexed()).isTrue();
    }

    @Test
    void upgrade_preservesExistingVersionWhenNewVersionIsNull() {
        ModuleNode node = ModuleNode.external("com.example", "my-lib", "1.0");

        node.upgrade(null, "my-project");

        assertThat(node.getVersion()).isEqualTo("1.0");
    }

    @Test
    void equals_basedOnModuleKeyOnly_sameArtifactDifferentVersionsAreEqual() {
        ModuleNode a = ModuleNode.external("com.example", "my-lib", "1.0");
        ModuleNode b = ModuleNode.external("com.example", "my-lib", "2.0");

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void equals_sameArtifactIndexedVsExternal_areEqual() {
        ModuleNode external = ModuleNode.external("com.example", "lib", "1.0");
        ModuleNode indexed  = ModuleNode.indexed("com.example", "lib", "1.0", "project");

        assertThat(external).isEqualTo(indexed);
    }

    @Test
    void equals_differentArtifactIds_areNotEqual() {
        ModuleNode a = ModuleNode.external("com.example", "lib-a", "1.0");
        ModuleNode b = ModuleNode.external("com.example", "lib-b", "1.0");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equals_differentGroupIds_areNotEqual() {
        ModuleNode a = ModuleNode.external("com.example", "lib", "1.0");
        ModuleNode b = ModuleNode.external("org.other",  "lib", "1.0");

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void indexedFactory_requiresNonNullProjectName() {
        assertThatNullPointerException()
                .isThrownBy(() -> ModuleNode.indexed("com.example", "lib", "1.0", null));
    }

    @Test
    void nullVersion_defaultsToUnknown() {
        ModuleNode node = ModuleNode.external("com.example", "lib", null);

        assertThat(node.getVersion()).isEqualTo("unknown");
    }

    @Test
    void toString_includesStatusAndKey() {
        ModuleNode node = ModuleNode.indexed("com.example", "lib", "1.0", "proj");

        assertThat(node.toString()).contains("com.example:lib");
        assertThat(node.toString()).contains("INDEXED");
        assertThat(node.toString()).contains("proj");
    }
}
