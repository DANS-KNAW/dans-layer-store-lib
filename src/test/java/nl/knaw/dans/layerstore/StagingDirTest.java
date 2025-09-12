package nl.knaw.dans.layerstore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StagingDirTest extends AbstractTestWithTestDir {

    @Test
    public void should_not_create_staging_dir() {
        var stagingDir = new StagingDir(testDir, 1234567890123L);
        assertThat(stagingDir.getPath()).doesNotExist();
        assertThat(stagingDir.getId()).isEqualTo(1234567890123L);
        assertThat(stagingDir.isStaged()).isFalse();
        assertThat(stagingDir.isClosed()).isTrue();
        assertThat(stagingDir.isOpen()).isFalse();
    }

}
