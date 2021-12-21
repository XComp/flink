/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.testutils.TestFileSystem;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code FileSystemBlobStoreTest} tests the {@link FileSystemBlobStore} implementation. */
public class FileSystemBlobStoreTest {

    private FileSystemBlobStore testInstance;
    private Path storagePath;

    @BeforeEach
    public void createTestInstance(@TempDir Path storagePath) throws IOException {
        this.testInstance = new FileSystemBlobStore(new TestFileSystem(), storagePath.toString());
        this.storagePath = storagePath;
    }

    public void finalizeTestInstance() throws IOException {
        testInstance.close();
    }

    @Test
    public void testSuccessfulPut() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("put");

        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);
        assertThat(getBlobDirectoryPath()).isEmptyDirectory();

        final boolean successfullyWritten =
                testInstance.put(temporaryFile.toFile(), jobId, blobKey);
        assertThat(successfullyWritten).isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId, blobKey)).isNotEmptyFile().hasSameTextualContentAs(temporaryFile);
    }

    @Test
    public void testMissingFilePut() throws IOException {
        assertThatThrownBy(
                        () ->
                                testInstance.put(
                                        new File("/not/existing/file"),
                                        new JobID(),
                                        new PermanentBlobKey()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    public void testSuccessfulGet() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("get");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();

        final Path targetFile = Files.createTempFile("filesystemblobstoretest-get-target-", "");
        assertThat(targetFile).isEmptyFile();
        final boolean successfullyGet = testInstance.get(jobId, blobKey, targetFile.toFile());
        assertThat(successfullyGet).isTrue();

        assertThat(targetFile).hasSameTextualContentAs(temporaryFile);
    }

    @Test
    public void testGetWithWrongJobId() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("get");
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), new JobID(), blobKey)).isTrue();

        assertThatThrownBy(
                        () ->
                                testInstance.get(
                                        new JobID(),
                                        blobKey,
                                        Files.createTempFile(
                                                        "filesystemblobstoretest-get-with-wrong-jobid-",
                                                        "")
                                                .toFile()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    public void testGetWithWrongBlobKey() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("get");

        final JobID jobId = new JobID();
        assertThat(testInstance.put(temporaryFile.toFile(), jobId, new PermanentBlobKey()))
                .isTrue();

        assertThatThrownBy(
                        () ->
                                testInstance.get(
                                        jobId,
                                        new PermanentBlobKey(),
                                        Files.createTempFile(
                                                        "filesystemblobstoretest-get-with-wrong-blobkey-",
                                                        "")
                                                .toFile()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    public void testSuccessfulDeleteOnlyBlob() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId, blobKey)).isNotEmptyFile();

        final boolean successfullyDeleted = testInstance.delete(jobId, blobKey);

        assertThat(successfullyDeleted).isTrue();
        assertThat(getPath(jobId)).doesNotExist();
    }

    @Test
    public void testSuccessfulDeleteBlob() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);
        final BlobKey otherBlobKey = new PermanentBlobKey();

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();
        // create another artifact to omit deleting the directory
        assertThat(testInstance.put(temporaryFile.toFile(), jobId, otherBlobKey)).isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId, blobKey)).isNotEmptyFile();
        assertThat(getPath(jobId, otherBlobKey)).isNotEmptyFile();

        final boolean successfullyDeleted = testInstance.delete(jobId, blobKey);

        assertThat(successfullyDeleted).isTrue();
        assertThat(getPath(jobId, otherBlobKey)).exists();
    }

    @Test
    public void testDeleteWithNotExistingJobId() {
        assertThat(testInstance.delete(new JobID(), new PermanentBlobKey())).isTrue();
    }

    @Test
    public void testDeleteWithNotExistingBlobKey() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();
        assertThat(testInstance.delete(jobId, new PermanentBlobKey())).isTrue();
        assertThat(getPath(jobId, blobKey)).exists();
    }

    @Test
    public void testDeleteAll() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, new PermanentBlobKey()))
                .isTrue();
        assertThat(testInstance.put(temporaryFile.toFile(), jobId, new PermanentBlobKey()))
                .isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId).toFile().listFiles()).hasSize(2);

        assertThat(testInstance.deleteAll(jobId)).isTrue();
        assertThat(getPath(jobId)).doesNotExist();
    }

    @Test
    public void testDeleteAllWithNotExistingJobId() {
        final JobID jobId = new JobID();
        assertThat(testInstance.deleteAll(jobId)).isTrue();
        assertThat(getPath(jobId)).doesNotExist();
    }

    private Path createTemporaryFileWithContent(String operationLabel) throws IOException {
        final String actualContent =
                String.format("Content for testing the %s operation", operationLabel);
        final Path temporaryFile =
                Files.createTempFile(
                        String.format("filesystemblobstoretest-%s-", operationLabel), "");
        try (BufferedWriter writer =
                new BufferedWriter(new FileWriter(temporaryFile.toAbsolutePath().toString()))) {
            writer.write(actualContent);
        }

        return temporaryFile;
    }

    private Path getBlobDirectoryPath() {
        return storagePath.resolve(FileSystemBlobStore.BLOB_PATH_NAME);
    }

    private Path getPath(JobID jobId) {
        return getBlobDirectoryPath().resolve(String.format("job_%s", jobId));
    }

    private Path getPath(JobID jobId, BlobKey blobKey) {
        return getPath(jobId).resolve(String.format("blob_%s", blobKey));
    }

    private BlobKey createPermanentBlobKeyFromFile(Path path) throws IOException {
        Preconditions.checkArgument(!Files.isDirectory(path));
        Preconditions.checkArgument(Files.exists(path));

        MessageDigest md = BlobUtils.createMessageDigest();
        try (InputStream is = new FileInputStream(path.toFile())) {
            final byte[] buf = new byte[1024];
            int bytesRead = is.read(buf);
            while (bytesRead >= 0) {
                md.update(buf, 0, bytesRead);
                bytesRead = is.read(buf);
            }

            return BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB, md.digest());
        }
    }
}
