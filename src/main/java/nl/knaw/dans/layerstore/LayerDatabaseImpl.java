/*
 * Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.layerstore;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.util.PersistenceProvider;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static nl.knaw.dans.layerstore.Item.Type;

@AllArgsConstructor
@Slf4j
public class LayerDatabaseImpl implements LayerDatabase {
    private PersistenceProvider<ItemRecord> persistenceProvider;

    @Override
    public void saveRecords(ItemRecord... records) {
        for (var record : records) {
            // If the record has no generatedId, then it is new and we can persist it.
            if (record.getGeneratedId() == null) {
                persistenceProvider.persist(record);
            }
            // If the record has a generatedId, but it is not in the database, then it is new and we can persist it.
            else if (persistenceProvider.get(record.getGeneratedId()) == null) {
                persistenceProvider.persist(record);
            }
            else {
                // If the record has a generatedId, and it is in the database, then it is an existing record, and we must
                // merge the changes into the database.
                persistenceProvider.update(record);
            }
        }
    }

    @Override
    public void deleteRecordsById(long... id) {
        for (long i : id) {
            persistenceProvider.delete(persistenceProvider.get(i));
        }
    }

    @Override
    public List<Item> listDirectory(String directoryPath) throws IOException {
        directoryPath = preprocessDirectoryArgument(directoryPath);
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<ItemRecord> cq = cb.createQuery(ItemRecord.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        Predicate hasParentPath = cb.like(itemRecordRoot.get("path"), directoryPath + "%");
        Predicate notSubdirectory = cb.notLike(itemRecordRoot.get("path"), directoryPath + "%/%");
        Predicate notSamePath = cb.notEqual(itemRecordRoot.get("path"), directoryPath);
        Predicate hasMaxLayerId = cb.equal(itemRecordRoot.get("layerId"), getMaxLayerIdSubquery(cq, cb, itemRecordRoot));
        cq.where(cb.and(hasParentPath, notSubdirectory, notSamePath, hasMaxLayerId));

        TypedQuery<ItemRecord> query = persistenceProvider.createQuery(cq);
        return query.getResultStream().map(ItemRecord::toItem).collect(Collectors.toList());
    }

    @Override
    public List<Item> listRecursive(String directoryPath) throws IOException {
        directoryPath = preprocessDirectoryArgument(directoryPath);
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<ItemRecord> cq = cb.createQuery(ItemRecord.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        Predicate hasParentPath = cb.like(itemRecordRoot.get("path"), directoryPath + "%");
        Predicate notSamePath = cb.notEqual(itemRecordRoot.get("path"), directoryPath);
        Predicate hasMaxLayerId = cb.equal(itemRecordRoot.get("layerId"), getMaxLayerIdSubquery(cq, cb, itemRecordRoot));
        cq.where(cb.and(hasParentPath, notSamePath, hasMaxLayerId));

        TypedQuery<ItemRecord> query = persistenceProvider.createQuery(cq);
        return query.getResultStream().map(ItemRecord::toItem).collect(Collectors.toList());
    }

    private static Subquery<Long> getMaxLayerIdSubquery(CriteriaQuery<ItemRecord> cq, CriteriaBuilder cb, Root<ItemRecord> itemRecordRoot) {
        Subquery<Long> subquery = cq.subquery(Long.class);
        Root<ItemRecord> subRoot = subquery.from(ItemRecord.class);
        subquery.select(cb.max(subRoot.get("layerId"))).where(cb.equal(subRoot.get("path"), itemRecordRoot.get("path")));
        return subquery;
    }

    @Override
    public List<ItemRecord> addDirectory(long layerId, String path) {
        String[] pathComponents = getPathComponents(path);
        String currentPath = "";
        List<ItemRecord> newRecords = new ArrayList<>();

        for (String component : pathComponents) {
            currentPath = currentPath.isEmpty() ? component : currentPath + "/" + component;
            List<ItemRecord> records = getRecordsByPath(currentPath);
            if (records.stream().anyMatch(r -> r.getType() == Type.File)) {
                throw new IllegalArgumentException("Cannot add directory " + records.get(0).getPath() + " because it is already occupied by a file.");
            }
            var recordsInLayer = records.stream().filter(r -> r.getLayerId() == layerId).toList();
            if (recordsInLayer.isEmpty()) {
                ItemRecord newRecord = ItemRecord.builder()
                    .layerId(layerId)
                    .path(currentPath)
                    .type(Type.Directory)
                    .build();
                newRecords.add(newRecord);
                saveRecords(newRecord);
            }
        }
        return newRecords;
    }

    private String[] getPathComponents(String path) {
        String[] pathComponentsWithoutRoot = path.split("/");
        String[] pathComponents = new String[pathComponentsWithoutRoot.length + 1];
        pathComponents[0] = "";
        System.arraycopy(pathComponentsWithoutRoot, 0, pathComponents, 1, pathComponentsWithoutRoot.length);
        return pathComponents;
    }

    @Override
    public List<Long> findLayersContaining(String path) {
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<Long> cq = cb.createQuery(Long.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        cq.select(itemRecordRoot.get("layerId")).where(cb.equal(itemRecordRoot.get("path"), path)).distinct(true);
        TypedQuery<Long> query = persistenceProvider.createQuery(cq);
        return query.getResultList();
    }

    @Override
    public List<ItemRecord> getRecordsByPath(String path) {
        log.debug("getRecordsByPath({})", path);
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<ItemRecord> cq = cb.createQuery(ItemRecord.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        cq.select(itemRecordRoot).where(cb.equal(itemRecordRoot.get("path"), path));
        cq.orderBy(cb.desc(itemRecordRoot.get("layerId")));
        TypedQuery<ItemRecord> query = persistenceProvider.createQuery(cq);
        return query.getResultList();
    }

    @Override
    public Stream<ItemRecord> getAllRecords() {
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<ItemRecord> cq = cb.createQuery(ItemRecord.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        cq.select(itemRecordRoot);

        TypedQuery<ItemRecord> query = persistenceProvider.createQuery(cq);
        return query.getResultStream();
    }

    @Override
    public boolean existsPathLike(String pathPattern) {
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<Boolean> cq = cb.createQuery(Boolean.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        cq.select(cb.literal(true)).where(cb.like(itemRecordRoot.get("path"), pathPattern));
        TypedQuery<Boolean> query = persistenceProvider.createQuery(cq);
        return query.getResultStream().findFirst().orElse(false);
    }

    @Override
    public List<Long> listLayerIds() {
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<Long> cq = cb.createQuery(Long.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        cq.select(itemRecordRoot.get("layerId")).distinct(true);
        TypedQuery<Long> query = persistenceProvider.createQuery(cq);
        return query.getResultList();
    }

    @Override
    public List<ItemRecord> getRecordsByLayerId(long layerId) {
        CriteriaBuilder cb = persistenceProvider.getCriteriaBuilder();
        CriteriaQuery<ItemRecord> cq = cb.createQuery(ItemRecord.class);
        Root<ItemRecord> itemRecordRoot = cq.from(ItemRecord.class);
        cq.select(itemRecordRoot).where(cb.equal(itemRecordRoot.get("layerId"), layerId));
        TypedQuery<ItemRecord> query = persistenceProvider.createQuery(cq);
        return query.getResultList();
    }

    private String preprocessDirectoryArgument(String directoryPath) throws NoSuchFileException, NotDirectoryException {
        if (directoryPath == null) {
            throw new IllegalArgumentException("directoryPath must not be null");
        }

        if (!directoryPath.isBlank()) {
            var records = getRecordsByPath(directoryPath);
            if (records.isEmpty()) {
                throw new NoSuchFileException("No such directory: " + directoryPath);
            }
            if (records.stream().anyMatch(r -> r.getType() != Type.Directory)) {
                throw new NotDirectoryException("Not a directory: " + directoryPath);
            }
            // Add an ending slash to directoryPath, if it doesn't have one yet.
            if (!directoryPath.endsWith("/")) {
                directoryPath += "/";
            }
        }
        return directoryPath;
    }

}
