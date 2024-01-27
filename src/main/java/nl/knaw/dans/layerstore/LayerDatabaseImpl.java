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

import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.SessionFactory;

import javax.persistence.TypedQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static nl.knaw.dans.layerstore.Item.Type;

// TODO: replace extending AbstractDAO with composition
public class LayerDatabaseImpl extends AbstractDAO<ItemRecord> implements LayerDatabase {

    public LayerDatabaseImpl(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    @Override
    public void saveRecords(ItemRecord... records) {
        for (var record : records) {
            // If the record has no generatedId, then it is new and we can persist it.
            if (record.getGeneratedId() == null) {
                persist(record);
            }
            // If the record has a generatedId, but it is not in the database, then it is new and we can persist it.
            else if (get(record.getGeneratedId()) == null) {
                persist(record);
            }
            else {
                // If the record has a generatedId, and it is in the database, then it is an existing record, and we must
                // merge the changes into the database.
                currentSession().merge(record);
            }
        }
    }

    @Override
    public void deleteRecordsById(long... id) {
        for (long i : id) {
            currentSession().delete(get(i));
        }
    }

    @Override
    public List<Item> listDirectory(String directoryPath) throws IOException {
        directoryPath = preprocessDirectoryArgument(directoryPath);
        // Treating the root directory as a special case, to prevent the root directory itself from being returned
        // as one of its own children.
        if (directoryPath.isEmpty()) {
            return namedTypedQuery("ItemRecord.listRootDirectory")
                .setParameter("pathWithTwoComponents", "%/%")
                .getResultList().stream().map(ItemRecord::toItem).toList();
        }
        return namedTypedQuery("ItemRecord.listDirectory")
            .setParameter("path", directoryPath + "%")
            .setParameter("pathWithTwoComponents", directoryPath + "%/%")
            .getResultList().stream().map(ItemRecord::toItem).toList();
    }

    @Override
    public List<Item> listRecursive(String directoryPath) throws IOException {
        return namedTypedQuery("ItemRecord.listRecursive")
            .setParameter("path", preprocessDirectoryArgument(directoryPath) + "%")
            .getResultList().stream().map(ItemRecord::toItem).toList();
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
                ItemRecord newRecord = new ItemRecord.Builder()
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

    private long getTopLayerId() {
        return namedTypedQuery("ItemRecord.getTopLayerId")
            .getSingleResult().getLayerId();
    }

    @Override
    public List<Long> findLayersContaining(String path) {
        return namedQuery("ItemRecord.findLayersContaining")
            .setParameter("path", path)
            .getResultList().stream().map(o -> (Long) o).toList();
    }

    @Override
    public List<ItemRecord> getRecordsByPath(String path) {
        return namedTypedQuery("ItemRecord.getRecordsByPath")
            .setParameter("path", path)
            .getResultList();
    }

    @Override
    public Stream<ItemRecord> getAllRecords() {
        TypedQuery<ItemRecord> query = namedTypedQuery("ItemRecord.getAllRecords");
        return query.getResultStream();
    }

    @Override
    public boolean hasPathLike(String pathPattern) {
        return namedQuery("ItemRecord.hasPathLike")
            .setParameter("pathPattern", pathPattern)
            .getResultList().stream().anyMatch(o -> (Boolean) o);
    }

    private String preprocessDirectoryArgument(String directoryPath) throws IOException {
        if (directoryPath == null) {
            throw new IllegalArgumentException("directoryPath must not be null");
        }

        if (!directoryPath.isBlank()) {
            var records = getRecordsByPath(directoryPath);
            if (records.isEmpty()) {
                throw new IllegalArgumentException("No such directory: " + directoryPath);
            }
            if (records.stream().anyMatch(r -> r.getType() != Type.Directory)) {
                throw new IllegalArgumentException("Not a directory: " + directoryPath);
            }
            // Add an ending slash to directoryPath, if it doesn't have one yet.
            if (!directoryPath.endsWith("/")) {
                directoryPath += "/";
            }
        }
        return directoryPath;
    }

}
