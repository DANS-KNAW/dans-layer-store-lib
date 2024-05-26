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

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;

public interface PersistenceProvider<E> {
    /**
     * Performs a saveOrUpdate in the current session.
     *
     * @param entity the entity to persist
     * @return the persisted entity
     */
    E persist(E entity);

    /**
     * Updates the entity in the current session.
     *
     * @param entity the entity to update
     */
    void update(E entity);

    /**
     * Deletes the entity from the current session.
     *
     * @param entity the entity to delete
     */
    void delete(E entity);

    /** 
     * Retrieves the entity with the given id from the current session.
     *
     * @param id the id of the entity to retrieve
     * @return the entity
     */
    E get(Long id);

    /**
     * Returns the CriteriaBuilder for the current session.
     *
     * @return the CriteriaBuilder
     */
    CriteriaBuilder getCriteriaBuilder();

    /**
     * Creates a query from the given CriteriaQuery.
     *
     * @param criteriaQuery the CriteriaQuery to create a query from
     * @return the query
     */
    <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery);

}