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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * A record in the database that represents a file or directory in a layer.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity(name = "listing_record")
public class ItemRecord {
    @Id
    @Column(name = "generated_id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long generatedId;

    @Column(name = "layer_id", nullable = false)
    private Long layerId;

    @Column(nullable = false)
    private String path;

    @Column(nullable = false)
    private Item.Type type;

    @Column
    @Lob
    private byte[] content;

    public Item toItem() {
        return new Item(path, type);
    }
}
