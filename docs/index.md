dans-layer-store-lib
====================

A library for hierarchical storage of files in a layered way.

## Layer store

A layer store is a hierarchical storage of files in a layered way. A layer contains a subset of the files and folders in the store. A layer is identified by a
unique Layer ID. This ID is a Unix timestamp in milliseconds. The layer IDs determine the order in which the layers are stacked. The contents of a layer store
can be transformed to a regular directory structure by stacking the layers in the order of their IDs. The layer with the highest ID is the top layer. Files in
newer layers override files with the same path in older layers.

An example of a layer store with three layers is shown below.

```text

     +-------------------------------------------------------+    
     |  Layer ID: 1706612162                                 |        
     |  .                                                    |        
     |  └── otherpath                                        |        
     |      └── to                                           |        
     |          └── otherdir                                 |        
     |              └── file3.txt                            |        
     +-------------------------------------------------------+        

     +-------------------------------------------------------+        
     |  Layer ID: 1705633401                                 |        
     |  .                                                    |        
     |  └── path                                             |        
     |      └── to                                           |        
     |          └── dir1                                     |        
     |              ├── file1.txt                            |        
     |              └── file2.txt                            |        
     +-------------------------------------------------------+        
                                                                      
     +-------------------------------------------------------+        
     |  Layer ID: 1701118823                                 |        
     |  .                                                    |        
     |  └── path                                             |        
     |      └── to                                           |        
     |          └── dir1                                     |        
     |              └── file1.txt                            |        
     +-------------------------------------------------------+        
```

Transforming this layer store to a regular directory structure results in the following directory structure:

```text
    +--------------------------------------------------------+         
    |  ├── otherpath                                         |         
    |  │   └── to                                            |         
    |  │       └── otherdir                                  |         
    |  │           └── file3.txt                             |         
    |  └── path                                              |         
    |      └── to                                            |         
    |          └── dir1                                      |         
    |              ├── file1.txt                             |         
    |              └── file2.txt                             |         
    +--------------------------------------------------------+         
```

Note that:

* `file1.txt` in layer `1705633401` overwrites `file1.txt` in layer `1701118823`.
* Adding files is done by adding them to the top layer.
* Removing files entails removing them from all layers (this is not shown in the example).

Layers are envisioned to be implemented as a set of archive files. A simple way to picture the transformation from layer store to directory structure is to
imagine the archive files being extracted to one temporary directory, one after the other, in the order of their layer ID.

## The `ItemStore` interface

The `dans-layer-store-lib` defines and implements an `ItemStore` interface. An `ItemStore` models basically a normal file/folder hierarchy. It provides methods
for adding, removing and retrieving items. In the scenario described above, it corresponds to the directory structure that results from transforming the layer
store to a regular directory structure.

The term **item** instead of file or folder is used to clearly distinguish between the model and the implementation, especially because the implementation will
involve "physical" files and folders.

Although `ItemStore` does not expose the concept of a layer, the purpose of this library is to provide a layered storage. The `ItemStore` interface is intended
to hide the layering from the user. It is not really intended to be implemented in other ways, although that is possible. The interface is geared towards
providing exactly the features needed to implement [ocfl-java]'s `Storage` interface without depending on any [OCFL] specific concepts. The two are connected
through the [dans-ocfl-java-extensions-lib].

## Layer database

The layer store is backed by a database. The purpose is to make operations that would otherwise be slow much faster. For example, listing the contents of a
folder in a layer store with many layers would require reading all layers. Depending on the type of archive, it could also mean reading all archive files. If
layer archives were stored on a very slow medium, such as tape, the problem would be compounded.

To solve this problem the layer database stores a record for each item in each layer. The record contains the item's path, and type. If it is a file the record
may also contain the entire content of the file. For which files the content is stored is configurable. Obviously, storing the content of all files would be
very expensive in terms of storage space, so it is recommended to only store the content of files that are expected to be relatively small and need to be read
often.

## Layer status

A layer can be in one of the following states. The state is composed of the following properties:

* open/closed — open means that the layer is still being written to. closed means that write operations are no longer allowed.
* staged — this means the layer is present in the staging area; if it is also closed, the name of the layer will end with `.closed`.
* archived — an archive has been created for the layer.

The following table shows the relationship between the states. It follows the normal lifecycle of a layer.

|   | open/closed | archived     | staged?                     | when?                          |
|:--|:------------|:-------------|:----------------------------|:-------------------------------|
| 1 | open        | not archived | yes                         | initial state of a top layer   |
| 2 | closed      | not archived | yes (with `.closed` suffix) | just before / during archiving |
| 3 | closed      | archived     | no                          | archiving succeeded            |
| 4 | open        | archived     | yes                         | reopened                       |
| 5 | closed      | archived     | yes (with `.closed` suffix) | closing a reopened layer       |

[OCFL]: https://ocfl.io/

[ocfl-java]: https://github.com/OCFL/ocfl-java

[dans-ocfl-java-extensions-lib]: https://github.com/DANS-KNAW/dans-ocfl-java-extensions-lib
