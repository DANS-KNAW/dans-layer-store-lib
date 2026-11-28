Design
======

<script>
window.addEventListener('load', function() {
    setColumnWidths("Part", "25%", "75%");
    setColumnWidths("Observation on storage", "25%", "75%");
});
</script>

This page documents the most important design decisions of the library, for future developers. It is a first draft; extend it as the design evolves.

## Overview

`LayeredItemStore` (the implementation of `ItemStore`) is a facade over three collaborating parts:

| Part                          | Responsibility                                                                                           |
|-------------------------------|----------------------------------------------------------------------------------------------------------|
| `LayerManager` / `Layer`      | Manage the stack of layers and their life cycle (staging directories and archives).                      |
| `LayerDatabase`               | An index with an `ItemRecord` for every file and directory in every layer. Used for listing and lookups. |
| `ArchiveProvider` / `Archive` | Create, read and write the archive (e.g. ZIP, TAR, DMF-TAR) for a layer.                                 |

The database exists because archives may live on media that are too slow for direct access (e.g. tape). Listing directories, checking existence and finding
the layer that holds the latest version of a path can therefore be answered without touching the archives.

## Layer state is derived from storage, not from the database

A layer is in one of the states `OPEN`, `CLOSED` or `ARCHIVED`. **This state is not stored in the database.** It is derived on every call to
`Layer.getState()` from what is actually present on storage:

| Observation on storage                       | State                                               |
|----------------------------------------------|-----------------------------------------------------|
| Staging directory `<id>` exists              | `OPEN`                                              |
| Staging directory `<id>.closed` exists       | `CLOSED`                                            |
| No staging directory, but the archive exists | `ARCHIVED`                                          |
| Neither staging directory nor archive exists | Error: inconsistent state (`IllegalStateException`) |

Rationale:

* **Single source of truth.** The files on storage are what actually matter. A separately stored state can get out of sync with them (crash between a file
  system operation and a database update, manual intervention, restore from backup). A derived state cannot.
* **Crash safety.** Every state transition is a single atomic file system operation: closing is a rename of `<id>` to `<id>.closed`; archiving ends with
  the removal of the staging directory after the archive has been written. There is no window in which "the state" and "the storage" disagree.
* **The database is an index, not a state store.** It only describes items, and its consistency with storage can be verified (see below).

Consequences for implementers:

* Do not cache the layer state. `LayerImpl.getState()` refreshes the `StagingDir` and asks the `ArchiveProvider` on every call (the `Archive` object's own
  "archived" flag may be stale if another instance for the same layer archived it).
* A layer is archived only when the archive exists **and** the staging directory is gone. Therefore the staging directory is deleted as the last step of
  archiving. While both exist, the layer is still considered `CLOSED`.
* Operations check the required state first (`checkState`) and are `synchronized` where a state change could otherwise race with the check.

## Layers and staging directories

* A layer ID is a Unix timestamp in milliseconds; the IDs define the stacking order. The layer with the highest ID is the top layer. Only the top layer is
  written to by normal operations. `newTopLayer()` waits a few milliseconds to avoid ID collisions, then closes and archives the previous top layer.
* The staging directory name encodes the staging state: `<id>` (open), `<id>.closed` (closed), `<id>.partial` (being unarchived for a `reopen`, not yet usable)
  and `<id>.deleting` (removal in progress; leftovers are removed on start-up because the layer was archived before deletion started).
* `StagingDir` validates that there is never more than one staging directory per layer ID, and the `LayerManagerImpl` constructor validates that the staging
  root contains nothing but correctly named layer directories (and, optionally, that the archive root contains only valid archives).
* `reopen()` of an archived layer unarchives into a `.partial` directory first and renames it to `<id>` only when complete; on failure the partial directory is
  deleted. Layers are thus never visible as open in a half-restored state. While the `.partial` directory exists, the layer still reports the ARCHIVED state
  and reads are served from the archive. A `.partial` directory found at the start of a `reopen()` is a leftover of an interrupted attempt (e.g., a crash) and
  is removed before unarchiving starts.
* `LayeredItemStore.reopenLayer(id)` refuses to reopen the top layer and refuses a second reopen of a layer that the same item store is already reopening.
  Callers in other processes must coordinate among themselves.
* An `Archive` may need to be recalled before it can be unarchived, for example from tape. `Archive.requestRecall()` starts the recall without waiting for it
  and `Archive.isRecalled()` reports when it is done; the defaults (do nothing, always recalled) fit archives that are always available. This lets a client
  wait for a recall that takes hours without blocking a thread in `reopen()`.

## Stack semantics

* Materializing the store means copying layers from oldest to newest; newer files overwrite older ones. Within `LayeredItemStore` this is implemented by the
  database: the first of `getRecordsByPath(path)` is the latest version, and its `layerId` tells which layer to read from.
* Writes go to the top layer only. Operations that must change files existing only in older layers (e.g. `moveDirectoryInternal`) fail if the source files are
  not all in the top layer (`checkAllSourceFilesOnlyInTopLayer`).
* Parent directories must exist in the store before a file is written; missing ancestors are created in both the top layer and the database.

## Database-backed content

Small files can optionally have a copy of their content in the database (`DatabaseBackedContentManager`), selected by path and optionally pre-processed (e.g.
compressed) before storage. `readFile` serves such content from the database, which avoids touching archives. `NoopDatabaseBackedContentManager` is
the default.

## Consistency checking

Because the database and storage are separate, they can diverge. The library therefore offers checks instead of trying to prevent every possible divergence:

* `checkSameLayersOnStorageAndDb()` compares the layer IDs found on storage with those known to the database (`LayerIdsMismatchException`).
* `checkLayerItemRecords(layerId)` compares the items in a layer on storage with the database records for that layer (`ItemsMismatchException`).

Since layer state lives only on storage, these checks never need to reconcile layer *state*, only layer and item *existence*.

Known limitation: file system changes and database updates in `LayeredItemStore` are not transactional (see the TODO in `writeFile`). A failed database update
after a successful file write leaves a discrepancy that the consistency checks will report.

## Archiving

* Archive formats are pluggable through `ArchiveProvider`/`Archive`. Implementations: ZIP, TAR and DMF-TAR (the latter runs `dmftar` locally or over SSH via the
  `AbstractRunner` implementations).
* Archiving can be done synchronously in the calling thread (`archiveDirectly`) or delegated to a `LayerArchiver` (typically a background executor) so that
  `newTopLayer()` returns quickly. Because state is derived from storage, a layer being archived in the background simply remains `CLOSED` until the archive
  is complete and the staging directory has been removed; no extra "archiving" state needs to be tracked.
* Archiving refuses to overwrite an existing archive unless `overwrite` is set. The check is made against the `ArchiveProvider` (i.e. against storage) rather
  than against flags on `Archive` objects.
