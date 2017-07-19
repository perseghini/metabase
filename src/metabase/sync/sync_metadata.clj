(ns metabase.sync.sync-metadata
  "Logic responsible for syncing the metadata for an entire database.
   Delegates to different subtasks:

   1.  Sync tables (`metabase.sync.sync-metadata.tables`)
   2.  Sync fields (`metabase.sync.sync-metadata.fields`)
   3.  Sync FKs    (`metabase.sync.sync-metadata.fks`)
   4.  Sync Metabase Metadata table (`metabase.sync.sync-metadata.metabase-metadata`)"
  (:require [metabase.sync.sync-metadata
             [fields :as sync-fields]
             [fks :as sync-fks]
             [metabase-metadata :as metabase-metadata]
             [tables :as sync-tables]]
            [metabase.sync.util :as sync-util]
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate sync-db-metadata!
  [database :- DatabaseInstance]
  (sync-util/sync-operation :database-sync database (format "Sync metadata for %s" (sync-util/name-for-logging database))
    ;; Make sure the relevant table models are up-to-date
    (sync-tables/sync-tables! database)
    ;; Now for each table, sync the fields
    (sync-fields/sync-fields! database)
    ;; Now for each table, sync the FKS. This has to be done after syncing all the fields to make sure target fields exist
    (sync-fks/sync-fks! database)
    ;; finally, sync the metadata metadata table if it exists.
    (metabase-metadata/sync-metabase-metadata! database)))
