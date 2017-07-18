(ns metabase.sync.sync-metadata.fks
  "Logic for updating FK properties of Fieldsfrom metadata fetched from a physical DB."
  (:require [metabase.models
             database
             table]
            [metabase.sync
             [fetch-metadata :as fetch-metadata]
             [util :as sync-util]]
            [schema.core :as s])
  (:import metabase.models.table.TableInstance
           metabase.models.database.DatabaseInstance))

(s/defn ^:private ^:always-validate sync-fks-for-table!
  [database :- DatabaseInstance, table :- TableInstance]
  (let [metadata (fetch-metadata/fk-metadata database table)]
    (throw (NoSuchMethodException.))))

(s/defn ^:always-validate sync-fks-for-table!
  [database :- DatabaseInstance]
  (doseq [table (sync-util/db->sync-tables database)]
    (sync-fks-for-table! database table)))
