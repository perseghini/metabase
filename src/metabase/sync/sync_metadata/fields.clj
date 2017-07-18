(ns metabase.sync.sync-metadata.fields
  "Logic for updating Metabase Field models from metadata fetched from a physical DB."
  (:require [metabase.models
             database
             table]
            [metabase.sync
             [fetch-metadata :as fetch-metadata]
             [util :as sync-util]]
            [schema.core :as s])
  (:import metabase.models.table.TableInstance
           metabase.models.database.DatabaseInstance))

(s/defn ^:private ^:always-validate sync-fields-for-table!
  [database :- DatabaseInstance, table :- TableInstance]
  (let [metadata (fetch-metadata/table-metadata database table)]
    (throw (NoSuchMethodException.))))

(s/defn ^:always-validate sync-fields!
  [database :- DatabaseInstance]
  (doseq [table (sync-util/db->sync-tables database)]
    (sync-fields-for-table! database table)))
