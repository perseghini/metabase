(ns metabase.sync.sync-metadata.metabase-metadata
  "Logic for syncing the special `metabase_metadata` table, which is a way for datasets
   such as the Sample Dataset to specific properties such as special types that should
   be applied during sync."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.models
             [field :refer [Field]]
             [table :refer [Table]]]
            [metabase.sync.fetch-metadata :as fetch-metadata :refer [DatabaseMetadataTable]]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.models.database.DatabaseInstance))

;; the _metabase_metadata table is a special table that can include Metabase metadata about the rest of the DB. This is used by the sample dataset

(s/defn ^:private ^:always-validate sync-metabase-metadata-table!
  "Databases may include a table named `_metabase_metadata` (case-insentive) which includes descriptions or other metadata about the `Tables` and `Fields`
   it contains. This table is *not* synced normally, i.e. a Metabase `Table` is not created for it. Instead, *this* function is called, which reads the data it
   contains and updates the relevant Metabase objects.

   The table should have the following schema:

     column  | type    | example
     --------+---------+-------------------------------------------------
     keypath | varchar | \"products.created_at.description\"
     value   | varchar | \"The date the product was added to our catalog.\"

   `keypath` is of the form `table-name.key` or `table-name.field-name.key`, where `key` is the name of some property of `Table` or `Field`.

   This functionality is currently only used by the Sample Dataset. In order to use this functionality, drivers must implement optional fn `:table-rows-seq`."
  [driver database :- DatabaseInstance, metabase-metadata-table :- DatabaseMetadataTable]
  (doseq [{:keys [keypath value]} (driver/table-rows-seq driver database metabase-metadata-table)]
    ;; TODO: this does not support schemas in dbs :(
    (let [[_ table-name field-name k] (re-matches #"^([^.]+)\.(?:([^.]+)\.)?([^.]+)$" keypath)]
      ;; ignore legacy entries that try to set field_type since it's no longer part of Field
      (when-not (= (keyword k) :field_type)
        (try (when-not (if field-name
                         (when-let [table-id (db/select-one-id Table
                                               ;; TODO: this needs to support schemas
                                               ;; TODO: eventually limit this to "core" schema tables
                                               :db_id  (:id database)
                                               :name   table-name
                                               :active true)]
                           (db/update-where! Field {:name     field-name
                                                    :table_id table-id}
                             (keyword k) value))
                         (db/update-where! Table {:name  table-name
                                                  :db_id (:id database)}
                           (keyword k) value))
               (log/error (u/format-color 'red "Error syncing _metabase_metadata: no matching keypath: %s" keypath)))
             (catch Throwable e
               (log/error (u/format-color 'red "Error in _metabase_metadata: %s" (.getMessage e)))))))))


(s/defn ^:always-validate is-metabase-metadata-table? :- s/Bool
  "Is this TABLE the special `_metabase_metadata` table?"
  [table :- DatabaseMetadataTable]
  (= "_metabase_metadata" (str/lower-case (:name table))))

(defn- maybe-sync-metabase-metadata-table!
  "Sync the `_metabase_metadata` table, a special table with Metabase metadata, if present.
   If per chance there were multiple `_metabase_metadata` tables in different schemas, just sync the first one we find."
  [database raw-tables]
  (when-let [metadata-table (first (filter is-metabase-metadata-table? raw-tables))]
    (sync-metabase-metadata-table! (driver/engine->driver (:engine database)) database metadata-table)))

(s/defn ^:always-validate sync-metabase-metadata! [database :- DatabaseInstance]
  (throw (UnsupportedOperationException.)))
