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

#_(log/debug (u/format-color 'cyan "Marking foreign key '%s.%s' -> '%s.%s'." (table-name-for-logging table) fk-column-name (table-name-for-logging dest-table) dest-column-name))

#_(defn- save-fks!
  "Update all of the FK relationships present in DATABASE based on what's captured in the raw schema.
   This will set :special_type :type/FK and :fk_target_field_id <field-id> for each found FK relationship.
   NOTE: we currently overwrite any previously defined metadata when doing this."
  [fk-sources]
  {:pre [(coll? fk-sources)
         (every? map? fk-sources)]}
  (doseq [{fk-source-id :source-column, fk-target-id :target-column} fk-sources]
    ;; TODO: eventually limit this to just "core" schema tables
    (when-let [source-field-id (db/select-one-id Field, :raw_column_id fk-source-id, :visibility_type [:not= "retired"])]
      (when-let [target-field-id (db/select-one-id Field, :raw_column_id fk-target-id, :visibility_type [:not= "retired"])]
        (db/update! Field source-field-id
          :special_type       :type/FK
          :fk_target_field_id target-field-id)))))

(s/defn ^:private ^:always-validate sync-fks-for-table!
  [database :- DatabaseInstance, table :- TableInstance]
  (let [metadata (fetch-metadata/fk-metadata database table)]
    (throw (NoSuchMethodException.))))

(s/defn ^:always-validate sync-fks!
  [database :- DatabaseInstance]
  (doseq [table (sync-util/db->sync-tables database)]
    (sync-fks-for-table! database table)))
