(ns metabase.sync.sync-metadata.fks
  "Logic for updating FK properties of Fields from metadata fetched from a physical DB."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase.models
             [field :refer [Field]]
             [table :as table :refer [Table]]]
            [metabase.sync
             [fetch-metadata :as fetch-metadata]
             [interface :as i]
             [util :as sync-util]]
            [metabase.util :as u]
            [schema.core :as s]
            [toucan.db :as db]))

(s/defn ^:private ^:always-validate mark-fk!
  [database :- i/DatabaseInstance, table :- i/TableInstance, fk :- i/FKMetadataEntry]
  (let [source-field (db/select-one Field
                       :table_id           (u/get-id table)
                       :%lower.name        (str/lower-case (:fk-column-name fk))
                       :fk_target_field_id nil
                       :active             true
                       :visibility_type    [:not= "retired"])
        dest-table   (when source-field
                       (db/select-one Table
                         :db_id           (u/get-id database)
                         :%lower.name     (str/lower-case (-> fk :dest-table :name))
                         :%lower.schema   (when-let [schema (-> fk :dest-table :schema)]
                                            (str/lower-case schema))
                         :active          true
                         :visibility_type nil))
        dest-field   (when dest-table
                       (db/select-one Field
                         :table_id           (u/get-id dest-table)
                         :%lower.name        (str/lower-case (:dest-column-name fk))
                         :active             true
                         :visibility_type    [:not= "retired"]))]
    (when (and source-field dest-table dest-field)
      (log/debug (u/format-color 'cyan "Marking foreign key from %s %s -> %s %s"
                   (sync-util/name-for-logging table)
                   (sync-util/name-for-logging source-field)
                   (sync-util/name-for-logging dest-table)
                   (sync-util/name-for-logging dest-field)))
      (db/update! Field (u/get-id source-field)
        :special_type       :type/FK
        :fk_target_field_id (u/get-id dest-field)))))


(s/defn ^:always-validate sync-fks-for-table!
  ([table :- i/TableInstance]
   (sync-fks-for-table! (table/database table) table))
  ([database :- i/DatabaseInstance, table :- i/TableInstance]
   (sync-util/with-error-handling (format "Error syncing FKs for %s" (sync-util/name-for-logging table))
     (doseq [fk (fetch-metadata/fk-metadata database table)]
       (mark-fk! database table fk)))))

(s/defn ^:always-validate sync-fks!
  [database :- i/DatabaseInstance]
  (doseq [table (sync-util/db->sync-tables database)]
    (sync-fks-for-table! database table)))
