(ns metabase.sync.sync-metadata.fields
  "Logic for updating Metabase Field models from metadata fetched from a physical DB."
  (:require [clojure
             [data :as data]
             [set :as set]]
            [clojure.tools.logging :as log]
            [metabase.models
             [field :refer [Field]]
             [humanization :as humanization]]
            [metabase.sync
             [fetch-metadata :as fetch-metadata]
             [util :as sync-util]]
            [metabase.util :as u]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.models.database.DatabaseInstance
           metabase.models.table.TableInstance))

(defn- update-field-from-field-def!
  "Update an EXISTING-FIELD from the given FIELD-DEF."
  {:arglists '([existing-field field-def])}
  [{:keys [id], :as existing-field} {field-name :name, :keys [base-type special-type pk? parent-id]}]
  (u/prog1 (assoc existing-field
             :base_type    base-type
             :display_name (or (:display_name existing-field)
                               (humanization/name->human-readable-name field-name))
             :special_type (or (:special_type existing-field)
                               special-type
                               (when pk?
                                 :type/PK))
             :parent_id    parent-id)
    ;; if we have a different base-type or special-type, then update
    (when (first (data/diff <> existing-field))
      (db/update! Field id
        :display_name (:display_name <>)
        :base_type    base-type
        :special_type (:special_type <>)
        :parent_id    parent-id))))

(defn- create-field-from-field-def!
  "Create a new `Field` from the given FIELD-DEF."
  {:arglists '([table-id field-def])}
  [table-id {field-name :name, :keys [base-type special-type pk? parent-id raw-column-id]}]
  {:pre [(integer? table-id) (string? field-name) (isa? base-type :type/*)]}
  (let [special-type (or special-type
                       (when pk? :type/PK))]
    (db/insert! Field
      :table_id      table-id
      :raw_column_id raw-column-id
      :name          field-name
      :display_name  (humanization/name->human-readable-name field-name)
      :base_type     base-type
      :special_type  special-type
      :parent_id     parent-id)))

(defn- save-nested-fields!
  "Save any nested `Fields` for a given parent `Field`.
   All field-defs provided are assumed to be children of the given FIELD."
  [{parent-id :id, table-id :table_id, :as parent-field} nested-field-defs]
  ;; NOTE: remember that we never retire any fields in dynamic-schema tables
  (let [existing-field-name->field (u/key-by :name (db/select Field, :parent_id parent-id))]
    (u/prog1 (set/difference (set (map :name nested-field-defs)) (set (keys existing-field-name->field)))
      (when (seq <>)
        (log/debug (u/format-color 'blue "Found new nested fields for field '%s': %s" (:name parent-field) <>))))

    (doseq [nested-field-def nested-field-defs]
      (let [{:keys [nested-fields], :as nested-field-def} (assoc nested-field-def :parent-id parent-id)]
        ;; NOTE: this recursively creates fields until we hit the end of the nesting
        (if-let [existing-field (existing-field-name->field (:name nested-field-def))]
          ;; field already exists, so we UPDATE it
          (cond-> (update-field-from-field-def! existing-field nested-field-def)
                  nested-fields (save-nested-fields! nested-fields))
          ;; looks like a new field, so we CREATE it
          (cond-> (create-field-from-field-def! table-id nested-field-def)
                  nested-fields (save-nested-fields! nested-fields)))))))

(s/defn ^:private ^:always-validate sync-fields-for-table!
  [database :- DatabaseInstance, table :- TableInstance]
  (let [metadata (fetch-metadata/table-metadata database table)]
    (throw (NoSuchMethodException.))))

(s/defn ^:always-validate sync-fields!
  [database :- DatabaseInstance]
  (doseq [table (sync-util/db->sync-tables database)]
    (sync-fields-for-table! database table)))
