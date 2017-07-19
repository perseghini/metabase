(ns metabase.sync.analyze.special-types
  "Logic for scanning values of a given field and updating special types as appropriate.
   Also known as 'fingerprinting', 'analysis', or 'classification'."
  (:require [clojure.tools.logging :as log]
            [metabase.models.field :refer [Field]]
            [metabase.sync.analyze.special-types
             [name :as name]
             [values :as values]]
            [metabase.sync.util :as sync-util]
            [metabase.util :as u]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.models.database.DatabaseInstance
           metabase.models.field.FieldInstance
           metabase.models.table.TableInstance))

(def ^:const ^Integer low-cardinality-threshold
  "Fields with less than this many distinct values should automatically be given a special type of `:type/Category`."
  300)

(s/defn ^:private ^:always-validate update-fields-last-analyzed!
  "Update the `last_analyzed` date for all the fields in TABLE."
  [table :- TableInstance]
  (db/update-where! Field {:table_id        (u/get-id table)
                           :active          true
                           :visibility_type [:not= "retired"]}
    :last_analyzed (u/new-sql-timestamp)))

(s/defn ^:private ^:always-validate fields-to-infer-special-types-for :- (s/maybe [FieldInstance])
  [table :- TableInstance]
  (seq (db/select Field
         :table_id        (u/get-id table)
         :special_type    nil
         :active          true
         :visibility_type [:not= "retired"])))

(s/defn ^:private ^:always-validate infer-special-types-for-table!
  [table :- TableInstance]
  ;; fetch any fields with no special type. See if we can infer a type from their name.
  (when-let [fields (fields-to-infer-special-types-for table)]
    (name/infer-special-types-by-name! table fields))
  ;; Ok, now fetch fields that *still* don't have a special type. Try to infer a type from a sequence of their values.
  (when-let [fields (fields-to-infer-special-types-for table)]
    (values/infer-special-types-by-value! table fields))
  ;; Ok, now let's mark all the fields as having been recently analyzed
  (update-fields-last-analyzed! table))


(s/defn ^:always-validate infer-special-types!
  [database :- DatabaseInstance]
  (let [tables (sync-util/db->sync-tables database)]
    (sync-util/with-emoji-progress-bar [emoji-progress-bar (count tables)]
      (doseq [table tables]
        (infer-special-types-for-table! table)
        (log/info (u/format-color 'blue "%s Analyzed %s" (emoji-progress-bar) (sync-util/name-for-logging table)))))))
