(ns metabase.sync.field-values
  "Logic for updating cached FieldValues for fields in a database."
  (:require [clojure.tools.logging :as log]
            [metabase.models.field-values :as field-values]
            [metabase.sync
             [interface :as i]
             [util :as sync-util]]
            [schema.core :as s]))

;; TODO - these should probably be moved to the `field-values` code in `metabase.models.field-values`, and used everywhere it's appropriate

(def ^:private ^:const ^Integer field-values-entry-max-length
  "The maximum character length for a stored `FieldValues` entry."
  100)

(def ^:private ^:const ^Integer field-values-total-max-length
  "Maximum total length for a `FieldValues` entry (combined length of all values for the field)."
  (* i/low-cardinality-threshold field-values-entry-max-length))



(s/defn ^:always-validate update-field-values! [database :- i/DatabaseInstance]
  (sync-util/sync-operation :cache-field-values database (format "Cache field values in %s" (sync-util/name-for-logging database))
    (doseq [table (sync-util/db->sync-tables database)
            field (sync-util/table->sync-fields table)
            :when (field-values/field-should-have-field-values? field)]
      (sync-util/with-error-handling (format "Error updating field values for %s" (sync-util/name-for-logging field))
        (log/debug (format "Updating field values for %s" (sync-util/name-for-logging field)))
        (field-values/update-field-values! field)))))
