(ns metabase.sync
  "Combined functions for running the entire Metabase sync process.
   This delegates to a few distinct steps, which in turn are broken out even further:

   1.  Sync Metadata      (`metabase.sync.sync-metadata`)
   2.  Analysis           (`metabase.sync.analyze`)
   3.  Cache Field Values (`metabase.sync.field-values`)"
  (:require [metabase.sync
             [analyze :as analyze]
             [field-values :as field-values]
             [interface :as i]
             [sync-metadata :as sync-metadata]]
            [schema.core :as s]))

(def ^:private SyncDatabaseOptions
  {(s/optional-key :full-sync?) s/Bool})

(s/defn ^:always-validate sync-database!
  ([database]
   (sync-database! database {:full-sync? true}))
  ([database :- i/DatabaseInstance, options :- SyncDatabaseOptions]
   ;; First make sure Tables, Fields, and FK information is up-to-date
   (sync-metadata/sync-db-metadata! database)
   (when (:full-sync? options)
     ;; Next, run the 'analysis' step where we do things like scan values of fields and update special types accordingly
     (analyze/analyze-db! database)
     ;; Finally, update FieldValues
     (field-values/update-field-values! database))))


(s/defn ^:always-validate sync-table!
  [table :- i/TableInstance]
  (throw (UnsupportedOperationException.)))
