(ns metabase.sync
  "Combined functions for running the entire Metabase sync process.
   This delegates to a few distinct steps, which in turn are broken out even further:

   1.  Sync Metadata      (`metabase.sync.sync-metadata`)
   2.  Analysis           (`metabase.sync.analyze`)
   3.  Cache Field Values (`metabase.sync.field-values`)"
  (:require metabase.models.database
            [metabase.sync
             [analyze :as analyze]
             [field-values :as field-values]
             [sync-metadata :as sync-metadata]]
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

;; TODO - add sync-options param?
(s/defn ^:always-validate sync-database!
  [database :- DatabaseInstance]
  ;; TODO - use sync-in-context?
  ;; First make sure Tables, Fields, and FK information is up-to-date
  (sync-metadata/sync-db-metadata! database)
  ;; Next, run the 'analysis' step where we do things like scan values of fields and update special types accordingly
  (analyze/analyze-db! database)
  ;; Finally, update FieldValues
  (field-values/update-field-values! database))
