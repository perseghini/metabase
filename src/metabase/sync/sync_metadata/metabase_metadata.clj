(ns metabase.sync.sync-metadata.metabase-metadata
  "Logic for syncing the special `metabase_metadata` table, which is a way for datasets
   such as the Sample Dataset to specific properties such as special types that should
   be applied during sync."
  (:require metabase.models.database
            [metabase.sync.fetch-metadata :as fetch-metadata]
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate sync-metabase-metadata! [database :- DatabaseInstance]
  (throw (NoSuchMethodException.)))
