(ns metabase.sync.sync-metadata.tables
  "Logic for updating Metabase Table models from metadata fetched from a physical DB."
  (:require metabase.models.database
            [metabase.sync.fetch-metadata :as fetch-metadata]
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate sync-tables!
  [database :- DatabaseInstance]
  (let [metadata (fetch-metadata/db-metadata database)]
    (throw (NoSuchMethodException.))))
