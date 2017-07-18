(ns metabase.sync.analyze.table-row-count
  "Logic for updating a Table's row count by running appropriate MBQL queries."
  (:require metabase.models.database
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate update-table-row-counts!
  [database :- DatabaseInstance]
  (throw (NoSuchMethodException.)))
