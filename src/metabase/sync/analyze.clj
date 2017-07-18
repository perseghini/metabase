(ns metabase.sync.analyze
  "Logic responsible for doing deep 'analysis' of the data inside a database.
   This is significantly more expensive than the basic sync-metadata step, and involves things
   like running MBQL queries and fetching values to do things like determine Table row counts
   and infer field special types."
  (:require metabase.models.database
            [metabase.sync.analyze
             [table-row-count :as table-row-count]
             [special-types :as special-types]]
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate analyze-db!
  [database :- DatabaseInstance]
  (table-row-count/update-table-row-counts! database)
  (special-types/infer-special-types! database))
