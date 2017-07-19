(ns metabase.sync.analyze
  "Logic responsible for doing deep 'analysis' of the data inside a database.
   This is significantly more expensive than the basic sync-metadata step, and involves things
   like running MBQL queries and fetching values to do things like determine Table row counts
   and infer field special types."
  (:require metabase.models.database
            [metabase.sync.analyze
             [table-row-count :as table-row-count]
             [special-types :as special-types]]
            [metabase.sync.util :as sync-util]
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate analyze-db!
  "Perform in-depth analysis on the data for all Tables in a given DATABASE.
   This is dependent on what each database driver supports, but includes things like cardinality testing and table row counting."
  [database :- DatabaseInstance]
  (sync-util/sync-operation :database-analyze database (format "Analyze data for %s" (sync-util/name-for-logging database))
    (table-row-count/update-table-row-counts! database)
    (special-types/infer-special-types! database)))
