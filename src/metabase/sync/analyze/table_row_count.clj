(ns metabase.sync.analyze.table-row-count
  "Logic for updating a Table's row count by running appropriate MBQL queries."
  (:require [clojure.tools.logging :as log]
            [metabase.db.metadata-queries :as queries]
            metabase.models.database
            [metabase.models.table :refer [Table]]
            [metabase.sync.util :as sync-util]
            [metabase.util :as u]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.models.database.DatabaseInstance
           metabase.models.table.TableInstance))

(s/defn ^:private ^:always-validate table-row-count :- (s/maybe s/Int)
  "Determine the count of rows in TABLE by running a simple structured MBQL query."
  [table :- TableInstance]
  (try
    (queries/table-row-count table)
    (catch Throwable e
      (log/warn (u/format-color 'red "Unable to determine row count for '%s': %s\n%s" (:name table) (.getMessage e) (u/pprint-to-str (u/filtered-stacktrace e)))))))

(s/defn ^:private ^:always-validate update-row-count-for-table!
  [table :- TableInstance]
  (when-let [row-count (table-row-count table)]
    (log/debug (format "Set table row count for '%s' to %d" (:name table) row-count))
    (db/update! Table (u/get-id table)
      :rows row-count)))

(s/defn ^:always-validate update-table-row-counts!
  [database :- DatabaseInstance]
  (doseq [table (sync-util/db->sync-tables database)]
    (update-row-count-for-table! table)))