(ns metabase.sync-database
  "The logic for doing DB and Table syncing itself."
  (:require [clojure.tools.logging :as log]
            [metabase
             [driver :as driver]
             [events :as events]
             [util :as u]]
            [metabase.models
             [raw-table :as raw-table]
             [table :as table]]
            [metabase.query-processor.interface :as i]
            [metabase.sync.util :as sync-util]
            [metabase.sync-database
             [analyze :as analyze]
             [introspect :as introspect]
             [sync :as sync]
             [sync-dynamic :as sync-dynamic]]
            [toucan.db :as db]))

;;; ------------------------------------------------------------ Sync Database ------------------------------------------------------------

(defn- -sync-database! [driver database full-sync?]
  (sync-util/with-start-and-finish-logging (format "Sync %s database '%s'" (name driver) (:name database))
    (sync-util/with-sync-events :database-sync (u/get-id database)
      (sync-util/with-db-logging-disabled
        ;; start with capturing a full introspection of the database
        (introspect/introspect-database-and-update-raw-tables! driver database)
        ;; use the introspected schema information and update our working data models
        (if (driver/driver-supports? driver :dynamic-schema)
          (sync-dynamic/scan-database-and-update-data-model! driver database)
          (sync/update-data-models-from-raw-tables! database))
        ;; now do any in-depth data analysis which requires querying the tables (if enabled)
        (when full-sync?
          (analyze/analyze-data-shape-for-tables! driver database))))))

(defn sync-database!
  "Sync DATABASE and all its Tables and Fields.

   Takes an optional kwarg `:full-sync?` which determines if we execute our table analysis work.  If this is not specified
   then we default to using the `:is_full_sync` attribute of the database."
  [database & {:keys [full-sync?]}]
  {:pre [(map? database)]}
  (sync-util/with-duplicate-ops-prevented :sync-database (u/get-id database)
    (let [driver     (driver/engine->driver (:engine database))
          full-sync? (if-not (nil? full-sync?)
                       full-sync?
                       (:is_full_sync database))]
      (driver/sync-in-context driver database (partial -sync-database! driver database full-sync?)))))


;;; ------------------------------------------------------------ Sync Table ------------------------------------------------------------

(defn- -sync-table! [driver database table full-sync?]
  (sync-util/with-start-and-finish-logging (format "Sync table '%s' from %s database '%s'" (:display_name table) (name driver) (:name database))
    (sync-util/with-db-logging-disabled
      ;; if the Table has a RawTable backing it then do an introspection and sync
      (when-let [raw-table (raw-table/RawTable (:raw_table_id table))]
        (introspect/introspect-raw-table-and-update! driver database raw-table)
        (sync/update-data-models-for-table! table))
      ;; if this table comes from a dynamic schema db then run that sync process now
      (when (driver/driver-supports? driver :dynamic-schema)
        (sync-dynamic/scan-table-and-update-data-model! driver database table))
      ;; analyze if we are supposed to
      (when full-sync?
        (analyze/analyze-table-data-shape! driver table)))
    (events/publish-event! :table-sync {:table_id (:id table)})))

(defn sync-table!
  "Sync a *single* TABLE and all of its Fields.
   This is used *instead* of `sync-database!` when syncing just one Table is desirable.

   Takes an optional kwarg `:full-sync?` which determines if we execute our table analysis work.  If this is not specified
   then we default to using the `:is_full_sync` attribute of the tables parent database."
  [table & {:keys [full-sync?]}]
  {:pre [(map? table)]}
  (let [database   (table/database table)
        driver     (driver/engine->driver (:engine database))
        full-sync? (if-not (nil? full-sync?)
                     full-sync?
                     (:is_full_sync database))]
    (driver/sync-in-context driver database (partial -sync-table! driver database table full-sync?))))
