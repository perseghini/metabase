(ns metabase.sync.fetch-metadata
  "Fetch metadata functions fetch 'snapshots' of the schema for a data warehouse database, including
   information about tables, schemas, and fields, and their types.
   For example, with SQL databases, these functions use the JDBC DatabaseMetaData to get this information."
  (:require [metabase.driver :as driver]
            [metabase.sync.interface :as i]
            [schema.core :as s]))

(s/defn ^:always-validate db-metadata :- i/DatabaseMetadata
  "Get basic Metadata about a Database and its Tables. Doesn't include information about the fields."
  [database :- i/DatabaseInstance]
  (driver/describe-database (driver/->driver database) database))

(s/defn ^:always-validate table-metadata :- i/TableMetadata
  [database :- i/DatabaseInstance, table :- i/TableInstance]
  (driver/describe-table (driver/->driver database) database table))

(s/defn ^:always-validate fk-metadata :- i/FKMetadata
  [database :- i/DatabaseInstance, table :- i/TableInstance]
  (let [driver (driver/->driver database)]
    (when (driver/driver-supports? driver :foreign-keys)
      (driver/describe-table-fks driver database table))))
