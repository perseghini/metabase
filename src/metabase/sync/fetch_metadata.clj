(ns metabase.sync.fetch-metadata
  "Fetch metadata functions fetch 'snapshots' of the schema for a data warehouse database, including
   information about tables, schemas, and fields, and their types.
   For example, with SQL databases, these functions use the JDBC DatabaseMetaData to get this information."
  (:require [metabase.models
             database
             table]
            [metabase.util.schema :as su]
            [schema.core :as s]
            [metabase.driver :as driver])
  (:import metabase.models.database.DatabaseInstance
           metabase.models.table.TableInstance))

(def DatabaseMetadataTable
  "Schema for the expected output of `describe-database` for a Table."
  {:name   su/NonBlankString
   :schema (s/maybe su/NonBlankString)})

(def DatabaseMetadata
  "Schema for the expected output of `describe-database`."
  {:tables #{DatabaseMetadataTable}})


(def TableMetadataField
  "Schema for a given Field as provided in `describe-table`."
  {:name                           su/NonBlankString
   :base-type                      su/FieldType
   (s/optional-key :special-type)  (s/maybe su/FieldType)
   (s/optional-key :pk?)           s/Bool
   (s/optional-key :nested-fields) #{(s/recursive #'TableMetadataField)}
   (s/optional-key :custom)        {s/Any s/Any}})

(def TableMetadata
  "Schema for the expected output of `describe-table`."
  {:name   su/NonBlankString
   :schema (s/maybe su/NonBlankString)
   :fields #{TableMetadataField}})


(def FKMetadata
  "Schema for the expected output of `describe-table-fks`."
  (s/maybe #{{:fk-column-name   su/NonBlankString
              :dest-table       {:name   su/NonBlankString
                                 :schema (s/maybe su/NonBlankString)}
              :dest-column-name su/NonBlankString}}))


(s/defn ^:always-validate db-metadata :- DatabaseMetadata
  "Get basic Metadata about a Database and its Tables. Doesn't include information about the fields."
  [database :- DatabaseInstance]
  (driver/describe-database (driver/->driver database) database))

(s/defn ^:always-validate table-metadata :- TableMetadata
  [database :- DatabaseInstance, table :- TableInstance]
  (driver/describe-table (driver/->driver database) database table))

(s/defn ^:always-validate fk-metadata :- FKMetadata
  [database :- DatabaseInstance, table :- TableInstance]
  (let [driver (driver/->driver database)]
    (when (driver/driver-supports? :foreign-keys)
      (driver/describe-table-fks driver database table))))
