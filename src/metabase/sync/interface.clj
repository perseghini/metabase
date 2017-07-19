(ns metabase.sync.interface
  "Schemas and constants used by the sync code.")

(def ^:const ^Integer low-cardinality-threshold
  "Fields with less than this many distinct values should automatically be given a special type of `:type/Category`."
  300)
