(ns metabase.sync.field-values
  "Logic for updating cached FieldValues for fields in a database."
  (:require metabase.models.database
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate update-field-values! [database :- DatabaseInstance]
  (throw (NoSuchMethodException.)))
