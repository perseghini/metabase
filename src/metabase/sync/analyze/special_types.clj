(ns metabase.sync.analyze.special-types
  "Logic for scanning values of a given field and updating special types as appropriate.
   Also known as 'fingerprinting', 'analysis', or 'classification'."
  (:require metabase.models.database
            [schema.core :as s])
  (:import metabase.models.database.DatabaseInstance))

(s/defn ^:always-validate infer-special-types!
  [database :- DatabaseInstance]
  (throw (NoSuchMethodException.)))
