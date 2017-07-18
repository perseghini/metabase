(ns metabase.sync.field-values
  "Logic for updating cached FieldValues for fields in a database."
  (:require [metabase.models
             database
             [field-values :as field-values]]
            [metabase.sync.analyze.special-types :as special-types]
            [schema.core :as s]
            [metabase.sync.util :as sync-util])
  (:import metabase.models.database.DatabaseInstance))

(def ^:private ^:const ^Integer field-values-entry-max-length
  "The maximum character length for a stored `FieldValues` entry."
  100)

(def ^:private ^:const ^Integer field-values-total-max-length
  "Maximum total length for a `FieldValues` entry (combined length of all values for the field)."
  (* special-types/low-cardinality-threshold field-values-entry-max-length))

#_(defn test-for-cardinality?
  "Should FIELD should be tested for cardinality?"
  [field is-new?]
  (or (field-values/field-should-have-field-values? field)
      (and (nil? (:special_type field))
           is-new?
           (not (isa? (:base_type field) :type/DateTime))
           (not (isa? (:base_type field) :type/Collection))
           (not (= (:base_type field) :type/*)))))

#_(defn- field-values-below-low-cardinality-threshold? [non-nil-values]
  (and (<= (count non-nil-values) special-types/low-cardinality-threshold)
      ;; very simple check to see if total length of field-values exceeds (total values * max per value)
       (let [total-length (reduce + (map (comp count str) non-nil-values))]
         (<= total-length field-values-total-max-length))))

#_(defn test:cardinality-and-extract-field-values
  "Extract field-values for FIELD.  If number of values exceeds `low-cardinality-threshold` then we return an empty set of values."
  [field field-stats]
  ;; TODO: we need some way of marking a field as not allowing field-values so that we can skip this work if it's not appropriate
  ;;       for example, :type/Category fields with more than MAX values don't need to be rescanned all the time
  (let [non-nil-values  (filter identity (queries/field-distinct-values field (inc low-cardinality-threshold)))
        ;; only return the list if we didn't exceed our MAX values and if the the total character count of our values is reasable (#2332)
        distinct-values (when (field-values-below-low-cardinality-threshold? non-nil-values)
                          non-nil-values)]
    (cond-> (assoc field-stats :values distinct-values)
      (and (nil? (:special_type field))
           (pos? (count distinct-values))) (assoc :special-type :type/Category))))


(s/defn ^:always-validate update-field-values! [database :- DatabaseInstance]
  (sync-util/sync-operation :cache-field-values database (format "Cache field values in %s" (sync-util/name-for-logging database))
    (throw (UnsupportedOperationException.))))
