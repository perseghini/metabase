(ns metabase.sync.analyze.special-types
  "Logic for scanning values of a given field and updating special types as appropriate.
   Also known as 'fingerprinting', 'analysis', or 'classification'."
  (:require [cheshire.core :as json]
            [clojure.math.numeric-tower :as math]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]
            [metabase.models.field :refer [Field]]
            [metabase.sync.util :as sync-util]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.models.database.DatabaseInstance
           metabase.models.table.TableInstance))

(def ^:private ^:const ^Float percent-valid-url-threshold
  "Fields that have at least this percent of values that are valid URLs should be given a special type of `:type/URL`."
  0.95)

(def ^:private ^:const ^Integer average-length-no-preview-threshold
  "Fields whose values' average length is greater than this amount should be marked as `preview_display = false`."
  50)

(def ^:const ^Integer low-cardinality-threshold
  "Fields with less than this many distinct values should automatically be given a special type of `:type/Category`."
  300)

;;; ------------------------------------------------------------ Sync Util Type Inference Fns ------------------------------------------------------------

(def ^:private ^:const pattern+base-types+special-type
  "Tuples of `[name-pattern set-of-valid-base-types special-type]`.
   Fields whose name matches the pattern and one of the base types should be given the special type.

   *  Convert field name to lowercase before matching against a pattern
   *  Consider a nil set-of-valid-base-types to mean \"match any base type\""
  (let [bool-or-int #{:type/Boolean :type/Integer}
        float       #{:type/Float}
        int-or-text #{:type/Integer :type/Text}
        text        #{:type/Text}]
    [[#"^.*_lat$"       float       :type/Latitude]
     [#"^.*_lon$"       float       :type/Longitude]
     [#"^.*_lng$"       float       :type/Longitude]
     [#"^.*_long$"      float       :type/Longitude]
     [#"^.*_longitude$" float       :type/Longitude]
     [#"^.*_rating$"    int-or-text :type/Category]
     [#"^.*_type$"      int-or-text :type/Category]
     [#"^.*_url$"       text        :type/URL]
     [#"^_latitude$"    float       :type/Latitude]
     [#"^active$"       bool-or-int :type/Category]
     [#"^city$"         text        :type/City]
     [#"^country$"      text        :type/Country]
     [#"^countryCode$"  text        :type/Country]
     [#"^currency$"     int-or-text :type/Category]
     [#"^first_name$"   text        :type/Name]
     [#"^full_name$"    text        :type/Name]
     [#"^gender$"       int-or-text :type/Category]
     [#"^last_name$"    text        :type/Name]
     [#"^lat$"          float       :type/Latitude]
     [#"^latitude$"     float       :type/Latitude]
     [#"^lon$"          float       :type/Longitude]
     [#"^lng$"          float       :type/Longitude]
     [#"^long$"         float       :type/Longitude]
     [#"^longitude$"    float       :type/Longitude]
     [#"^name$"         text        :type/Name]
     [#"^postalCode$"   int-or-text :type/ZipCode]
     [#"^postal_code$"  int-or-text :type/ZipCode]
     [#"^rating$"       int-or-text :type/Category]
     [#"^role$"         int-or-text :type/Category]
     [#"^sex$"          int-or-text :type/Category]
     [#"^state$"        text        :type/State]
     [#"^status$"       int-or-text :type/Category]
     [#"^type$"         int-or-text :type/Category]
     [#"^url$"          text        :type/URL]
     [#"^zip_code$"     int-or-text :type/ZipCode]
     [#"^zipcode$"      int-or-text :type/ZipCode]]))

;; Check that all the pattern tuples are valid
(when-not config/is-prod?
  (doseq [[name-pattern base-types special-type] pattern+base-types+special-type]
    (assert (instance? java.util.regex.Pattern name-pattern))
    (assert (every? (u/rpartial isa? :type/*) base-types))
    (assert (isa? special-type :type/*))))

(defn- infer-field-special-type
  "If `name` and `base-type` matches a known pattern, return the `special_type` we should assign to it."
  [field-name base-type]
  (when (and (string? field-name)
             (keyword? base-type))
    (or (when (= "id" (str/lower-case field-name)) :type/PK)
        (some (fn [[name-pattern valid-base-types special-type]]
                (when (and (some (partial isa? base-type) valid-base-types)
                           (re-matches name-pattern (str/lower-case field-name)))
                  special-type))
              pattern+base-types+special-type))))

;;; ------------------------------------------------------------ Special Type tests ------------------------------------------------------------

(defn- test:no-preview-display
  "If FIELD's is textual and its average length is too great, mark it so it isn't displayed in the UI."
  [driver field field-stats]
  (if-not (and (= :normal (:visibility_type field))
               (isa? (:base_type field) :type/Text))
    ;; this field isn't suited for this test
    field-stats
    ;; test for avg length
    (throw (UnsupportedOperationException.))
    #_(let [avg-len (u/try-apply (:field-avg-length driver) field)]
      (if-not (and avg-len (> avg-len average-length-no-preview-threshold))
        field-stats
        (do
          (log/debug (u/format-color 'green "%s has an average length of %d. Not displaying it in previews." (sync-util/name-for-logging field) avg-len))
          (assoc field-stats :preview-display false))))))

(defn- test:url-special-type
  "If FIELD is texual, doesn't have a `special_type`, and its non-nil values are primarily URLs, mark it as `special_type` `:type/URL`."
  [driver field field-stats]
  (if-not (and (not (:special_type field))
               (isa? (:base_type field) :type/Text))
    ;; this field isn't suited for this test
    field-stats
    ;; test for url values
    (let [percent-urls 0] ; NOCOMMIT
      (if-not (and (float? percent-urls)
                   (>= percent-urls 0.0)
                   (<= percent-urls 100.0)
                   (> percent-urls percent-valid-url-threshold))
        field-stats
        (do
          (log/debug (u/format-color 'green "%s is %d%% URLs. Marking it as a URL." (sync-util/name-for-logging field) (int (math/round (* 100 percent-urls)))))
          (assoc field-stats :special-type :url))))))

(defn- values-are-valid-json?
  "`true` if at every item in VALUES is `nil` or a valid string-encoded JSON dictionary or array, and at least one of those is non-nil."
  [values]
  (try
    (loop [at-least-one-non-nil-value? false, [val & more] values]
      (cond
        (and (not val)
             (not (seq more))) at-least-one-non-nil-value?
        (str/blank? val)       (recur at-least-one-non-nil-value? more)
        ;; If val is non-nil, check that it's a JSON dictionary or array. We don't want to mark Fields containing other
        ;; types of valid JSON values as :json (e.g. a string representation of a number or boolean)
        :else                  (do (u/prog1 (json/parse-string val)
                                     (assert (or (map? <>)
                                                 (sequential? <>))))
                                   (recur true more))))
    (catch Throwable _
      false)))

(defn- test:json-special-type
  "Mark FIELD as `:json` if it's textual, doesn't already have a special type, the majority of it's values are non-nil, and all of its non-nil values
   are valid serialized JSON dictionaries or arrays."
  [driver field field-stats]
  (if (or (:special_type field)
          (not (isa? (:base_type field) :type/Text)))
    ;; this field isn't suited for this test
    field-stats
    ;; check for json values
    (if-not (values-are-valid-json? (take driver/max-sync-lazy-seq-results (driver/field-values-lazy-seq driver field)))
      field-stats
      (do
        (log/debug (u/format-color 'green "%s looks like it contains valid JSON objects. Setting special_type to :type/SerializedJSON." (sync-util/name-for-logging field)))
        (assoc field-stats :special-type :type/SerializedJSON, :preview-display false)))))

(defn- values-are-valid-emails?
  "`true` if at every item in VALUES is `nil` or a valid email, and at least one of those is non-nil."
  [values]
  (try
    (loop [at-least-one-non-nil-value? false, [val & more] values]
      (cond
        (and (not val)
             (not (seq more))) at-least-one-non-nil-value?
        (str/blank? val)       (recur at-least-one-non-nil-value? more)
        ;; If val is non-nil, check that it's a JSON dictionary or array. We don't want to mark Fields containing other
        ;; types of valid JSON values as :json (e.g. a string representation of a number or boolean)
        :else                  (do (assert (u/is-email? val))
                                   (recur true more))))
    (catch Throwable _
      false)))

(defn- test:email-special-type
  "Mark FIELD as `:email` if it's textual, doesn't already have a special type, the majority of it's values are non-nil, and all of its non-nil values
   are valid emails."
  [driver field field-stats]
  (if (or (:special_type field)
          (not (isa? (:base_type field) :type/Text)))
    ;; this field isn't suited for this test
    field-stats
    ;; check for emails
    (if-not (values-are-valid-emails? (take driver/max-sync-lazy-seq-results (driver/field-values-lazy-seq driver field)))
      field-stats
      (do
        (log/debug (u/format-color 'green "%s looks like it contains valid email addresses. Setting special_type to :type/Email." (sync-util/name-for-logging field)))
        (assoc field-stats :special-type :type/Email, :preview-display true)))))

(defn- test:new-field
  "Do the various tests that should only be done for a new `Field`.
   We only run most of the field analysis work when the field is NEW in order to favor performance of the sync process."
  [driver field field-stats]
  (->> field-stats
       (test:no-preview-display driver field)
       (test:url-special-type   driver field)
       (test:json-special-type  driver field)
       (test:email-special-type driver field)))


;;; ------------------------------------------------------------ Etc ------------------------------------------------------------

(s/defn ^:private ^:always-validate update-fields-last-analyzed!
  "Update the `last_analyzed` date for all the fields in TABLE."
  [table :- TableInstance]
  (db/update-where! Field {:table_id        (u/get-id table)
                           :active          true
                           :visibility_type [:not= "retired"]}
    :last_analyzed (u/new-sql-timestamp)))

(s/defn ^:private ^:always-validate infer-special-types-for-table!
  [table :- TableInstance]
  ;; TODO - need to do the actual special type tests
  (throw (UnsupportedOperationException.))
  (update-fields-last-analyzed! table))


(s/defn ^:always-validate infer-special-types!
  [database :- DatabaseInstance]
  (let [tables (sync-util/db->sync-tables database)]
    (sync-util/with-emoji-progress-bar [emoji-progress-bar (count tables)]
      (doseq [table tables]
        (try
          (infer-special-types-for-table! table)
          (catch Throwable t
            (log/error "Unexpected error analyzing table" t))
          (finally
            (log/info (u/format-color 'blue "%s Analyzed %s" (emoji-progress-bar) (sync-util/name-for-logging table)))))))))
