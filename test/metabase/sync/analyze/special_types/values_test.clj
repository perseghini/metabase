(ns metabase.sync.analyze.special-types.values-test
  (:require [expectations :refer :all]
            [metabase.driver :as driver]
            [metabase.sync.analyze.special-types.values :as special-types-values]
            [metabase.test.data :as data]
            [metabase.test.data.datasets :as datasets]
            [toucan.db :as db]))

(defn- field-values
  "Return a sequence of values for Field. This is the same data that other functions see; it is a result of
   calling `metabase.sync.analyze.special-types.values/field-values`."
  ([table-kw field-kw]
   (field-values (db/select-one 'Field :id (data/id table-kw field-kw))))
  ([field]
   (let [driver (driver/->driver (db/select-one-field :db_id 'Table :id (:table_id field)))]
     (#'special-types-values/field-values driver field))))

;;; FIELD-AVG-LENGTH
(datasets/expect-with-all-engines
  16
  (Math/round (#'special-types-values/avg-length (field-values :venues :name))))
