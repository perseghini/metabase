(ns metabase.sync.sync-metadata.tables
  "Logic for updating Metabase Table models from metadata fetched from a physical DB."
  (:require [clojure.string :as str]
            [metabase.models
             [humanization :as humanization]
             [table :refer [Table]]]
            [metabase.sync.fetch-metadata :as fetch-metadata]
            [schema.core :as s]
            [toucan.db :as db])
  (:import metabase.models.database.DatabaseInstance))

;;; ------------------------------------------------------------  "Crufty" Tables ------------------------------------------------------------

;; Crufty tables are ones we know are from frameworks like Rails or Django and thus automatically mark as `:cruft`

(def ^:private ^:const crufty-table-patterns
  "Regular expressions that match Tables that should automatically given the `visibility-type` of `:cruft`.
   This means they are automatically hidden to users (but can be unhidden in the admin panel).
   These `Tables` are known to not contain useful data, such as migration or web framework internal tables."
  #{;; Django
    #"^auth_group$"
    #"^auth_group_permissions$"
    #"^auth_permission$"
    #"^django_admin_log$"
    #"^django_content_type$"
    #"^django_migrations$"
    #"^django_session$"
    #"^django_site$"
    #"^south_migrationhistory$"
    #"^user_groups$"
    #"^user_user_permissions$"
    ;; Drupal
    #".*_cache$"
    #".*_revision$"
    #"^advagg_.*"
    #"^apachesolr_.*"
    #"^authmap$"
    #"^autoload_registry.*"
    #"^batch$"
    #"^blocked_ips$"
    #"^cache.*"
    #"^captcha_.*"
    #"^config$"
    #"^field_revision_.*"
    #"^flood$"
    #"^node_revision.*"
    #"^queue$"
    #"^rate_bot_.*"
    #"^registry.*"
    #"^router.*"
    #"^semaphore$"
    #"^sequences$"
    #"^sessions$"
    #"^watchdog$"
    ;; Rails / Active Record
    #"^schema_migrations$"
    ;; PostGIS
    #"^spatial_ref_sys$"
    ;; nginx
    #"^nginx_access_log$"
    ;; Liquibase
    #"^databasechangelog$"
    #"^databasechangeloglock$"
    ;; Lobos
    #"^lobos_migrations$"})

(defn- is-crufty-table?
  "Should we give newly created TABLE a `visibility_type` of `:cruft`?"
  [table]
  (boolean (some #(re-find % (str/lower-case (:name table))) crufty-table-patterns)))


;;; ------------------------------------------------------------ Syncing ------------------------------------------------------------

(defn ^:deprecated update-table-from-tabledef!
  "Update `Table` with the data from TABLE-DEF."
  [{:keys [id display_name], :as existing-table} {table-name :name}]
  {:pre [(integer? id)]}
  (let [updated-table (assoc existing-table
                        :display_name (or display_name (humanization/name->human-readable-name table-name)))]
    ;; the only thing we need to update on a table is the :display_name, if it never got set
    (when (nil? display_name)
      (db/update! Table id
        :display_name (:display_name updated-table)))
    ;; always return the table when we are done
    updated-table))

(defn ^:deprecated create-table-from-tabledef!
  "Create `Table` with the data from TABLE-DEF."
  [database-id {schema-name :schema, table-name :name, raw-table-id :raw-table-id, visibility-type :visibility-type}]
  (if-let [existing-id (db/select-one-id Table :db_id database-id, :raw_table_id raw-table-id, :schema schema-name, :name table-name, :active false)]
    ;; if the table already exists but is marked *inactive*, mark it as *active*
    (db/update! Table existing-id
      :active true)
    ;; otherwise create a new Table
    (db/insert! Table
      :db_id           database-id
      :raw_table_id    raw-table-id
      :schema          schema-name
      :name            table-name
      :visibility_type visibility-type
      :display_name    (humanization/name->human-readable-name table-name)
      :active          true)))

(s/defn ^:always-validate sync-tables!
  [database :- DatabaseInstance]
  (let [metadata (fetch-metadata/db-metadata database)]
    (throw (NoSuchMethodException.))))
