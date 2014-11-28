require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'

# Make sure we're using pg high enough for type casts
gem 'pg', '~> 0.18.0.pre20141117110243'
require 'pg'

require 'active_record/connection_adapters/postgresql/utils'
require 'active_record/connection_adapters/postgresql/column'
require 'active_record/connection_adapters/postgresql/oid'
require 'active_record/connection_adapters/postgresql/quoting'
require 'active_record/connection_adapters/postgresql/referential_integrity'
require 'active_record/connection_adapters/postgresql/schema_definitions'
require 'active_record/connection_adapters/postgresql/schema_statements'
require 'active_record/connection_adapters/postgresql/database_statements'

require 'arel/visitors/bind_visitor'

require 'ipaddr'

module ActiveRecord
  module ConnectionHandling # :nodoc:
    VALID_CONN_PARAMS = [:host, :hostaddr, :port, :dbname, :user, :password, :connect_timeout,
                         :client_encoding, :options, :application_name, :fallback_application_name,
                         :keepalives, :keepalives_idle, :keepalives_interval, :keepalives_count,
                         :tty, :sslmode, :requiressl, :sslcompression, :sslcert, :sslkey,
                         :sslrootcert, :sslcrl, :requirepeer, :krbsrvname, :gsslib, :service]

    # Establishes a connection to the database that's used by all Active Record objects
    def postgresql_connection(config)
      conn_params = config.symbolize_keys

      conn_params.delete_if { |_, v| v.nil? }

      # Map ActiveRecords param names to PGs.
      conn_params[:user] = conn_params.delete(:username) if conn_params[:username]
      conn_params[:dbname] = conn_params.delete(:database) if conn_params[:database]

      # Forward only valid config params to PGconn.connect.
      conn_params.keep_if { |k, _| VALID_CONN_PARAMS.include?(k) }

      # The postgres drivers don't allow the creation of an unconnected PGconn object,
      # so just pass a nil connection object for the time being.
      ConnectionAdapters::PostgreSQLAdapter.new(nil, logger, conn_params, config)
    end
  end

  module ConnectionAdapters
    # The PostgreSQL adapter works with the native C (https://bitbucket.org/ged/ruby-pg) driver.
    #
    # Options:
    #
    # * <tt>:host</tt> - Defaults to a Unix-domain socket in /tmp. On machines without Unix-domain sockets,
    #   the default is to connect to localhost.
    # * <tt>:port</tt> - Defaults to 5432.
    # * <tt>:username</tt> - Defaults to be the same as the operating system name of the user running the application.
    # * <tt>:password</tt> - Password to be used if the server demands password authentication.
    # * <tt>:database</tt> - Defaults to be the same as the user name.
    # * <tt>:schema_search_path</tt> - An optional schema search path for the connection given
    #   as a string of comma-separated schema names. This is backward-compatible with the <tt>:schema_order</tt> option.
    # * <tt>:encoding</tt> - An optional client encoding that is used in a <tt>SET client_encoding TO
    #   <encoding></tt> call on the connection.
    # * <tt>:min_messages</tt> - An optional client min messages that is used in a
    #   <tt>SET client_min_messages TO <min_messages></tt> call on the connection.
    # * <tt>:variables</tt> - An optional hash of additional parameters that
    #   will be used in <tt>SET SESSION key = val</tt> calls on the connection.
    # * <tt>:insert_returning</tt> - An optional boolean to control the use or <tt>RETURNING</tt> for <tt>INSERT</tt> statements
    #   defaults to true.
    #
    # Any further options are used as connection parameters to libpq. See
    # http://www.postgresql.org/docs/9.1/static/libpq-connect.html for the
    # list of parameters.
    #
    # In addition, default connection parameters of libpq can be set per environment variables.
    # See http://www.postgresql.org/docs/9.1/static/libpq-envars.html .
    class PostgreSQLAdapter < AbstractAdapter
      ADAPTER_NAME = 'PostgreSQL'.freeze

      NATIVE_DATABASE_TYPES = {
        primary_key: "serial primary key",
        bigserial: "bigserial",
        string:      { name: "character varying" },
        text:        { name: "text" },
        integer:     { name: "integer" },
        float:       { name: "float" },
        decimal:     { name: "decimal" },
        datetime:    { name: "timestamp" },
        time:        { name: "time" },
        date:        { name: "date" },
        daterange:   { name: "daterange" },
        numrange:    { name: "numrange" },
        tsrange:     { name: "tsrange" },
        tstzrange:   { name: "tstzrange" },
        int4range:   { name: "int4range" },
        int8range:   { name: "int8range" },
        binary:      { name: "bytea" },
        boolean:     { name: "boolean" },
        bigint:      { name: "bigint" },
        xml:         { name: "xml" },
        tsvector:    { name: "tsvector" },
        hstore:      { name: "hstore" },
        inet:        { name: "inet" },
        cidr:        { name: "cidr" },
        macaddr:     { name: "macaddr" },
        uuid:        { name: "uuid" },
        json:        { name: "json" },
        jsonb:       { name: "jsonb" },
        ltree:       { name: "ltree" },
        citext:      { name: "citext" },
        point:       { name: "point" },
        bit:         { name: "bit" },
        bit_varying: { name: "bit varying" },
        money:       { name: "money" },
      }

      OID = PostgreSQL::OID #:nodoc:

      include PostgreSQL::Quoting
      include PostgreSQL::ReferentialIntegrity
      include PostgreSQL::SchemaStatements
      include PostgreSQL::DatabaseStatements
      include Savepoints

      def schema_creation # :nodoc:
        PostgreSQL::SchemaCreation.new self
      end

      # Adds +:array+ option to the default set provided by the
      # AbstractAdapter
      def prepare_column_options(column, types) # :nodoc:
        spec = super
        spec[:array] = 'true' if column.respond_to?(:array) && column.array
        spec[:default] = "\"#{column.default_function}\"" if column.default_function
        spec
      end

      # Adds +:array+ as a valid migration key
      def migration_keys
        super + [:array]
      end

      # Returns +true+, since this connection adapter supports prepared statement
      # caching.
      def supports_statement_cache?
        true
      end

      def supports_index_sort_order?
        true
      end

      def supports_partial_index?
        true
      end

      def supports_transaction_isolation?
        true
      end

      def supports_foreign_keys?
        true
      end

      def supports_views?
        true
      end

      def index_algorithms
        { concurrently: 'CONCURRENTLY' }
      end

      # SQL statements counter pool
      #
      # We don't care about collisions in this simple hash implementation, because
      # they have a performance impact only.
      class CountedStatementPool
        def initialize(max)
          @max = max
          clear
        end

        def [](sql_key)
          @cache[sql_key.hash % @max]
        end

        def []=(sql_key, count)
          @cache[sql_key.hash % @max] = count
        end

        def delete(sql_key)
          @cache[sql_key.hash % @max] = nil
        end

        def clear
          @cache = Array.new(@max)
        end
      end

      class PreparedStatementPool
        PoolEntry = Struct.new :stmt_key, :enc_type_map, :dec_type_map, :field_names, :types

        def initialize(connection, max)
          @connection = connection
          @max = max
          @cache = Hash.new { |h,pid| h[pid] = {} }
          @counter = 0
          @pending_query_sql = nil
          @pending_query_proc = nil
        end

        def length;       cache.length; end

        def [](sql_key)
          # Ensure a LRU cache behaviour
          entry = cache.delete(sql_key)
          cache[sql_key] = entry if entry
          entry
        end

        # requires a subsequent call to execute_pending_query to process the database query
        def add(sql_key, sql, *args)
          @counter += 1
          stmt_key = "a#{@counter}"
          pool_entry = PreparedStatementPool::PoolEntry.new(stmt_key, *args)
          set_pending(sql){ @connection.send_prepare(stmt_key, sql) }
          cache[sql_key] = pool_entry
        end

        # requires a subsequent call to execute_pending_query to process the database query
        def clear
          dealloc(cache.each_value.map(&:stmt_key))
          cache.clear
        end

        def delete_without_dealloc(sql_key)
          cache.delete sql_key
        end

        # requires a subsequent call to execute_pending_query to process the database query
        def delete(sql_key)
          dealloc([cache[sql_key].stmt_key])
          delete_without_dealloc(sql_key)
        end

        # requires a subsequent call to execute_pending_query to process the database query
        def delete_oversized
          if @max <= cache.size
            # remove 5% least recently used statements in a single query
            keys = (@max * 95 / 100 .. cache.size).map do
              cache.shift.last.stmt_key
            end
            dealloc(keys)
          end
        end

        def send_pending_query
          if @pending_query_proc
            @pending_query_proc.call
            @pending_query_proc = nil
            true
          else
            false
          end
        end

        def finish_pending_query
          return unless @pending_query_sql
          @pending_query_sql = nil
          @connection.get_last_result
        rescue PG::Error => err
          yield err, @pending_query_sql
        end

        def execute_pending_query
          send_pending_query
          finish_pending_query
        end

        def discard_pending_query
          @pending_query_sql = nil
          @pending_query_proc = nil
        end

        private

          def cache
            @cache[Process.pid]
          end

          def dealloc(stmt_keys)
            return if stmt_keys.empty?
            sql = stmt_keys.map { |key| "DEALLOCATE #{key};" }.join
            set_pending(sql){ @connection.send_query(sql) }
          end

          def set_pending(sql, &block)
            raise ArgumentError, "another pending query is already running: #{@pending_query_sql}" if @pending_query_proc
            @pending_query_sql = sql
            @pending_query_proc = block
          end
      end

      # Initializes and connects a PostgreSQL adapter.
      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger)

        @visitor = Arel::Visitors::PostgreSQL.new self
        if self.class.type_cast_config_to_boolean(config.fetch(:prepared_statements) { true })
          @prepared_statements = true
        else
          @prepared_statements = false
        end

        @connection_parameters, @config = connection_parameters, config

        # @local_tz is initialized as nil to avoid warnings when connect tries to use it
        @local_tz = nil
        @table_alias_length = nil
        # Use a dummy pool to succeed the call to finish_pending_query while calling connect
        @prepared_statements_pool = Struct.new(:finish_pending_query).new

        connect

        statement_limit = self.class.type_cast_config_to_integer(config.fetch(:statement_limit) { 1000 })
        @counted_statements_pool = CountedStatementPool.new(statement_limit)
        @prepared_statements_pool = PreparedStatementPool.new(@connection, statement_limit)

        if postgresql_version < 80200
          raise "Your version of PostgreSQL (#{postgresql_version}) is too old, please upgrade!"
        end

        # Enable optimized encoders for selected object types.
        # This is used for adapter-internal queries and if column informations
        # are not available for the given query bind parameter.
        enc_tm = ::PG::TypeMapByClass.new
        enc_tm[Integer] = ::PG::TextEncoder::Integer.new
        enc_tm[TrueClass] = ::PG::TextEncoder::Boolean.new
        enc_tm[FalseClass] = ::PG::TextEncoder::Boolean.new
        enc_tm[Float] = ::PG::TextEncoder::Float.new
        @connection.type_map_for_queries = enc_tm

        # Enable type casted output for selected OIDs.
        # This is used for all adapter-internal queries and queries executed per
        # PostgreSQLAdapter#query, #execute and #select_* methods.
        dec_tm = ::PG::TypeMapByOid.new
        dec_tm.add_coder ::PG::TextDecoder::Integer.new oid: 20, name: 'int8'
        dec_tm.add_coder ::PG::TextDecoder::Integer.new oid: 21, name: 'int2'
        dec_tm.add_coder ::PG::TextDecoder::Integer.new oid: 23, name: 'int4'
        dec_tm.add_coder ::PG::TextDecoder::Integer.new oid: 26, name: 'oid'
        dec_tm.add_coder ::PG::TextDecoder::Boolean.new oid: 16, name: 'bool'
        dec_tm.add_coder ::PG::TextDecoder::Float.new oid: 700, name: 'float4'
        dec_tm.add_coder ::PG::TextDecoder::Float.new oid: 701, name: 'float8'
        @connection.type_map_for_results = dec_tm


        @type_map = Type::HashLookupTypeMap.new
        initialize_type_map(type_map)

        @local_tz = execute('SHOW TIME ZONE', 'SCHEMA').first["TimeZone"]
        @use_insert_returning = @config.key?(:insert_returning) ? self.class.type_cast_config_to_boolean(@config[:insert_returning]) : true
      end

      # Clears the prepared statements cache.
      def clear_cache!
        finish_pending_query
        @counted_statements_pool.clear
        @prepared_statements_pool.clear
        @prepared_statements_pool.execute_pending_query rescue PG::Error
      end

      def raw_connection
        finish_pending_query
        super
      end

      def truncate(table_name, name = nil)
        exec_query "TRUNCATE TABLE #{quote_table_name(table_name)}", name, []
      end

      # Is this connection alive and ready for queries?
      def active?
        finish_pending_query
        @connection.query 'SELECT 1'
        true
      rescue PGError, ActiveRecord::StatementInvalid
        false
      end

      # Close then reopen the connection.
      def reconnect!
        finish_pending_query
        super
        @connection.reset
        configure_connection
      end

      def reset!
        clear_cache!
        reset_transaction
        unless @connection.transaction_status == ::PG::PQTRANS_IDLE
          @connection.query 'ROLLBACK'
        end
        @connection.query 'DISCARD ALL'
        configure_connection
      end

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        finish_pending_query
        super
        @connection.close rescue nil
      end

      def native_database_types #:nodoc:
        NATIVE_DATABASE_TYPES
      end

      # Returns true, since this connection adapter supports migrations.
      def supports_migrations?
        true
      end

      # Does PostgreSQL support finding primary key on non-Active Record tables?
      def supports_primary_key? #:nodoc:
        true
      end

      # Enable standard-conforming strings if available.
      def set_standard_conforming_strings
        old, self.client_min_messages = client_min_messages, 'panic'
        execute('SET standard_conforming_strings = on', 'SCHEMA') rescue nil
      ensure
        self.client_min_messages = old
      end

      def supports_ddl_transactions?
        true
      end

      def supports_explain?
        true
      end

      # Returns true if pg > 9.1
      def supports_extensions?
        postgresql_version >= 90100
      end

      # Range datatypes weren't introduced until PostgreSQL 9.2
      def supports_ranges?
        postgresql_version >= 90200
      end

      def supports_materialized_views?
        postgresql_version >= 90300
      end

      def enable_extension(name)
        exec_query("CREATE EXTENSION IF NOT EXISTS \"#{name}\"").tap {
          reload_type_map
        }
      end

      def disable_extension(name)
        exec_query("DROP EXTENSION IF EXISTS \"#{name}\" CASCADE").tap {
          reload_type_map
        }
      end

      def extension_enabled?(name)
        if supports_extensions?
          res = exec_query "SELECT EXISTS(SELECT * FROM pg_available_extensions WHERE name = '#{name}' AND installed_version IS NOT NULL) as enabled",
            'SCHEMA'
          res.cast_values.first
        end
      end

      def extensions
        if supports_extensions?
          exec_query("SELECT extname from pg_extension", "SCHEMA").cast_values
        else
          super
        end
      end

      # Returns the configured supported identifier length supported by PostgreSQL
      def table_alias_length
        @table_alias_length ||= query('SHOW max_identifier_length', 'SCHEMA')[0][0].to_i
      end

      # Set the authorized user for this session
      def session_auth=(user)
        clear_cache!
        exec_query "SET SESSION AUTHORIZATION #{user}"
      end

      def use_insert_returning?
        @use_insert_returning
      end

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      def update_table_definition(table_name, base) #:nodoc:
        PostgreSQL::Table.new(table_name, base)
      end

      def lookup_cast_type(sql_type) # :nodoc:
        oid = execute("SELECT #{quote(sql_type)}::regtype::oid", "SCHEMA").first['oid'].to_i
        super(oid)
      end

      def column_name_for_operation(operation, node) # :nodoc:
        OPERATION_ALIASES.fetch(operation) { operation.downcase }
      end

      OPERATION_ALIASES = { # :nodoc:
        "maximum" => "max",
        "minimum" => "min",
        "average" => "avg",
      }

      protected

        # Returns the version of the connected PostgreSQL server.
        def postgresql_version
          @connection.server_version
        end

        # See http://www.postgresql.org/docs/9.1/static/errcodes-appendix.html
        FOREIGN_KEY_VIOLATION = "23503"
        UNIQUE_VIOLATION      = "23505"

        def translate_exception(exception, message)
          return exception unless exception.respond_to?(:result)

          case exception.result.try(:error_field, PGresult::PG_DIAG_SQLSTATE)
          when UNIQUE_VIOLATION
            RecordNotUnique.new(message, exception)
          when FOREIGN_KEY_VIOLATION
            InvalidForeignKey.new(message, exception)
          else
            super
          end
        end

      private

        def get_oid_type(oid, fmod, column_name, sql_type = '') # :nodoc:
          if !type_map.key?(oid)
            load_additional_types(type_map, [oid])
          end

          type_map.fetch(oid, fmod, sql_type) {
            warn "unknown OID #{oid}: failed to recognize type of '#{column_name}'. It will be treated as String."
            Type::Value.new.tap do |cast_type|
              type_map.register_type(oid, cast_type)
            end
          }
        end

        def initialize_type_map(m) # :nodoc:
          register_class_with_limit m, 'int2', OID::Integer
          m.alias_type 'int4', 'int2'
          m.alias_type 'int8', 'int2'
          m.alias_type 'oid', 'int2'
          m.register_type 'float4', OID::Float.new
          m.alias_type 'float8', 'float4'
          m.register_type 'text', Type::Text.new
          register_class_with_limit m, 'varchar', Type::String
          m.alias_type 'char', 'varchar'
          m.alias_type 'name', 'varchar'
          m.alias_type 'bpchar', 'varchar'
          m.register_type 'bool', OID::Boolean.new
          register_class_with_limit m, 'bit', OID::Bit
          register_class_with_limit m, 'varbit', OID::BitVarying
          m.alias_type 'timestamptz', 'timestamp'
          m.register_type 'date', OID::Date.new
          m.register_type 'time', OID::Time.new

          m.register_type 'money', OID::Money.new
          m.register_type 'bytea', OID::Bytea.new
          m.register_type 'point', OID::Point.new
          m.register_type 'hstore', OID::Hstore.new
          m.register_type 'json', OID::Json.new
          m.register_type 'jsonb', OID::Jsonb.new
          m.register_type 'cidr', OID::Cidr.new
          m.register_type 'inet', OID::Inet.new
          m.register_type 'uuid', OID::Uuid.new
          m.register_type 'xml', OID::Xml.new
          m.register_type 'tsvector', OID::SpecializedString.new(:tsvector)
          m.register_type 'macaddr', OID::SpecializedString.new(:macaddr)
          m.register_type 'citext', OID::SpecializedString.new(:citext)
          m.register_type 'ltree', OID::SpecializedString.new(:ltree)

          # FIXME: why are we keeping these types as strings?
          m.alias_type 'interval', 'varchar'
          m.alias_type 'path', 'varchar'
          m.alias_type 'line', 'varchar'
          m.alias_type 'polygon', 'varchar'
          m.alias_type 'circle', 'varchar'
          m.alias_type 'lseg', 'varchar'
          m.alias_type 'box', 'varchar'

          m.register_type 'timestamp' do |_, _, sql_type|
            precision = extract_precision(sql_type)
            OID::DateTime.new(precision: precision)
          end

          m.register_type 'numeric' do |_, fmod, sql_type|
            precision = extract_precision(sql_type)
            scale = extract_scale(sql_type)

            # The type for the numeric depends on the width of the field,
            # so we'll do something special here.
            #
            # When dealing with decimal columns:
            #
            # places after decimal  = fmod - 4 & 0xffff
            # places before decimal = (fmod - 4) >> 16 & 0xffff
            if fmod && (fmod - 4 & 0xffff).zero?
              # FIXME: Remove this class, and the second argument to
              # lookups on PG
              Type::DecimalWithoutScale.new(precision: precision)
            else
              OID::Decimal.new(precision: precision, scale: scale)
            end
          end

          load_additional_types(m)
        end

        def extract_limit(sql_type) # :nodoc:
          case sql_type
          when /^bigint/i;    8
          when /^smallint/i;  2
          else super
          end
        end

        # Extracts the value from a PostgreSQL column default definition.
        def extract_value_from_default(oid, default) # :nodoc:
          case default
            # Quoted types
            when /\A[\(B]?'(.*)'::/m
              $1.gsub(/''/, "'")
            # Boolean types
            when 'true', 'false'
              default
            # Numeric types
            when /\A\(?(-?\d+(\.\d*)?\)?(::bigint)?)\z/
              $1
            # Object identifier types
            when /\A-?\d+\z/
              $1
            else
              # Anything else is blank, some user type, or some function
              # and we can't know the value of that, so return nil.
              nil
          end
        end

        def extract_default_function(default_value, default) # :nodoc:
          default if has_default_function?(default_value, default)
        end

        def has_default_function?(default_value, default) # :nodoc:
          !default_value && (%r{\w+\(.*\)} === default)
        end

        def load_additional_types(type_map, oids = nil) # :nodoc:
          if supports_ranges?
            query = <<-SQL
              SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype
              FROM pg_type as t
              LEFT JOIN pg_range as r ON oid = rngtypid
            SQL
          else
            query = <<-SQL
              SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, t.typtype, t.typbasetype
              FROM pg_type as t
            SQL
          end

          if oids
            query += "WHERE t.oid::integer IN (%s)" % oids.join(", ")
          end

          initializer = OID::TypeMapInitializer.new(type_map)
          records = execute(query, 'SCHEMA')
          initializer.run(records)
        end

        FEATURE_NOT_SUPPORTED = "0A000" #:nodoc:

        def finish_pending_query
          @prepared_statements_pool.finish_pending_query do |err, sql|
            raise translate_exception_class(err, sql)
          end
        end

        def execute_and_clear(sql, name, binds)
          sql_key = sql_key(sql)
          pgresult, pool_entry = if pool_entry = @prepared_statements_pool[sql_key]
            exec_prepared(sql, name, binds, pool_entry)
          elsif count = @counted_statements_pool[sql_key]
            count += 1
            @counted_statements_pool[sql_key] = count
            # Create a prepared statement if the query is used twice
            prepare = @prepared_statements && count >= 2 && sql_key
            exec_counted(sql, name, binds, prepare)
          else
            @counted_statements_pool[sql_key] = 1
            exec_counted(sql, name, binds, false)
          end

          ret = yield(pgresult, pool_entry)
          pgresult.clear
          ret
        end

        def exec_counted(sql, name, binds, sql_key)
          if without_prepared_statement?(binds)
            finish_pending_query
            @connection.send_query(sql, [])
            type_casted_binds = binds
          else
            pg_encoders = binds.map do |col, val|
              col && col.cast_type.respond_to?(:pg_encoder) ? col.cast_type.pg_encoder : nil
            end
            enc_type_map = PG::TypeMapByColumn.new(pg_encoders).with_default_type_map( @connection.type_map_for_queries )

            type_casted_binds = binds.map do |col, val|
              [col, type_cast(val, col)]
            end
            type_casted_values = type_casted_binds.map { |_, val| val }

            finish_pending_query
            @connection.send_query(sql, type_casted_values, 0, enc_type_map)
          end

          if sql_key
            @counted_statements_pool.delete(sql_key)
            pool_entry = @prepared_statements_pool.add(sql_key, sql, enc_type_map)
          else
            @prepared_statements_pool.delete_oversized
          end

          pgresult = log(sql, name, type_casted_binds) do
            # do an extra call to PG::Connection#block, because although
            # get_last_result is GVL friendly, it doesn't stop on Ctrl-C
            @connection.block
            @connection.get_last_result
          end

          @prepared_statements_pool.send_pending_query

          [pgresult, pool_entry]
        rescue ActiveRecord::StatementInvalid => e
          @prepared_statements_pool.discard_pending_query
          @prepared_statements_pool.delete_without_dealloc(sql_key) if sql_key
          raise e
        end

        def exec_prepared(sql, name, binds, pool_entry)
          unless pool_entry.enc_type_map
            pg_encoders = binds.map do |col, val|
              col && col.cast_type.respond_to?(:pg_encoder) ? col.cast_type.pg_encoder : nil
            end
            pool_entry.enc_type_map = PG::TypeMapByColumn.new(pg_encoders).with_default_type_map( @connection.type_map_for_queries )
          end

          type_casted_binds = binds.map do |col, val|
            [col, type_cast(val, col)]
          end
          type_casted_values = type_casted_binds.map { |_, val| val }

          finish_pending_query
          @connection.send_query_prepared(pool_entry.stmt_key, type_casted_values, 0, pool_entry.enc_type_map)

          pgresult = log(sql, name, type_casted_binds, pool_entry.stmt_key) do
            # do an extra call to PG::Connection#block, because although
            # get_last_result is GVL friendly, it doesn't stop on Ctrl-C
            @connection.block
            @connection.get_last_result
          end

          [pgresult, pool_entry]
        rescue ActiveRecord::StatementInvalid => e
          # Annoyingly, the code for prepared statements whose return value may
          # have changed is FEATURE_NOT_SUPPORTED.  Check here for more details:
          # http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/utils/cache/plancache.c#l573
          if e.original_exception.is_a?(::PG::FeatureNotSupported)
            @prepared_statements_pool.delete(sql_key(sql))
            retry
          else
            raise e
          end
        end

        # Returns the statement identifier for the client side cache
        # of statements
        def sql_key(sql)
          "#{schema_search_path}-#{sql}"
        end

        # Connects to a PostgreSQL server and sets up the adapter depending on the
        # connected server's characteristics.
        def connect
          @connection = PGconn.connect(@connection_parameters)

          # Money type has a fixed precision of 10 in PostgreSQL 8.2 and below, and as of
          # PostgreSQL 8.3 it has a fixed precision of 19. PostgreSQLColumn.extract_precision
          # should know about this but can't detect it there, so deal with it here.
          OID::Money.precision = (postgresql_version >= 80300) ? 19 : 10

          configure_connection
        rescue ::PG::Error => error
          if error.message.include?("does not exist")
            raise ActiveRecord::NoDatabaseError.new(error.message, error)
          else
            raise
          end
        end

        # Configures the encoding, verbosity, schema search path, and time zone of the connection.
        # This is called by #connect and should not be called manually.
        def configure_connection
          if @config[:encoding]
            @connection.set_client_encoding(@config[:encoding])
          end
          self.client_min_messages = @config[:min_messages] || 'warning'
          self.schema_search_path = @config[:schema_search_path] || @config[:schema_order]

          # Use standard-conforming strings if available so we don't have to do the E'...' dance.
          set_standard_conforming_strings

          # If using Active Record's time zone support configure the connection to return
          # TIMESTAMP WITH ZONE types in UTC.
          # (SET TIME ZONE does not use an equals sign like other SET variables)
          if ActiveRecord::Base.default_timezone == :utc
            execute("SET time zone 'UTC'", 'SCHEMA')
          elsif @local_tz
            execute("SET time zone '#{@local_tz}'", 'SCHEMA')
          end

          # SET statements from :variables config hash
          # http://www.postgresql.org/docs/8.3/static/sql-set.html
          variables = @config[:variables] || {}
          variables.map do |k, v|
            if v == ':default' || v == :default
              # Sets the value to the global or compile default
              execute("SET SESSION #{k} TO DEFAULT", 'SCHEMA')
            elsif !v.nil?
              execute("SET SESSION #{k} TO #{quote(v)}", 'SCHEMA')
            end
          end
        end

        # Returns the current ID of a table's sequence.
        def last_insert_id(sequence_name) #:nodoc:
          Integer(last_insert_id_value(sequence_name))
        end

        def last_insert_id_value(sequence_name)
          last_insert_id_result(sequence_name).rows.first.first
        end

        def last_insert_id_result(sequence_name) #:nodoc:
          exec_query("SELECT currval('#{sequence_name}')", 'SQL')
        end

        # Returns the list of a table's column names, data types, and default values.
        #
        # The underlying query is roughly:
        #  SELECT column.name, column.type, default.value
        #    FROM column LEFT JOIN default
        #      ON column.table_id = default.table_id
        #     AND column.num = default.column_num
        #   WHERE column.table_id = get_table_id('table_name')
        #     AND column.num > 0
        #     AND NOT column.is_dropped
        #   ORDER BY column.num
        #
        # If the table name is not prefixed with a schema, the database will
        # take the first match from the schema search path.
        #
        # Query implementation notes:
        #  - format_type includes the column size constraint, e.g. varchar(50)
        #  - ::regclass is a function that gives the id for a table name
        def column_definitions(table_name) # :nodoc:
          exec_query(<<-end_sql, 'SCHEMA').rows
              SELECT a.attname, format_type(a.atttypid, a.atttypmod),
                     pg_get_expr(d.adbin, d.adrelid), a.attnotnull, a.atttypid, a.atttypmod
                FROM pg_attribute a LEFT JOIN pg_attrdef d
                  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
               WHERE a.attrelid = '#{quote_table_name(table_name)}'::regclass
                 AND a.attnum > 0 AND NOT a.attisdropped
               ORDER BY a.attnum
          end_sql
        end

        def extract_table_ref_from_insert_sql(sql) # :nodoc:
          sql[/into\s+([^\(]*).*values\s*\(/im]
          $1.strip if $1
        end

        def create_table_definition(name, temporary, options, as = nil) # :nodoc:
          PostgreSQL::TableDefinition.new native_database_types, name, temporary, options, as
        end
    end
  end
end
