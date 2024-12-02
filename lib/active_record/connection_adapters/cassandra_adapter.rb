require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'cassandra'
require 'active_cassandra/cf'
require 'active_cassandra/sqlparser.tab'
require 'active_cassandra/cassandra_arel_visitor'

module ActiveRecord
  class Base
    def self.cassandra_connection(config)
      # config.symbolize_keys!
      host = config[:host] || '127.0.1.1'
      port = config[:port] || 9042

      unless (keyspace = config[:keyspace] || config[:database])
        raise ArgumentError, "No database file specified. Missing argument: keyspace"
      end
      client = Cassandra.cluster(
        hosts:  ["#{host}"]
      )
      # client.each_host do |host| # automatically discovers all peers
      #   puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
      # end
      session = client.connect(keyspace)
      ConnectionAdapters::CassandraAdapter.new(session, logger, config)
    end
  end # class Base

  module ConnectionAdapters
    class CassandraAdapter < AbstractAdapter
      def initialize(client, logger, config)
        super(client, logger)
        @config = config
        @connection = client
      end

      def supports_count_distinct?
        false
      end

      def exec_query(sql, name = nil, binds = [], prepare: false)
        # parsed_sql = ActiveCassandra::SQLParser.new(sql).parse
        puts "sql to execute: #{sql}"
        @connection.execute(sql)
      end

      def current_keyspace
          # You should have stored the current keyspace during connection
          @connection.keyspace
      end

      def data_source_sql(table_name, type: "BASE TABLE")
        #escaped_table_name = table_name.gsub("'", "''")
        #escaped_keyspace = @current_keyspace.gsub("'", "''")
        <<-CQL
          SELECT table_name
          FROM system_schema.tables
            WHERE table_name = '#{table_name}';
        CQL
      end

      def to_cql(ast)
        visitor = CassandraArelVisitor.new(self)
        visitor.accept(ast).to_sql
      end



      def select(sql, name = nil)
        log(sql, name)

        parsed_sql = ActiveCassandra::SQLParser.new(sql).parse

        cf = parsed_sql[:table].to_sym
        cond = parsed_sql[:condition]
        count = parsed_sql[:count]
        # not implemented:
        # distinct = parsed_sql[:distinct]
        sqlopts, casopts = rowopts(parsed_sql)

        if count and cond.empty? and sqlopts.empty?
          [{count => @connection.count_range(cf, casopts)}]
        elsif is_id?(cond)
          ks = [cond].flatten
          @connection.multi_get(cf, ks, casopts).values
        else
          rows = @connection.get_range(cf, casopts).select {|i| i.columns.length > 0 }.map do |key_slice|
            key_slice_to_hash(key_slice)
          end

          unless cond.empty?
            rows = filter(cond).call(rows)
          end

          if (offset = sqlopts[:offset])
            rows = rows.slice(offset..-1)
          end

          if (limit = sqlopts[:limit])
            rows = rows.slice(0, limit)
          end

          count ? [{count => rows.length}] : rows
        end
      end




      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
        log(sql, name)

        parsed_sql = ActiveCassandra::SQLParser.new(sql).parse
        table = parsed_sql[:table]
        cf = table.to_sym
        column_list = parsed_sql[:column_list]
        value_list = parsed_sql[:value_list]

        class_name = ActiveRecord::Base.class_name(table)
        rowid = Module.const_get(class_name).__identify.to_s

        nvs = {}
        column_list.zip(value_list).each {|n, v| nvs[n] = v.to_s }

        @connection.insert(cf, rowid, nvs)

        return rowid
      end

      def update_sql(sql, name = nil)
        log(sql, name)
        parsed_sql = ActiveCassandra::SQLParser.new(sql).parse
        cf = parsed_sql[:table].to_sym
        cond = parsed_sql[:condition]

        nvs = {}
        parsed_sql[:set_clause_list].each do |n, v|
          n = n.split('.').last
          nvs[n] = v.to_s
        end

        n = 0

        if is_id?(cond)
          ks = [cond].flatten
          rs = @connection.multi_get(cf, ks)

          ks.each do |key|
            row = rs[key]
            @connection.insert(cf, key, row.merge(nvs))
            n += 1
          end
        else
          rows = @connection.get_range(cf).select {|i| i.columns.length > 0 }.map do |key_slice|
            key_slice_to_hash(key_slice)
          end

          unless cond.empty?
            rows = filter(cond).call(rows)
          end

          rows.each do |row|
            @connection.insert(cf, row['id'], row.merge(nvs))
            n += 1
          end
        end

        return n
      end

      def delete_sql(sql, name = nil)
        log(sql, name)

        parsed_sql = ActiveCassandra::SQLParser.new(sql).parse
        cf = parsed_sql[:table].to_sym
        cond = parsed_sql[:condition]

        n = 0

        if is_id?(cond)
          [cond].flatten.each do |key|
            @connection.remove(cf, key)
            n += 1
          end
        else
          rows = @connection.get_range(cf).select {|i| i.columns.length > 0 }

          unless cond.empty?
            rows = rows.map {|i| key_slice_to_hash(i) }
            rows = filter(cond).call(rows)

            rows.each do |row|
              @connection.remove(cf, row['id'])
              n += 1
            end
          else
            rows.each do |key_slice|
              @connection.remove(cf, key_slice.key)
              n += 1
            end
          end
        end

        return n
      end

      def add_limit_offset!(sql, options)
        if (limit = options[:limit])
          if limit.kind_of?(Numeric)
            sql << " LIMIT #{limit.to_i}"
          else
            sql << " LIMIT #{quote(limit)}"
          end
        end

        if (offset = options[:offset])
          if offset.kind_of?(Numeric)
            sql << " OFFSET #{offset.to_i}"
          else
            sql << " OFFSET #{quote(offset)}"
          end
        end
      end

      private
      def key_slice_to_hash(key_slice)
        hash = {'id' => key_slice.key}

        key_slice.columns.each do |i|
          column = i.column
          hash[column.name] = column.value
        end

        return hash
      end

      def is_id?(cond)
        not cond.kind_of?(Array) or not cond.all? {|i| i.kind_of?(Hash) }
      end

      def filter(cond)
        fs = []

        cond.each do |c|
          name, op, expr, has_not = c.values_at(:name, :op, :expr, :not)
          name = name.split('.').last
          expr = Regexp.compile(expr) if op == '$regexp'

          func = case op
                 when '$in'
                   lambda {|i| expr.include?(i) }
                 when '$bt'
                   lambda {|i| expr[0] <= i and i <= expr[1] }
                 when '$regexp'
                   lambda {|i| i =~ Regexp.compile(expr) }
                 when :'>=', :'<=', :'>', :'<'
                   lambda {|i| i.to_i.send(op, expr.to_i) }
                 else
                   lambda {|i| i.send(op, expr) }
                 end

          fs << (has_not ? lambda {|row| not func.call(row[name]) } : lambda {|row| func.call(row[name])})
        end

        lambda do |rows|
          fs.inject(rows) {|r, f| r.select {|i| f.call(i) } }
        end
      end

      def rowopts(parsed_sql)
        order, limit, offset = parsed_sql.values_at(:order, :limit, :offset)
        sqlopts = {}
        casopts = {}

        # not implemented:
        # if order
        #   name, type = order.values_at(:name, :type)
        #   ...
        # end

        if offset
          if offset.kind_of?(Numeric)
            sqlopts[:offset] = offset
          else
            # XXX: offset is not equals to SQL OFFSET
            casopts[:start] = offset
          end
        end

        if limit
          if limit.kind_of?(Numeric)
            sqlopts[:limit] = limit
          else
            # XXX: limit is not equals to SQL LIMIT
            casopts[:finish] = limit
          end
        end
        return [sqlopts, casopts]
      end

      class TableDefinition
        attr_reader :columns

        def initialize(adapter, table_name, options = {})
          @adapter = adapter
          @table_name = table_name
          @primary_key = options[:primary_key]
          @id = options[:id]
          @columns = []
        end

        # Define a column
        def column(name, type, options = {})
          @columns << ColumnDefinition.new(name, type, options)
        end

        # Define shorthand methods for common types
        def string(name, options = {})
          column(name, :string, options)
        end

        # supposed to create `created_at` and `updated_at` columns
        def timestamps(options = {})
          column('created_at', :timestamp, options)
          column('updated_at', :timestamp, options)
        end

        def text(name, options = {})
          column(name, :text, options)
        end

        def integer(name, options = {})
          column(name, :integer, options)
        end

        def bigint(name, options = {})
          column(name, :bigint, options)
        end

        def float(name, options = {})
          column(name, :float, options)
        end

        def decimal(name, options = {})
          column(name, :decimal, options)
        end

        def boolean(name, options = {})
          column(name, :boolean, options)
        end

        def datetime(name, options = {})
          column(name, :datetime, options)
        end

        def timestamp(name, options = {})
          column(name, :timestamp, options)
        end

        def date(name, options = {})
          column(name, :date, options)
        end

        def time(name, options = {})
          column(name, :time, options)
        end

        def uuid(name, options = {})
          column(name, :uuid, options)
        end

        def binary(name, options = {})
          column(name, :binary, options)
        end

        def json(name, options = {})
          column(name, :json, options)
        end

        def jsonb(name, options = {})
          column(name, :jsonb, options)
        end

        # Handle indexes or other table-level options if needed
      end

      # ColumnDefinition class to represent a single column
      class ColumnDefinition
        attr_reader :name, :type, :options

        def initialize(name, type, options = {})
          @name = name
          @type = type
          @options = options
        end

        def null
          options.fetch(:null, true)
        end

        def default
          options[:default]
        end
      end # class ColumnDefinition

      public
      def create_table(table_name, options = {})
        options[:force] = true if options[:force].nil?

        # Handle table options
        table_options = options[:options] || ''

        # Initialize column definitions array
        columns_cql = []

        # Handle primary key options
        primary_key = options[:primary_key] || 'id'
        primary_key_type = options[:id] || :uuid

        # Add primary key column
        columns_cql << "#{quote_column_name(primary_key)} #{map_type(primary_key_type)} PRIMARY KEY"

        # Extract columns from the block
        if block_given?
          # Capture the table definition
          table_definition = TableDefinition.new(self, table_name, primary_key: primary_key, id: false)
          yield table_definition

          # Iterate over defined columns
          table_definition.columns.each do |column|
            columns_cql << column_to_cql(column)
          end
        end

        # Construct the CQL statement
        cql = "CREATE TABLE #{quote_table_name(table_name)} (\n  #{columns_cql.join(",\n  ")}\n) #{table_options};"

        # Execute the CQL statement
        @connection.execute(cql)
      end

      private

      # Convert a column definition to CQL
      def column_to_cql(column)
        "#{quote_column_name(column.name)} #{map_type(column.type)}#{null_constraint(column)}#{default_value(column)}"
      end

      # Handle NULL constraints (Cassandra treats columns as nullable by default)
      def null_constraint(column)
        column.null ? '' : ' NOT NULL'
      end

      # Handle default values
      def default_value(column)
        return '' unless column.default

        " DEFAULT #{format_default(column.default, column.type)}"
      end

      # Format default values based on type
      def format_default(value, type)
        case type
        when :string, :text, :uuid
          "'#{value}'"
        when :integer, :bigint, :float, :decimal
          value.to_s
        when :boolean
          value ? 'true' : 'false'
        when :datetime, :timestamp
          # Cassandra requires timestamps in specific formats
          # You might need to handle this appropriately
          "'#{value}'"
        else
          "'#{value}'" # Fallback to string
        end
      end

      # Example type mapping (extend as needed)
      def map_type(type)
        case type.to_sym
        when :string, :text
          'text'
        when :integer
          'int'
        when :bigint
          'bigint'
        when :float
          'float'
        when :decimal
          'decimal'
        when :boolean
          'boolean'
        when :datetime, :timestamp
          'timestamp'
        when :date
          'date'
        when :time
          'time'
        when :uuid
          'uuid'
        when :binary
          'blob'
        else
          'text' # Default to text for unknown types
        end
      end

      def column_definitions(table_name)
        keyspace = current_keyspace
        table = table_name.to_s

        cql = <<-CQL
          SELECT column_name, type, kind
          FROM system_schema.columns
          WHERE keyspace_name = '#{escape_cql(keyspace)}'
            AND table_name = '#{escape_cql(table)}';
        CQL

        result = @connection.execute(cql)

        # Process the result into an array of field hashes
        fields = result.map do |row|
          {
            name: row['column_name'],
            type: row['type'],
            kind: row['kind']
          }
        end

        fields
      rescue Cassandra::Errors::InvalidError => e
        raise ActiveRecord::StatementInvalid.new(e.message)
      end

      # Override to create a new column from field metadata
      def new_column_from_field(table_name, field)
        name = field[:name]
        type = map_type(field[:type])
        default = nil
        is_null = determine_null_constraint(table_name, field)

        type.define_singleton_method(:deduplicate) { self }
        type.define_singleton_method(:sql_type) { self }
        Column.new(name, default, type, is_null)
      end

      def escape_cql(identifier)
        identifier.gsub("'", "''")
      end

      def extract_default(field)
        return nil if field[:kind] == 'none'

        case field[:type].downcase
        when 'text', 'varchar', 'uuid', 'blob'
          field[:default_expression].gsub("'", "")
        when 'int', 'bigint', 'float', 'decimal'
          field[:default_expression].to_i
        when 'boolean'
          field[:default_expression] == 'true'
        when 'timestamp', 'datetime', 'date', 'time'
          begin
            Time.parse(field[:default_expression])
          rescue ArgumentError
            nil
          end
        else
          field[:default_expression]
        end
      end

      # Determine if the column has a NOT NULL constraint
      def determine_null_constraint(table_name, field)
        # Since ScyllaDB doesn't provide null constraints in system_schema.columns,
        # we need to infer it based on how the migration was defined.
        # One approach is to track non-null constraints during migration parsing
        # and store them in a separate data structure.
        # For simplicity, assume all columns are nullable unless they are part of the primary key.

        primary_keys = primary_keys(table_name)
        !primary_keys.include?(field[:name])
      end

      def primary_keys(table_name)
        keyspace = current_keyspace
        table = table_name.to_s

        cql = <<-CQL
          SELECT column_name, kind, position
          FROM system_schema.columns
          WHERE keyspace_name = '#{escape_cql(keyspace)}'
            AND table_name = '#{escape_cql(table)}'
            AND kind IN ('partition_key', 'clustering_key')
          ALLOW FILTERING;
        CQL

        result = @connection.execute(cql)

        # Sort partition keys first, then clustering keys based on position
        partition_keys = result.select { |row| row['kind'] == 'partition_key' }.sort_by { |row| row['position'] }
        clustering_keys = result.select { |row| row['kind'] == 'clustering_key' }.sort_by { |row| row['position'] }

        # Combine partition and clustering keys
        (partition_keys + clustering_keys).map { |row| row['column_name'] }
      rescue Cassandra::Errors::InvalidError => e
        raise ActiveRecord::StatementInvalid.new(e.message)
      end

    end # class CassandraAdapter
  end # module ConnectionAdapters
end # module ActiveRecord