# lib/sql_to_cql_parser/parser.rb
module SqlToCqlParser
  class Parser
    def initialize(tokens)
      @tokens = tokens
      @position = 0
    end

    def parse
      statement = parse_statement
      translate_to_cql(statement)
    end

    private

    def current_token
      @tokens[@position]
    end

    def peek_token
      @tokens[@position + 1]
    end

    def advance
      @position += 1
    end

    def expect(type, value = nil)
      token = current_token
      if token.nil? || token.type != type || (value && token.value != value)
        raise "Expected #{type} #{value}, but got #{token&.type} #{token&.value}"
      end
      advance
      token
    end

    def parse_statement
      token = current_token
      case token&.type
      when :keyword
        case token.value
        when 'CREATE'
          parse_create
        when 'ALTER'
          parse_alter
        when 'DROP'
          parse_drop
        when 'SELECT'
          parse_select
        when 'INSERT'
          parse_insert
        when 'UPDATE'
          parse_update
        when 'DELETE'
          parse_delete
        else
          raise "Unsupported SQL statement: #{token.value}"
        end
      else
        raise "Unsupported SQL statement starting with: #{token&.value}"
      end
    end

    def parse_create
      expect(:keyword, 'CREATE')
      expect(:keyword, 'TABLE')
      table_name = expect(:identifier).value
      expect(:symbol, '(')
      columns = []
      primary_key = []
      while current_token && current_token.type != :symbol || current_token.value != ')'
        if current_token.type == :keyword && current_token.value == 'PRIMARY'
          primary_key = parse_primary_key
        else
          column = parse_column
          columns << column
        end
        if current_token && current_token.type == :symbol && current_token.value == ','
          expect(:symbol, ',')
        else
          break
        end
      end
      expect(:symbol, ')')
      expect(:symbol, ';') if current_token && current_token.value == ';'
      { type: 'CREATE_TABLE', table_name: table_name, columns: columns, primary_key: primary_key }
    end

    def parse_primary_key
      expect(:keyword, 'PRIMARY')
      expect(:keyword, 'KEY')
      expect(:symbol, '(')
      key_columns = []
      while current_token && current_token.type != :symbol || current_token.value != ')'
        key_columns << expect(:identifier).value
        if current_token && current_token.type == :symbol && current_token.value == ','
          expect(:symbol, ',')
        else
          break
        end
      end
      expect(:symbol, ')')
      key_columns
    end

    def parse_column
      name = expect(:identifier).value
      type = expect(:identifier).value.downcase.to_sym
      constraints = {}
      while current_token && (current_token.type == :keyword || current_token.type == :symbol)
        case current_token.value
        when 'NOT'
          expect(:keyword, 'NOT')
          expect(:keyword, 'NULL')
          constraints[:null] = false
        when 'DEFAULT'
          expect(:keyword, 'DEFAULT')
          value = parse_default_value
          constraints[:default] = value
        else
          break
        end
      end
      { name: name, type: type, constraints: constraints }
    end

    def parse_default_value
      token = current_token
      case token.type
      when :literal
        value = token.value
        expect(:literal)
        value
      when :number
        value = token.value.to_i
        expect(:number)
        value
      else
        raise "Unsupported default value: #{token.value}"
      end
    end

    def parse_alter
      # Implement ALTER TABLE parsing if needed
      raise "ALTER TABLE parsing not implemented yet."
    end

    def parse_drop
      expect(:keyword, 'DROP')
      expect(:keyword, 'TABLE')
      table_name = expect(:identifier).value
      expect(:symbol, ';') if current_token && current_token.value == ';'
      { type: 'DROP_TABLE', table_name: table_name }
    end

    def parse_select_columns

      if current_token.type == :symbol && current_token.value == '*'
        expect(:symbol, '*')
        return ['*']
      else
        columns = []

        while current_token && !(current_token.type == :keyword && %w(FROM WHERE LIMIT ORDER).include?(current_token.value.upcase)) && !(current_token.type == :symbol && current_token.value == ';')
          if current_token.type == :identifier
            column_name = expect(:identifier).value
            # Remove tablename. part if present
            column_name = column_name.split('.').last
            columns << column_name
          else
            raise "Unexpected token in SELECT columns: #{current_token.type} #{current_token.value}"
          end
          break unless current_token && current_token.type == :symbol && current_token.value == ','
          expect(:symbol, ',')
        end
        columns
      end
    end

    def parse_where
      expect(:keyword, 'WHERE')
      conditions = []
      while current_token && !(current_token.type == :keyword && %w(LIMIT ORDER).include?(current_token.value.upcase)) && !(current_token.type == :symbol && current_token.value == ';')
        left = expect(:identifier).value
        operator = expect(:symbol, '=').value # Simplistic: only handling '=' operator
        right = parse_condition_value
        conditions << { left: left, operator: operator, right: right }
        break unless current_token && current_token.type == :keyword && current_token.value.upcase == 'AND'
        expect(:keyword, 'AND')
      end
      conditions
    end


    def parse_condition_value
      token = current_token
      case token.type
      when :symbol
        if token.value == '?'
          expect(:symbol, '?')
          return '?'
        end
      when :literal
        value = "'#{token.value}'"
        expect(:literal)
        value
      when :number
        value = token.value.to_i
        expect(:number)
        value
      when :keyword
        if ['TRUE', 'FALSE'].include?(token.value.upcase)
          value = token.value.downcase == 'true'
          expect(:keyword)
          value
        else
          raise "Unsupported condition value keyword: #{token.value}"
        end
      when :identifier
        value = token.value
        expect(:identifier)
        value
      else
        raise "Unsupported condition value type: #{token.type}"
      end
    end

    def parse_limit
      expect(:keyword, 'LIMIT')
      limit = expect(:number).value.to_i
      limit
    end

    def parse_order_by
      expect(:keyword, 'ORDER')
      expect(:keyword, 'BY')
      column = expect(:identifier).value
      direction = 'ASC' # Default direction
      if current_token&.type == :keyword && %w(ASC DESC).include?(current_token.value.upcase)
        direction = expect(:keyword).value.upcase
      end
      { column: column, direction: direction }
    end

    def parse_select
      expect(:keyword, 'SELECT')
      columns = parse_select_columns
      expect(:keyword, 'FROM')
      table_name = expect(:identifier).value
      where_clause = nil
      limit = nil
      order_by = nil

      if current_token&.type == :keyword && current_token.value.upcase == 'WHERE'
        where_clause = parse_where
      end

      if current_token&.type == :keyword && current_token.value.upcase == 'LIMIT'
        limit = parse_limit
      end

      if current_token&.type == :keyword && current_token.value.upcase == 'ORDER'
        order_by = parse_order_by
      end

      expect(:symbol, ';') if current_token && current_token.value == ';'

      puts "result: #{columns}, #{table_name}, #{where_clause}, #{limit}, #{order_by}"

      {
        type: 'SELECT',
        columns: columns,
        table_name: table_name,
        where: where_clause,
        limit: limit,
        order_by: order_by
      }
    end

    def parse_insert
      expect(:keyword, 'INSERT')
      expect(:keyword, 'INTO')
      table_name = expect(:identifier).value

      # Handle column list
      expect(:symbol, '(')
      columns = []
      while current_token && current_token.type != :symbol || current_token.value != ')'
        columns << expect(:identifier).value
        if current_token && current_token.type == :symbol && current_token.value == ','
          expect(:symbol, ',')
        else
          break
        end
      end
      expect(:symbol, ')')

      # Handle VALUES
      expect(:keyword, 'VALUES')
      expect(:symbol, '(')
      values = []
      while current_token && current_token.type != :symbol || current_token.value != ')'
        values << parse_condition_value
        if current_token && current_token.type == :symbol && current_token.value == ','
          expect(:symbol, ',')
        else
          break
        end
      end
      expect(:symbol, ')')

      expect(:symbol, ';') if current_token && current_token.value == ';'

      {
        type: 'INSERT',
        table_name: table_name,
        columns: columns,
        values: values
      }
    end

    def parse_update
      # Implement UPDATE parsing if needed
      raise "UPDATE parsing not implemented yet."
    end

    def parse_delete
      # Implement DELETE parsing if needed
      raise "DELETE parsing not implemented yet."
    end

    def translate_to_cql(statement)
      puts "translate_to_cql: #{statement.inspect}"
      case statement[:type]
      when 'CREATE_TABLE'
        translate_create_table(statement)
      when 'DROP_TABLE'
        translate_drop_table(statement)
      when 'SELECT'
        translate_select(statement)
      when 'INSERT'
        translate_insert(statement)
      else
        raise "Unsupported statement type: #{statement[:type]}"
      end
    end

    def translate_create_table(statement)
      table_name = statement[:table_name]
      columns = statement[:columns].map do |col|
        col_def = "#{col[:name]} #{map_sql_type_to_cql(col[:type])}"
        col_def += " NOT NULL" unless col[:constraints][:null].nil? || col[:constraints][:null]
        col_def += " DEFAULT #{format_default(col[:constraints][:default], col[:type])}" if col[:constraints][:default]
        col_def
      end
      primary_key = statement[:primary_key]
      cql = "CREATE TABLE #{quote_ident(table_name)} (\n  #{columns.join(",\n  ")}"
      if primary_key.any?
        cql += ",\n  PRIMARY KEY (#{primary_key.map { |k| quote_ident(k) }.join(", ")})"
      end
      cql += "\n);"
      cql
    end

    def translate_drop_table(statement)
      table_name = statement[:table_name]
      "DROP TABLE #{quote_ident(table_name)};"
    end

    def translate_select(statement)
      puts "translate_select: #{statement.inspect}"
      columns = statement[:columns]
      table_name = statement[:table_name]
      where_clause = statement[:where]
      limit = statement[:limit]
      order_by = statement[:order_by]

      cql = "SELECT #{columns.join(', ')} FROM #{table_name}"

      cql += " WHERE #{where_clause.map { |cond| "#{cond[:left]} = #{cond[:right]}" }.join(' AND ')}" if where_clause
      cql += " LIMIT #{limit}" if limit
      cql += ";"
      cql
    end

    def translate_insert(statement)
      puts "translate_insert: #{statement.inspect}"
      table_name = statement[:table_name]
      columns = statement[:columns]
      values = statement[:values]
      cql = "INSERT INTO #{table_name} (#{columns.join(', ')}) VALUES (#{values.join(', ')})"
      cql
    end

    def map_sql_type_to_cql(sql_type)
      case sql_type
      when :string, :varchar
        'text'
      when :integer, :int
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

    def format_default(default, type)
      case type
      when :string, :varchar
        "'#{default}'"
      when :integer, :int, :bigint, :float, :decimal
        default.to_s
      when :boolean
        default ? 'true' : 'false'
      when :datetime, :timestamp, :date, :time
        "'#{default}'" # CQL expects timestamp literals in quotes
      when :uuid
        "'#{default}'"
      when :binary
        "0x#{default}" # CQL represents blobs as hexadecimal
      else
        "'#{default}'"
      end
    end

    def quote_ident(identifier)
      "\"#{identifier}\""
    end
  end
end
