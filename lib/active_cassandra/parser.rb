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

    def parse_select
      # Implement SELECT parsing if needed
      raise "SELECT parsing not implemented yet."
    end

    def parse_insert
      # Implement INSERT parsing if needed
      raise "INSERT parsing not implemented yet."
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
      case statement[:type]
      when 'CREATE_TABLE'
        translate_create_table(statement)
      when 'DROP_TABLE'
        translate_drop_table(statement)
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
