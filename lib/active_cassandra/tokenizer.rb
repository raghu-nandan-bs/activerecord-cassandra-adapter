# lib/sql_to_cql_parser/tokenizer.rb
module SqlToCqlParser
  class Token
    attr_reader :type, :value

    def initialize(type, value)
      @type = type
      @value = value
    end

    def to_s
      "#{type}: #{value}"
    end
  end

  class Tokenizer
    KEYWORDS = %w[
      CREATE TABLE ALTER DROP SELECT INSERT INTO VALUES
       UPDATE DELETE PRIMARY KEY USING WITH FROM WHERE LIMIT ORDER BY AND OR
      IF EXISTS
    ].freeze

    SYMBOLS = %w[( ) ? , ; = .].freeze

    def initialize(input)
      @input = input.strip
      @position = 0
      @tokens = []
    end

    def tokenize
      while current_char
        puts "current_char: #{current_char}\n"
        if whitespace?(current_char)
          advance
        elsif comment_start?
          skip_comment
        elsif symbol?(current_char)
          @tokens << Token.new(:symbol, current_char)
          advance
        elsif string_start?
          @tokens << Token.new(:literal, parse_string)
        elsif digit?(current_char)
          @tokens << Token.new(:number, parse_number)
        else
          word = parse_word
          if KEYWORDS.include?(word.upcase)
            @tokens << Token.new(:keyword, word.upcase)
          else
            @tokens << Token.new(:identifier, word)
          end
        end
        puts "tokens so far: #{@tokens.inspect}"
      end
      @tokens
    end

    private

    def current_char
      @input[@position]
    end

    def peek_char
      @input[@position + 1]
    end

    def advance
      @position += 1
    end

    def whitespace?(char)
      char =~ /\s/
    end

    def comment_start?
      current_char == '/' && peek_char == '*'
      puts "found comment begin..."
    end

    def skip_comment
      while current_char && current_char != "*" && peek_char != '/'
        advance
      end
      puts "skipping comment..."
      puts "current_char: #{current_char}"
      puts "peek_char: #{peek_char}"
      advance
    end

    def symbol?(char)
      SYMBOLS.include?(char)
    end

    def string_start?
      current_char == "'" || current_char == '"'
    end

    def parse_string
      quote = current_char
      advance
      start_pos = @position
      while current_char && current_char != quote
        if current_char == '\\' && peek_char == quote
          advance # Skip the escape character
        end
        advance
      end
      string = @input[start_pos...@position]
      advance # Skip the closing quote
      string.gsub("\\#{quote}", quote)
    end

    def digit?(char)
      char =~ /\d/
    end

    def parse_number
      start_pos = @position
      while current_char && digit?(current_char)
        advance
      end
      @input[start_pos...@position]
    end

    def parse_word
      start_pos = @position
      while current_char && (current_char =~ /[A-Za-z0-9_\.\*]/)
        advance
      end
      word = @input[start_pos...@position]

      # if current_char == '.' && peek_char && peek_char =~ /[A-Za-z_]/
      #   @tokens << Token.new(:identifier, word)
      #   advance  # Skip the dot
      #   @tokens << Token.new(:symbol, '.')
      #   word = parse_word  # Parse the table name after the dot
      # end

      word
    end
  end
end
