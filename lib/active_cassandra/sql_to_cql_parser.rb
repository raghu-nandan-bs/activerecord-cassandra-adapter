require_relative 'tokenizer'
require_relative 'parser'

module SqlToCqlParser
  def self.to_cql(sql)
    tokenizer = Tokenizer.new(sql)
    tokens = tokenizer.tokenize
    puts ">>>>>>>>>>>>>>>"
    puts "tokens: #{tokens.inspect}"
    parser = Parser.new(tokens)
    parser.parse
  end

  def self.translate_to_cql(tokens)
    puts "<<<<<<<<<<<<<<<<"
    puts "tokens: #{tokens.inspect}"
    Parser.to_cql(tokens)
  end
end