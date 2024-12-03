require_relative 'tokenizer'
require_relative 'parser'

module SqlToCqlParser
  def self.to_cql(sql)
    tokenizer = Tokenizer.new(sql)
    tokens = tokenizer.tokenize
    parser = Parser.new(tokens)
    parser.parse
  end

  def self.tokens_to_cql(tokens)
    parser = Parser.new(tokens)
    parser.parse
  end
end