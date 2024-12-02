require_relative 'tokenizer'
require_relative 'parser'

module SqlToCqlParser
  def self.to_cql(sql)
    tokenizer = Tokenizer.new(sql)
    tokens = tokenizer.tokenize
    parser = Parser.new(tokens)
    parser.parse
  end
end