module ActiveRecordShards
  module ConnectionSwitcher
    def connection_specification_name
      puts "[ActiveRecordShards - Override] connection_specification_name"
      super if defined?(super)
      name
    end
  end
end