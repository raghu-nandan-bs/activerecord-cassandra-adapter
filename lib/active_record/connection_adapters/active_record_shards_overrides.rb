module ActiveRecordShards
  module ConnectionSwitcher
    def connection_specification_name
      puts "[ActiveRecordShards - Override] connection_specification_name"
      super if defined?(super)
      name
    end
  end

  class ShardSelectionOverrides
    def resolve_connection_name(sharded: is_sharded?, configurations: configurations)
      puts "[ActiveRecordShards - Override] resolve_connection_name"
      super if defined?(super)
      name
    end
  end

  ActiveRecordShards::ShardSelection.prepend(ShardSelectionOverrides)
end