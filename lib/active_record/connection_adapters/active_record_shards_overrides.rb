require 'active_record_shards/connection_switcher-6-1'

module CassandraActiveRecordShardOverride
  module CassandraConnectionSwitcherOverride
    def connection_specification_name
      puts "[Cassandra Driver - Override] connection_specification_name"
      super if defined?(super)
    end
  end

end

ActiveRecordShards::ConnectionSwitcher.prepend(CassandraActiveRecordShardOverride::CassandraConnectionSwitcherOverride)
