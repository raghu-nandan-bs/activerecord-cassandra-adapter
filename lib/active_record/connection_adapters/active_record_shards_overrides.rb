
require 'active_record'
require 'active_record/base'


puts "[DEBUG] Starting override definition"

module CassandraActiveRecordShardOverride
  module CassandraConnectionSwitcherOverride
    def self.included(base)
      puts "[DEBUG] Module included in #{base}"
    end

    def self.prepended(base)
      puts "[DEBUG] Module prepended to #{base}"
    end

    def connection_specification_name
      puts "[Cassandra Driver - Override] connection_specification_name"
      #puts "superclass.connection_specification_name: #{superclass.connection_specification_name}"
      puts "@connection_specification_name: #{@connection_specification_name}"

      super if defined?(super)
    end
  end
end

if defined?(ActiveRecordShards::ConnectionSwitcher)
  puts "[DEBUG] Found ActiveRecordShards::ConnectionSwitcher, applying override"
  # ActiveRecordShards::ConnectionSwitcher.prepend(CassandraActiveRecordShardOverride::CassandraConnectionSwitcherOverride)
  ActiveRecord::Base.extend(CassandraActiveRecordShardOverride::CassandraConnectionSwitcherOverride)
else
  puts "[DEBUG] WARNING: ActiveRecordShards::ConnectionSwitcher not found!"
end

puts "[DEBUG] Override definition completed"