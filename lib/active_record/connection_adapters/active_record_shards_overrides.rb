
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
      # Is this cassandra application record base class?
      if @connection_specification_name && @connection_specification_name.to_s.start_with?('Cassandra')
        puts "returning @connection_specification_name: #{@connection_specification_name}"
        return @connection_specification_name
      # or does it directly inherit from the cassandra application record base class?
      elsif self.class.superclass && self.class.superclass.respond_to?(:connection_specification_name) \
         && self.class.superclass.connection_specification_name.to_s.start_with?('Cassandra')
        puts "returning self.superclass.connection_specification_name: #{self.class.superclass.connection_specification_name}"
        return self.class.superclass.connection_specification_name
      else
        super
      end
    end
  end
end

#if defined?(ActiveRecordShards::ConnectionSwitcher)
#  puts "[DEBUG] Found ActiveRecordShards::ConnectionSwitcher, applying override"
#  # ActiveRecordShards::ConnectionSwitcher.prepend(CassandraActiveRecordShardOverride::CassandraConnectionSwitcherOverride)
#  ActiveRecord::Base.extend(CassandraActiveRecordShardOverride::CassandraConnectionSwitcherOverride)
#else
#  puts "[DEBUG] WARNING: ActiveRecordShards::ConnectionSwitcher not found!"
#end
#
#puts "[DEBUG] Override definition completed"