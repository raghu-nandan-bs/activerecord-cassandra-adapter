
require 'active_record'
require 'active_record/base'

module CassandraActiveRecordShardOverride
  module CassandraConnectionSwitcherOverride
    def connection_specification_name
      # Is this cassandra application record base class?
      if @connection_specification_name && @connection_specification_name.to_s.start_with?('Cassandra')
        return @connection_specification_name
      # or does it directly inherit from the cassandra application record base class?
      elsif self.superclass && self.superclass.respond_to?(:connection_specification_name) \
         && self.superclass.connection_specification_name.to_s.start_with?('Cassandra')
        return self.superclass.connection_specification_name
      else
        super
      end
    end
  end
end
