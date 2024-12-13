module ActiveRecord
  module ConnectionAdapters
    class CassandraAdapter
      # ----------------- connection related overrides -----------------
      class ConnectionPool < ActiveRecord::ConnectionAdapters::ConnectionPool
        def clear_active_connections!
          # no-op
        end

        def establish_connection
          # no-op
        end

        def active_connection_name
          @connection.keyspace
        end
      end # class ConnectionPool
    end # class CassandraAdapter
  end # module ConnectionAdapters
end # module ActiveRecord