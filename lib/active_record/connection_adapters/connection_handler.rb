module ActiveRecord
  module ConnectionAdapters
    class CassandraAdapter
      class ConnectionHandler < ActiveRecord::ConnectionAdapters::ConnectionHandler
        private
        def new_connection_pool(db_config, **kwargs)
          # Return an instance of your custom pool class here.
          CassandraAdapter::ConnectionPool.new(db_config, **kwargs)
        end
      end
    end # class CassandraAdapter
  end # module ConnectionAdapters
end # module ActiveRecord