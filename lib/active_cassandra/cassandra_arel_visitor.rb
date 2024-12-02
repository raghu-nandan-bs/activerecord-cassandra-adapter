# lib/active_record/connection_adapters/cassandra_arel_visitor.rb
module ActiveRecord
  module ConnectionAdapters
    class CassandraArelVisitor < Arel::Visitors::ToSql
      # Handle SELECT queries
      def visit_Arel_Nodes_SelectManager(o, collector)
        cql = []
        cql << "SELECT"

        # Projections (columns to select)
        if o.projections.any?
          projections = o.projections.map { |p| accept p, collector }.join(", ")
          cql << projections
        else
          cql << "*"
        end

        # FROM clause
        cql << "FROM"
        cql << accept(o.source, collector)

        # WHERE clause
        if o.constraints.any?
          where_clause = o.constraints.map { |c| accept(c, collector) }.join(" AND ")
          cql << "WHERE #{where_clause}"
        end

        # ORDER BY clause
        if o.orders.any?
          order_clause = o.orders.map { |o| accept(o, collector) }.join(", ")
          cql << "ORDER BY #{order_clause}"
        end

        # LIMIT clause
        if o.limit
          cql << "LIMIT #{accept(o.limit, collector)}"
        end

        # OFFSET is not supported in CQL
        # You might need to handle pagination differently

        # Combine the CQL parts
        collector << cql.join(" ")
      end

      def visit_Arel_Nodes_Equality(o, collector)
        collector = accept(o.left, collector)
        collector << " = "
        collector = accept(o.right, collector)
      end

      # Handle other predicates like GreaterThan, LessThan, etc.
      def visit_Arel_Nodes_GreaterThan(o, collector)
        collector = accept(o.left, collector)
        collector << " > "
        collector = accept(o.right, collector)
      end

      def visit_Arel_Nodes_LessThan(o, collector)
        collector = accept(o.left, collector)
        collector << " < "
        collector = accept(o.right, collector)
      end

      # Handle LIKE
      def visit_Arel_Nodes_Like(o, collector)
        collector = accept(o.left, collector)
        collector << " LIKE "
        collector = accept(o.right, collector)
      end

      def visit_Arel_Attributes_Attribute(o, collector)
        collector << quote_column_name(o.name)
      end

      def visit_Arel_Nodes_Order(o, collector)
        direction = o.descending ? "DESC" : "ASC"
        collector << "#{accept(o.expr, collector)} #{direction}"
      end

      def visit_Arel_Nodes_Limit(o, collector)
        collector << o.expr.to_sql
      end

    end
  end
end
