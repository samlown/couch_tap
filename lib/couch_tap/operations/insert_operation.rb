
module CouchTap
  module Operations
    class InsertOperation
      attr_reader :table, :top_level, :primary_key, :id, :attributes

      def initialize(table, top_level, primary_key, id, attributes)
        @table = table
        @top_level = top_level
        @primary_key = primary_key
        @id = id
        @attributes = attributes
      end

      def ==(other)
        other.is_a?(InsertOperation) &&
          table == other.table &&
          top_level == other.top_level &&
          primary_key == other.primary_key &&
          id == other.id &&
          attributes == other.attributes
      end
    end
  end
end
