
module CouchTap
  module Operations
    class DeleteOperation
      attr_reader :table, :top_level, :id, :primary_key

      def initialize(table, top_level, primary_key, id)
        @table = table
        @top_level = top_level
        @id = id
        @primary_key = primary_key
      end

      def ==(other)
        other.is_a?(DeleteOperation) &&
          table == other.table &&
          top_level == other.top_level &&
          id == other.id &&
          primary_key == other.primary_key
      end
    end
  end
end
