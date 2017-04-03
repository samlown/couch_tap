
module CouchTap
  module Operations
    class InsertOperation
      attr_reader :table, :top_level, :id, :attributes

      def initialize(table, top_level, id, attributes)
        @table = table
        @top_level = top_level
        @id = id
        @attributes = attributes
      end
    end
  end
end
