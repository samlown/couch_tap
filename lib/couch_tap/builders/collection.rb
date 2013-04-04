
module CouchTap

  module Builder

    #
    # Collection Builder. Go through each sub-table definition and recursively
    # prepare the data ready to be inserted into the database.
    #
    class Collection

      attr_reader :parent

      def initialize(parent, opts = {}, &block)
        @_tables = []
        @parent  = parent

        instance_eval(&block)
      end

      def execute
        @_tables.each do |table|
          table.execute
        end
      end

      def handler
        parent.handler
      end

      #### DSL Methods

      def table(name, opts = {}, &block)
        @_tables << Table.new(parent, name, opts, &block)
      end

    end
  end
end

