
module CouchTap

  module Destroyers

    #
    # Collection Destroyer. Go through each sub-table definition and remove
    # all references to the parent document.
    #
    class Collection

      attr_reader :parent

      def initialize(parent, opts = {}, &block)
        @_tables = []
        @parent  = parent

        instance_eval(&block)
      end

      def execute(query_executor)
        # Just go through each table and ask it to execute itself
        @_tables.each do |table|
          table.execute(query_executor)
        end
      end

      #### DSL Methods

      def table(name, opts = {}, &block)
        @_tables << Table.new(parent, name, opts, &block)
      end

    end
  end
end
