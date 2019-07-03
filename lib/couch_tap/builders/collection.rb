
module CouchTap

  module Builders

    #
    # Collection Builder. Go through each sub-table definition and recursively
    # prepare the data ready to be inserted into the database.
    #
    class Collection

      attr_reader :parent, :field

      def initialize(parent, field, opts = {}, &block)
        @_tables = []
        @parent  = parent
        @field   = field

        instance_eval(&block)
      end

      def execute(operations_queue)
        @_tables.each do |table|
          table.execute(operations_queue)
        end
      end

      #### DSL Methods

      def table(name, opts = {}, &block)
        source = parent.data[field.to_s] || []
        source = [source] if source.is_a?(Hash)
        source.each do |item|
          options = opts.merge(:data => item)
          @_tables << Table.new(parent, name, options, &block)
        end
      end

    end
  end
end

