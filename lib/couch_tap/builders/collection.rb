
module CouchTap

  module Builder
    class Collection

      attr_reader :handler, :parent, :field, :data

      def initialize(handler, parent, field, opts = {}, &block)
        @handler = handler
        @parent  = parent
        @field   = field
        @data    = parent.data[field] || []
        @_block  = block
      end

      def execute
        if @data.is_a?(Array)
          instance_eval(&@_block)
        else
          logger.error "Ignoring collection '#{field}' as not availble in document"
        end
      end

      #### DSL Methods

      def table(name, opts = {}, &block)
        # 1. Perform a delete of all current rows with matching base primary key
        dataset = handler.changes.database[name]
        dataset.where(base.key_filter).delete

        # 2. Create new rows using TableRow
        id = parent.id
        data.each_with_index do |item, i|
          TableRow.new(handler, name, id, item, :parent => self, :skip_find => true, &block).execute
        end
      end

      #### Support Methods

      def document
        @parent.document
      end

      def base
        @parent ? @parent.base : self
      end

      def logger
        CouchTap.logger
      end

    end
  end
end

