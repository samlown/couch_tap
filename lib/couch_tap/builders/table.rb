
module CouchTap

  module Builders

    #
    # Deal with a table definition that will automatically insert
    # a new row into the table.
    #
    class Table

      attr_reader :attributes
      attr_reader :handler, :name, :data, :primary_keys

      def initialize(parent, name, data = nil, opts = {}, &block)
        @_collections = []
        @attributes   = {}

        @parent = parent
        @data   = data
        @name   = name

        # Deal with special options
        @parent     = opts[:parent]

        @primary_keys = parent.primary_keys.dup
        @primary_keys << (opts[:primary_key] || "#{@name.to_s.singluraize}_id").to_sym

        if @data
          find_existing_row_and_set_attributes unless opts[:skip_find]
          set_attributes_from_data
        end
        instance_eval(&block) if block_given?
      end

      def handler
        parent.handler
      end

      def id
        handler.id
      end

      # The document currently being handled. Should always be the
      # base document, even if dealing with collections.
      def document
        parent ? parent.document : @data
      end
      alias doc document

      def base
        parent ? parent.base : self
      end

      def key_filter
        hash = {}
        primary_keys.each do |k|
          hash[k] = attributes[k]
        end
        hash
      end

      #### DSL Methods

      def column(*args)
        return unless item
        column = args.first
        field  = args.last
        if block_given?
          set_attribute(column, yield)
        elsif field.is_a?(Symbol)
          set_attribute(column, item[field.to_s])
        elsif args.length > 1
          set_attribute(column, field)
        end
      end

      def collection(field, opts = {}, &block)
        @_collections << Collection.new(handler, self, field.to_s, &block)
      end

      #### Support Methods

      def execute
        # Perform basic data entry
        dataset = handler.changes.database[name]
        if data.nil?
          dataset.where(key_filter).delete
        else
          insert_or_update
        end

        # If there are any collections, reload the row so that
        # any IDs will be set correctly.
        if @_collections.length > 0
          find_existing_row_and_set_attributes
          @_collections.each{|collection| collection.execute}
        end
      end


      protected

      def persisted?
        @persisted
      end

      def insert_or_update
        if persisted?
          dataset.where(key_filter).update(attributes)
        else
          set_primary_key
          dataset.insert(attributes)
        end
      end

      def schema
        @schema ||= @handler.changes.schema(name)
      end

      def database
        @database ||= @handler.changes.database
      end

      def set_primary_key
        key_filter.each do |k,v|
          set_attribute(k, v)
        end
      end

      def find_existing_row_and_set_attributes
        row = database[name].where(key_filter).first
        if row.present?
          @persisted = true
          attributes.update(row)
        end
      end

      # Take the document and try to automatically set the fields from the columns
      def set_attributes_from_data
        data.each do |k,v|
          k = k.to_sym
          next if k == :_id || k == :_rev
          if schema.column_names.include?(k)
            set_attribute(k, v)
          end
        end
      end

      def set_attribute(column, value)
        attributes[column.to_sym] = value
      end


      def logger
        CouchTap.logger
      end

    end

  end
end

