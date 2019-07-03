require 'logging'

module CouchTap

  module Builders

    #
    # Deal with a table definition that will automatically insert
    # a new row into the table.
    #
    class Table

      attr_reader :attributes
      attr_reader :parent, :name, :data, :primary_keys

      def initialize(parent, name, opts = {}, &block)
        @_collections = []

        @parent = parent
        @data   = opts[:data] || parent.document
        @name   = name.to_sym

        @primary_keys = parent.primary_keys.dup
        unless opts[:primary_key] === false
          @primary_keys << (opts[:primary_key] || "#{@name.to_s.singularize}_id").to_sym
        end

        # Prepare the attributes
        @attributes = {}
        set_primary_keys
        set_attributes_from_data

        instance_eval(&block) if block_given?
      end

      def handler
        parent.handler
      end

      def id
        handler.id
      end

      def document
        parent.document
      end
      alias doc document

      # Grab the latest set of values to filter with.
      # This is only relevant in sub-tables.
      def key_filter
        hash = {}
        primary_keys.each do |k|
          hash[k] = attributes[k]
        end
        hash
      end

      #### DSL Methods

      def column(*args)
        column = args.first
        field  = args.last
        if block_given?
          set_attribute(column, yield)
        elsif field.is_a?(Symbol)
          set_attribute(column, data[field.to_s])
        elsif args.length > 1
          set_attribute(column, field)
        end
      end

      def collection(field, opts = {}, &block)
        @_collections << Collection.new(self, field, opts, &block)
      end

      #### Support Methods

      def execute(operations_queue)
        # Insert the record and prepare ID for sub-tables
        operations_queue.add_operation(CouchTap::Operations::InsertOperation.new(name, parent.is_a?(DocumentHandler), primary_keys.first, id, attributes))
        
        logger.debug({"id" => handler.id,
                      "action" => "add_operation",
                      "table" => name,
                      "thread" => Thread.current[:name]})
        # TODO remove this?
        set_attribute(primary_keys.last, id) unless id.blank?

        # Now go through each collection entry
        if @_collections.length > 0
          @_collections.each do |collection|
            collection.execute(operations_queue)
          end
        end
      end


      private

      def schema
        handler.schema(name)
      end

      # Set the primary keys in the attributes so that the insert request
      # will have all it requires.
      #
      # This methods has two modes of operation to handle the first table
      # definition and sub-tables.
      #
      def set_primary_keys
        base = parent.key_filter.dup

        # Are we dealing with the first table?
        base[primary_keys.first] = id if base.empty?

        attributes.update(base)
      end

      # Take the document and try to automatically set the fields from the columns
      def set_attributes_from_data
        return unless data.is_a?(Hash) || data.is_a?(CouchRest::Document)
        data.each do |k,v|
          k = k.to_sym
          next if k == :_id || k == :_rev
          if schema.column_names.include?(k)
            set_attribute(k, v)
          end
        end
      end

      def set_attribute(name, value)
        name   = name.to_sym
        column = schema.columns[name]
        return if column.nil?
        # Perform basic typecasting to avoid errors with empty fields
        # in databases that do not support them.
        case column[:type]
        when :string
          value = value.nil? ? nil : value.to_s
        when :integer
          value = value.to_i
        when :float
          value = value.to_f
        else
          value = nil if value.to_s.empty?
        end
        attributes[name] = value
      end
      
      def logger
        Logging.logger[self]
      end
    end
  end
end

