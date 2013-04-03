
module CouchTap

  # A new row instance is created for each table contained within
  # a document definition during exectution.
  #
  # The life of a TableRow is expected to be very short as one is created
  # from the moment a new document enters the changing queue until
  # it has been successfully stored in the the database.
  #
  class TableRow

    attr_reader :attributes

    attr_reader :id, :data, :handler, :name, :parent, :primary_keys

    def initialize(handler, table_name, id, data = nil, opts = {}, &block)
      @handler    = handler
      @id         = id
      @data       = data
      @name       = table_name
      @attributes = {}
      @persisted  = false
      @_collections = []

      # Deal with special options
      @parent     = opts[:parent]

      key = (opts[:key] || "#{@name.to_s.singluraize}_id").to_sym
      @primary_keys = parent ? parent.primary_keys.clone : []
      @primary_keys << key

      # Always set the first primary key
      attributes[primary_keys.first] = id

      if @data
        find_existing_row_and_set_attributes unless opts[:skip_find]
        set_attributes_from_data
      end
      instance_eval(&block) if block_given?
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

