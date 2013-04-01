
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

    attr_reader :id, :data, :handler, :name, :parent

    def initialize(handler, table_name, id, data = nil, opts = {}, &block)
      @handler    = handler
      @id         = id
      @data       = data
      @name       = table_name
      @attributes = {}

      # Deal with special options
      @parent     = opts[:parent]
      @id_key     = opts[:id_key] || :id
      @index      = opts[:index] || 0
      if opts[:field]
        self.class.define_method(opts[:field].to_s.singlurize) { @data }
      end

      if @data
        find_existing_row_and_set_attributes
        set_columns_from_fields
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

    #### DSL Methods

    def column(*args)
      return unless item
      column = args.first
      field  = args.last
      if block_given?
        set_column(column, yield)
      elsif field.is_a?(Symbol)
        set_column(column, item[field.to_s])
      elsif args.length > 1
        set_column(column, field)
      end
    end

    def collection(table, opts = {}, &block)
      key   = opts[:foreign_key] || (name.to_s.singularize + '_id').to_sym
      field = opts[:field] || table
      items = opts[:source] || data[field.to_s] || []

      items.each_with_index do |item, i|
        TableRow.new(handler, table, id, item, :field => field, :id_key => key, :parent => self, :index => i, &block).execute
      end
    end


    #### Support Methods

    def execute
      dataset = handler.changes.database[name]
      if data.nil?
        dataset.where(@id_key => id).delete
      else
        insert_or_update
      end
    end


    protected

    def insert_or_update
      if attributes[:id]
        dataset.where(:id => attributes[:id]).update(attributes)
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

      set_column(:id, id)
    end

    def find_existing_row_and_set_attributes
      row = database[name].where(@id_key => id).limit(nil, @index).first
      attributes.update(row) unless row.nil?
    end

    # Take the document and try to automatically set the fields from the columns
    def set_columns_from_fields
      data.each do |k,v|
        k = k.to_sym
        next if k == :_id || k == :_rev
        if schema.column_names.include?(k)
          set_column(k, v)
        end
      end
    end

    def set_column(column, value)
      attributes[column.to_sym] = value
    end


    def logger
      CouchTap.logger
    end

  end

end

