
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

    attr_reader :id, :document, :handler, :name
    alias doc document

    def initialize(handler, table_name, id, document = nil, opts = {}, &block)
      @handler    = handler
      @id         = id
      @document   = document
      @name       = table_name
      @attributes = {}

      if @document
        set_existing_attributes(:id => id)
        set_columns_from_fields
      end
      instance_eval(&block) if block_given?
    end

    def column(*args)
      return unless document
      column = args.first
      field  = args.last
      if block_given?
        set_column(column, yield)
      elsif field.is_a?(Symbol)
        set_column(column, document[field.to_s])
      elsif args.length > 1
        set_column(column, field)
      end
    end

    # Not ready yet!
    #def collection(hash, &block)
    #  field = hash.keys.first
    #  table = hash[field]
    #  (document[field.to_s] || []).each do |item|
    #    TableRow.new(handler, table, :foreign_key => '', &block).execute
    #  end
    #end

    def execute
      dataset = handler.changes.database[name]
      if document.nil?
        dataset.where(:id => id).delete
      else
        if attributes[:id]
          dataset.where(:id => id).update(attributes)
        else
          set_primary_key
          dataset.insert(attributes)
        end
      end
    end

    protected

    def schema
      @schema ||= @handler.changes.schema(name)
    end

    def database
      @database ||= @handler.changes.database
    end

    def set_primary_key
      set_column(:id, id)
    end

    def set_existing_attributes(filter)
      row = database[name].where(filter).first
      attributes.update(row) unless row.nil?
    end

    # Take the document and try to automatically set the fields from the columns
    def set_columns_from_fields
      document.each do |k,v|
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

