
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

    attr_reader :document, :handler, :name
    alias doc document

    def initialize(handler, table_name, document, opts = {}, &block)
      @handler    = handler
      @document   = document
      @name       = table_name
      @attributes = {}

      set_existing_attributes(:id => document['_id'])

      set_columns_from_fields
      instance_eval(&block)

    end

    def column(column, field)
      if block_given?
        set_column(column, yield)
      elsif field.is_a?(Symbol)
        set_column(column, document[field.to_s])
      else
        set_column(column, field)
      end
    end

    def collection(hash, &block)
      field = hash.keys.first
      table = hash[field]
      (document[field.to_s] || []).each do |item|
        TableRow.new(handler, table, :foreign_key => '', &block).save
      end
    end

    def save
      dataset = handler.changes.database[name]
      if attributes[:id]
        dataset.update(attributes).where(:id => document['_id'])
      else
        dataset.insert(attributes)
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
      set_column :id, document['_id']
    end

    def set_existing_attributes(filter)
      row = database[name].where(filter).first
      attributes.merge(row) unless row.nil?
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
      attributes[key.to_sym] = value
    end


    def logger
      CouchTap.logger
    end

  end

end

