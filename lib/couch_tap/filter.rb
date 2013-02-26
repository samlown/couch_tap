module CouchTap

  class Filter

    attr_reader :changes

    attr_accessor :document, :schema, :collection_item, :row
    alias doc document

    def initialize(changes, &block)
      @changes  = changes
      @_block   = &block
    end

    def table(name)
      self.schema  = changes.schema(name)
      self.row     = {}
      set_columns_from_fields
      yield if block_given?
      save_row
    end

    def collection(field, &block)
      (document[field.to_s] || []).each do |item|
        self.collection_item = item
        yield item
        self.collection_item = nil
      end
    end


    #### Table Methods

    def primary_key(column, field)
      value = document[field.to_s]
      @primary_key = value
      set_column(column, value)
    end

    def foreign_key(column, field)
      column(column, field)
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


    def execute(document)
      self.document = document
      instance_eval(@_block)
    end

    protected

    def set_column(key, value)
      row[key.to_sym] = value
    end

    # Take the document and try to automatically set the fields from the columns
    def set_columns_from_fields
      document.each do |k,v|
        k = k.to_sym
        k = :id if k == :_id # Primary key default
        if schema.column_names.include?(k)
          row[k] = v
        end
      end
    end

    def save_row

    end

    def find_current
      
    end

  end

end
