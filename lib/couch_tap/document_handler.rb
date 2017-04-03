module CouchTap

  class DocumentHandler

    attr_reader :changes, :filter, :mode
    attr_accessor :id, :document, :operations_queue

    def initialize(changes, filter = {}, &block)
      @changes  = changes
      @filter   = filter
      @_block   = block
      @mode     = nil
    end

    def handles?(doc)
      @filter.each do |k,v|
        return false if doc[k.to_s] != v
      end
      true
    end

    ### START DSL

    # Handle a table definition.
    def table(name, opts = {}, &block)
      if @mode == :delete
        Destroyers::Table.new(self, name, opts, &block).execute(operations_queue)
      elsif @mode == :insert
        Builders::Table.new(self, name, opts, &block).execute(operations_queue)
      end
    end

    ### END DSL

    def handler
      self
    end

    def primary_keys
      []
    end

    def key_filter
      {}
    end

    def id
      document['_id']
    end

    def insert(document, operations_queue)
      @mode = :insert
      self.document = document
      self.operations_queue = operations_queue 
      instance_eval(&@_block)
    end

    def delete(document, query_executor)
      @mode = :delete
      self.document = document
      self.operations_queue = operations_queue 
      instance_eval(&@_block)
    end

    def schema(name)
      changes.schema(name)
    end
  end
end
