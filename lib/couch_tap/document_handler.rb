module CouchTap

  class DocumentHandler

    attr_reader :changes, :filter, :mode
    attr_accessor :id, :document, :query_executor

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
        Destroyers::Table.new(self, name, opts, &block).execute(query_executor)
      elsif @mode == :insert
        Builders::Table.new(self, name, opts, &block).execute(query_executor)
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

    def insert(document, query_executor)
      @mode = :insert
      self.document = document
      self.query_executor = query_executor
      instance_eval(&@_block)
    end

    def delete(document, query_executor)
      @mode = :delete
      self.document = document
      self.query_executor = query_executor
      instance_eval(&@_block)
    end

    def schema(name)
      changes.schema(name)
    end

    def database
      query_executor.database
    end

  end

end
