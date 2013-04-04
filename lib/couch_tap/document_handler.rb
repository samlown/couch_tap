module CouchTap

  class DocumentHandler

    attr_reader :changes, :filter, :mode
    attr_accessor :id, :document

    def initialize(changes, filter = {}, &block)
      @changes  = changes
      @filter   = filter
      @_block   = block
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
        Destroyers::Table.new(self, name, opts, &block).execute
      else
        Builders::Table.new(self, name, opts, &block).execute
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

    def insert(document)
      @mode = :insert
      self.document = document
      instance_eval(&@_block)
    end

    def delete(document)
      @mode = :delete
      self.document = document
      instance_eval(&@_block)
    end

    def schema(name)
      changes.schema(name)
    end

    def database
      changes.database
    end

  end

end
