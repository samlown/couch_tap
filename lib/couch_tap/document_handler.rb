module CouchTap

  class DocumentHandler

    attr_reader :changes, :filter
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

    ### DSL

    # Handle a table definition.
    def table(name, opts = {}, &block)
      TableRow.new(self, name, id, document, opts, &block).execute
    end

    ### END DSL

    def add(id, document)
      self.id       = id
      self.document = document
      instance_eval(&@_block)
    end

    def drop(id)
      self.id       = id
      self.document = nil
      instance_eval(&@_block)
    end

  end

end
