module CouchTap

  class DocumentHandler

    attr_reader :changes, :filter
    attr_accessor :document, :id

    def initialize(changes, filter, &block)
      @changes  = changes
      @filter   = filter
      @_block   = block
    end

    def handles?(doc)
      @filter.each do |k,v|
        return false if document[k.to_s] != v
      end
      true
    end

    ### DSL

    # Handle a table definition.
    def table(name, &block)
      TableRow.new(self, name, document, {}, &block).execute
    end

    ### END DSL

    def add(document)
      self.document = document
      self.id       = document['_id']
      instance_eval(&@_block)
    end

    def drop(id)
      self.document = nil
      self.id       = id
      instance_eval(&@_block)
    end

  end

end
