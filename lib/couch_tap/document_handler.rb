module CouchTap

  class DocumentHandler

    attr_reader :changes
    attr_accessor :document

    def initialize(changes, &block)
      @changes  = changes
      @_block   = block
    end

    def table(name, &block)
      TableRow.new(self, name, document, {}, &block).save
    end

    def execute(document)
      self.document = document
      instance_eval(&@_block)
    end

  end

  def logger
    CouchTap.logger
  end

end
