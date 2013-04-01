
module CouchTap

  class Collection

    attr_reader :id, :data, :handler, :name, :parent

    def initialize(parent, data = {}, opts = {}, &block)
      @parent = parent
      @data   = data
      instance_eval(&block) if block_given?
    end

    #### DSL Methods

    def table(name, opts = {}, &block)
      # 1. Perform a delete of all current rows

      # 2. Create new rows


    end

    #### Support Methods

    def base
      @parent ? @parent.base : self
    end

    def logger
      CouchTap.logger
    end

  end

end

