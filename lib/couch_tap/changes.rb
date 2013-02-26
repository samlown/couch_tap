
module CouchTap
  class Changes

    attr_accessor :source, :database, :schemas

    # Start a new Changes instance by connecting to the provided
    # CouchDB to see if the database exists.
    def initialize(opts = "", &block)
      @schemas = {}
      @_document_filters = []
      @source = CouchRest.database(opts)
      info = @source.info
      puts "Connected to CouchDB: #{info}"
      self.instance_eval(&block)
    end

    def database(opts)
      @database = Sequel.connect(opts)
    end

    def filter(filter = {}, &block)
      @_document_filters << [
        filter,
        &block
      ]
    end

    def schema(name)
      @schemas[name.to_sym] ||= Schema.new(database, name)
    end

    # Start listening to the incoming changes feed
    def start

    end

    protected

    def find_document_filter_block(document)
      block = nil
      @_document_filters.each do |row|
        row[0].each do |k,v|
          block = (document[k.to_s] == v ? row[1] : nil)
        end
      end
      block
    end


  end
end
