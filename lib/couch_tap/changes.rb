
module CouchTap
  class Changes

    attr_accessor :source, :database, :schemas, :seq

    # Start a new Changes instance by connecting to the provided
    # CouchDB to see if the database exists.
    def initialize(opts = "", &block)
      @schemas = {}
      @_document_filters = []
      @source = CouchRest.database(opts)
      info = @source.info
      logger.info "Connected to CouchDB: #{info}"
      self.instance_eval(&block)
      start
    end

    def database(opts)
      @database = Sequel.connect(opts)
      find_or_create_sequence_number
    end

    def filter(filter = {}, &block)
      @_document_filters << [
        filter,
        Filter.new(self, &block)
      ]
    end

    def schema(name)
      @schemas[name.to_sym] ||= Schema.new(database, name)
    end

    # Actually start listening to the CouchDB changes feed.
    # By this stage we should have a sequence id so we know where to start from
    # and all the filters should have been prepared.
    def start
      logger.info "Listening to changes feed..."
      @source.changes :since => seq, :feed => 'continuous' do |row|
        if row['deleted']
          logger.info "Received delete seq. #{row['seq']} id: #{row['id']}"
          # Not sure what to do with this yet!
        else
          logger.info "Received change seq. #{row['seq']} id: #{row['id']}"
          doc = @source.get(row['id'])
        end
        filter = find_document_filter(doc)
        filter.execute(doc) if filter
        update_sequence(row['seq'])
      end
    end

    protected

    def find_document_filter(document)
      filter = nil
      @_document_filters.each do |row|
        row[0].each do |k,v|
          filter = (document[k.to_s] == v ? row[1] : nil)
        end
      end
      filter
    end

    def find_or_create_sequence_number
      schema = @database.schema(:couch_sequence)
      if schema.nil?
        # Try to create the sequence table
        create_sequence_table
      end
      self.seq = @database[:couch_sequence].where(:name => @source.name).first[:seq]
    end

    def update_sequence(seq)
      @database[:couch_sequence].update(:seq => seq).where(:name => @source.name)
      self.seq = seq
    end

    def create_sequence_table
      @database.create_table :couch_sequence do
        String :name, :primary_key => true
        Bignum :seq, :default => 0
        DateTime :created_at
        DateTime :updated_at
      end
      # Add first row
      @database[:couch_sequence].insert(:name => @source.name)
    end

    def logger
      CouchTap.logger
    end

  end
end
