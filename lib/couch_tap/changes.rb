
module CouchTap
  class Changes

    attr_reader :source, :database, :schemas, :handlers

    attr_accessor :seq

    # Start a new Changes instance by connecting to the provided
    # CouchDB to see if the database exists.
    def initialize(opts = "", &block)
      raise "Block required for changes!" unless block_given?

      @schemas  = {}
      @handlers = []
      @source   = CouchRest.database(opts)
      info      = @source.info

      logger.info "Connected to CouchDB: #{info['db_name']}"

      # Prepare the definitions
      instance_eval(&block)
    end

    # Dual-purpose method, accepts configuration of database
    # or returns a previous definition.
    def database(opts = nil)
      if opts
        @database ||= Sequel.connect(opts)
        find_or_create_sequence_number
      end
      @database
    end

    def document(filter = {}, &block)
      @handlers << DocumentHandler.new(self, filter, &block)
    end

    def schema(name)
      @schemas[name.to_sym] ||= Schema.new(database, name)
    end

    # Start listening to the CouchDB changes feed. Must be called from
    # a EventMachine run block for the HttpRequest to take control.
    # By this stage we should have a sequence id so we know where to start from
    # and all the filters should have been prepared.
    def start
      logger.info "Listening to changes feed..."

      url     = File.join(source.root, '_changes')
      query   = {:since => seq, :feed => 'continuous', :heartbeat => 30000}
      request = EventMachine::HttpRequest.new(url, :inactivity_timeout => 70)
      @http   = request.get(:query => query, :keepalive => true)
      @parser = Yajl::Parser.new
      @parser.on_parse_complete = method(:process_row)

      @http.stream do |chunk|
        logger.debug "Chunk: #{chunk.strip}"
        @parser << chunk
      end
      @http.errback do |err|
        logger.error "Connection Failed: #{err.error}"
      end
    end

    protected

    def process_row(row)
      id  = row['id']

      # Sometimes CouchDB will send an update to keep the connection alive
      if id
        seq = row['seq']

        # Wrap the whole request in a transaction
        database.transaction do
          if row['deleted']
            # Delete all the entries
            logger.info "Received delete seq. #{seq} id: #{id}"
            handlers.each{ |handler| handler.delete('_id' => id) }
          else
            logger.info "Received change seq. #{seq} id: #{id}"
            doc = fetch_document(id)
            find_document_handlers(doc).each do |handler|
              # Delete all previous entries of doc, then re-create
              handler.delete(doc)
              handler.insert(doc)
            end
          end

          update_sequence(seq)
        end # transaction

      elsif row['last_seq']
        logger.info "Received last seq: #{row['last_seq']}"
      end
    end

    def fetch_document(id)
      source.get(id)
    end

    def find_document_handlers(document)
      @handlers.reject{ |row| !row.handles?(document) }
    end

    def find_or_create_sequence_number
      create_sequence_table unless database.table_exists?(:couch_sequence)
      self.seq = database[:couch_sequence].where(:name => source.name).first[:seq]
    end

    def update_sequence(seq)
      database[:couch_sequence].where(:name => source.name).update(:seq => seq)
      self.seq = seq
    end

    def create_sequence_table
      database.create_table :couch_sequence do
        String :name, :primary_key => true
        Bignum :seq, :default => 0
        DateTime :created_at
        DateTime :updated_at
      end
      # Add first row
      database[:couch_sequence].insert(:name => source.name)
    end

    def logger
      CouchTap.logger
    end

  end
end
