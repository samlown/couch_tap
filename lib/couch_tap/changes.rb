
module CouchTap
  class Changes

    COUCHDB_HEARTBEAT  = 30
    INACTIVITY_TIMEOUT = 70
    RECONNECT_TIMEOUT  = 15

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
      @http     = HTTPClient.new

      logger.info "Connected to CouchDB: #{info['db_name']}"

      # Prepare the definitions
      instance_eval(&block)
    end

    #### DSL

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

    #### END DSL

    def schema(name)
      @schemas[name.to_sym] ||= Schema.new(database, name)
    end

    # Start listening to the CouchDB changes feed. Must be called from
    # a EventMachine run block for the HttpRequest to take control.
    # By this stage we should have a sequence id so we know where to start from
    # and all the filters should have been prepared.
    def start
      prepare_parser
      perform_request
    end

    protected

    def perform_request
      logger.info "#{source.name}: listening to changes feed from seq: #{seq}"

      url = File.join(source.root, '_changes')
      uri = URI.parse(url)
      # Authenticate?
      if uri.user.present? && uri.password.present?
        @http.set_auth(source.root, uri.user, uri.password)
      end

      # Make sure the request has the latest sequence
      query = {:since => seq, :feed => 'continuous', :heartbeat => COUCHDB_HEARTBEAT * 1000}
      
      # Perform the actual request for chunked content
      @http.get_content(url, query) do |chunk|
        # logger.debug chunk.strip
        @parser << chunk
      end

    rescue HTTPClient::ConnectTimeoutError, HTTPClient::TimeoutError => e
      logger.error "#{source.name}: connection failed: #{e.message}, attempting to reconnect in #{RECONNECT_TIMEOUT}s..."
      wait RECONNECT_TIMEOUT
      retry
    end

    def prepare_parser
      @parser = Yajl::Parser.new
      @parser.on_parse_complete = method(:process_row)
      @parser
    end

    def process_row(row)
      id  = row['id']

      # Sometimes CouchDB will send an update to keep the connection alive
      if id
        seq = row['seq']

        # Wrap the whole request in a transaction
        database.transaction do
          if row['deleted']
            # Delete all the entries
            logger.info "#{source.name}: received DELETE seq. #{seq} id: #{id}"
            handlers.each{ |handler| handler.delete('_id' => id) }
          else
            logger.info "#{source.name}: received CHANGE seq. #{seq} id: #{id}"
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
        logger.info "#{source.name}: received last seq: #{row['last_seq']}"
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
      row = database[:couch_sequence].where(:name => source.name).first
      self.seq = (row ? row[:seq] : 0)
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
