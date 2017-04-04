
module CouchTap
  class Changes
    COUCHDB_HEARTBEAT  = 30
    INACTIVITY_TIMEOUT = 70
    RECONNECT_TIMEOUT  = 15

    attr_reader :source, :schemas, :handlers

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

    def database(cfg)
      @database_config = cfg
    end

    def document(filter = {}, &block)
      @handlers << DocumentHandler.new(self, filter, &block)
    end

    #### END DSL

    def schema(name)
      @schemas[name.to_sym] ||= Schema.new(@query_executor.database, name)
    end

    # Start listening to the CouchDB changes feed. Must be called from
    # a EventMachine run block for the HttpRequest to take control.
    # By this stage we should have a sequence id so we know where to start from
    # and all the filters should have been prepared.
    def start
      raise "Cannot work without a DB destination!!" unless @database_config
      prepare_consumer
      prepare_parser
      perform_request
    end

    def seq
      @query_executor.seq
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
      query = {:since => seq, :feed => 'continuous', :heartbeat => COUCHDB_HEARTBEAT * 1000,
               :include_docs => true}

      while true do
        # Perform the actual request for chunked content
        @http.get_content(url, query) do |chunk|
          # logger.debug chunk.strip
          @parser << chunk
        end
        logger.error "#{source.name}: connection ended, attempting to reconnect in #{RECONNECT_TIMEOUT}s..."
        wait RECONNECT_TIMEOUT
      end

    rescue HTTPClient::TimeoutError, HTTPClient::BadResponseError => e
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
      # Sometimes CouchDB will send an update to keep the connection alive
      if id  = row['id']
        logger.debug "Processing Document with id #{id} in #{source.name}"
        # Wrap the whole request in a transaction
        @query_executor.row row['seq'] do
          if row['deleted']
            # Delete all the entries
            handlers.each{ |handler| handler.delete({ '_id' => id }, @operations_queue) }
          else
            doc = row['doc']
            find_document_handlers(doc).each do |handler|
              # Delete all previous entries of doc, then re-create
              handler.delete(doc, @operations_queue)
              handler.insert(doc, @operations_queue)
            end
          end
        end # transaction
      elsif row['last_seq']
        logger.info "#{source.name}: received last seq: #{row['last_seq']}"
      end
    end

    def prepare_consumer
      @operations_queue = CouchTap::OperationsQueue.new
      @query_executor = CouchTap::QueryExecutor.new(source.name, @operations_queue, @database_config)
    end

    def find_document_handlers(document)
      @handlers.reject{ |row| !row.handles?(document) }
    end

    def logger
      CouchTap.logger
    end

  end
end
