require "retryable"

module CouchTap
  class Changes
    COUCHDB_HEARTBEAT  = 30
    INACTIVITY_TIMEOUT = 70
    RECONNECT_TIMEOUT  = 15

    IDLE = 0
    RUNNING = 1
    STOPPED = 2

    attr_reader :source, :schemas, :handlers, :query_executor

    # Start a new Changes instance by connecting to the provided
    # CouchDB to see if the database exists.
    def initialize(opts, &block)
      raise "Block required for changes!" unless block_given?

      @schemas  = {}
      @handlers = []
      @source   = CouchRest.database(opts.fetch(:couch_db))
      @metrics  = Metrics.new(couch_db: @source.name)
      @http     = HTTPClient.new

      @timeout  = opts.fetch(:timeout, 60)

      logger.info "Connected to CouchDB: #{@source.info['db_name']}"

      @status = IDLE

      # Prepare the definitions
      instance_eval(&block)
    end

    #### DSL

    def database(cfg)
      @batch_size = cfg.fetch(:batch_size)
      @operations_queue = CouchTap::OperationsQueue.new(@batch_size * 2)
      @query_executor = CouchTap::QueryExecutor.new(source.name, @operations_queue, @metrics, cfg.merge(timeout: @timeout))
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
      raise "Cannot work without a DB destination!!" unless @query_executor
      start_consumer
      prepare_parser
      start_producer
    end

    def stop
      @status = STOPPED
      stop_timer
      stop_consumer
    end

    def stop_consumer
      @query_executor.stop
      @consumer.join
    end

    def start_producer
      reload_seq_from_db
      @status = RUNNING
      reprocess
      start_timer
      live_processing
    end

    def stop_timer
      @timer.wait
    end

    protected

    def changes_url
      url = File.join(source.root, '_changes')
      uri = URI.parse(url)
      # Authenticate?
      if uri.user.present? && uri.password.present?
        @http.set_auth(source.root, uri.user, uri.password)
      end
      url
    end

    def reload_seq_from_db
      @seq = @query_executor.seq || 0
    end

    def reprocess
      @metrics.set_tag(:feed_mode, "reprocess")
      logger.info "#{source.name}: Reprocessing changes from seq: #{@seq}"

      loop do
      # Make sure the request has the latest sequence
        query = { since: @seq, feed: 'normal', heartbeat: COUCHDB_HEARTBEAT * 1000, include_docs: true, limit: @batch_size }

        logger.debug "#{source.name}: Reprocessing changes from seq #{@seq} limit: #{@batch_size}"

        data = Yajl::Parser.parse(@http.get_content(changes_url, query))
        results = data['results']
        results.each { |r| process_row r }
        @seq = data['last_seq'] if data['last_seq']
        break if results.count < @batch_size
      end
      logger.info "Reprocessing finished for #{source.name} at #{@seq}"
    end

    def live_processing
      @metrics.set_tag(:feed_mode, "live")
      retry_exception = Proc.new do |exception|
        logger.error "#{source.name}: connection failed: #{exception.message}, attempting to reconnect in #{RECONNECT_TIMEOUT}s..."
      end
      Retryable.retryable(tries: 4,
                          sleep: lambda { |n| 4**n },
                          exception_cb: retry_exception,
                          on: [HTTPClient::TimeoutError, HTTPClient::BadResponseError]) do

        logger.info "#{source.name}: listening to changes feed from seq: #{@seq}"

        while @status == RUNNING do
          # Make sure the request has the latest sequence
          query = { since: @seq, feed: 'continuous', heartbeat: COUCHDB_HEARTBEAT * 1000, include_docs: true }

          # Perform the actual request for chunked content
          @http.get_content(changes_url, query) do |chunk|
            # logger.debug chunk.strip
            @parser << chunk
            break unless @status == RUNNING
          end
          logger.error "#{source.name}: connection ended, attempting to reconnect in #{RECONNECT_TIMEOUT}s..."
          sleep RECONNECT_TIMEOUT
          reload_seq_from_db
        end
      end
    end

    def prepare_parser
      @parser = Yajl::Parser.new
      @parser.on_parse_complete = method(:process_row)
      @parser
    end

    def process_row(row)
      # Sometimes CouchDB will send an update to keep the connection alive
      if id  = row['id']
        @metrics.increment('documents_parsed')
        logger.debug "Processing Document with id #{id} in #{source.name}"
        # Wrap the whole request in a transaction
        @operations_queue.add_operation Operations::BeginTransactionOperation.new
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
        @operations_queue.add_operation Operations::EndTransactionOperation.new(row['seq'])
      elsif row['last_seq']
        logger.info "#{source.name}: received last seq: #{row['last_seq']}"
      end
    end

    def start_consumer
      @consumer = Thread.new do
        @query_executor.start
      end
      @consumer.abort_on_exception = true
    end

    def start_timer
      @timer = Timer.new @timeout do
        @operations_queue.add_operation Operations::TimerFiredSignal.new
      end
      @timer.run
    end

    def find_document_handlers(document)
      @handlers.reject{ |row| !row.handles?(document) }
    end

    def logger
      CouchTap.logger
    end
  end
end
