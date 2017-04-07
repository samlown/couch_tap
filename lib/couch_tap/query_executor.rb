
require 'couch_tap/query_buffer'

module CouchTap
  class QueryExecutor

    attr_reader :database, :seq

    def initialize(name, queue, data)
      logger.debug "Connecting to db at #{data.fetch :db}"
      @database = Sequel.connect(data.fetch(:db))
      @database.loggers << logger
      @database.sql_log_level = :debug

      @batch_size = data.fetch(:batch_size, 1)
      logger.debug "Batch size set to #{@batch_size}"

      @schemas = {}
      @name = name

      @queue = queue

      @buffer = QueryBuffer.new

      @seq = find_or_create_sequence_number(name)
      logger.info "QueryExecutor successfully initialised with sequence: #{@seq}"
    end

    def start
      while op = @queue.pop
        case op
        when Operations::InsertOperation
          @buffer.insert(op)
        when Operations::DeleteOperation
          @buffer.delete(op)
        when Operations::BeginTransactionOperation
          # Nothing
        when Operations::EndTransactionOperation
          @seq = op.sequence
          run_transaction(@seq) if @buffer.size >= @batch_size
        when Operations::CloseQueueOperation
          logger.info "Queue closed, finishing..."
          break
        else
          raise "Unknown operation #{op}"
        end
      end
    end

    def stop
      @queue.close
    end

    private

    def run_transaction(seq)
      logger.debug "Starting batch!"
      batch_summary = {}
      total_timing = measure do
        @database.transaction do
          @buffer.each do |entity|
           logger.debug "Processing queries for #{entity.name}"
            batch_summary[entity.name] ||= []
            if entity.any_delete?
              delta = measure do
                database[entity.name].where({ entity.primary_key => entity.deletes }).delete
              end
              batch_summary[entity.name] << "Deleted #{entity.deletes.size} in #{delta} ms."
              logger.debug "#{entity.name}: #{entity.deletes.size} rows deleted in #{delta} ms."
            end
            if entity.any_insert?
              keys = columns(entity.name)
              values = entity.insert_values(keys)
              delta = measure do
                database[entity.name].import(keys, values)
              end
              batch_summary[entity.name] << "Inserted #{values.size} in #{delta} ms."
              logger.debug "#{entity.name}:  #{values.size} rows inserted in #{delta} ms."
            end
          end

          logger.debug "Changes applied, updating sequence number now to #{@seq}"
          update_sequence(seq)
          logger.debug "#{@name}'s new sequence: #{seq}"
        end

        logger.info "Batch applied at #{@name} in #{total_timing} ms. Sequence: #{seq}"
        logger.info "Summary: #{batch_summary}"

        logger.debug "Clearing buffer"
        @buffer.clear
      end
    end

    def measure(&block)
      t0 = Time.now
      yield
      ((Time.now - t0) * 1000).round 2
    end

    # TODO unite this cache and the one in changes.rb:48
    # in a common one
    def columns(table_name)
      schema = @schemas[table_name] ||= Schema.new(database, table_name)
      schema.column_names
    end

    def find_or_create_sequence_number(name)
      create_sequence_table(name) unless database.table_exists?(:couch_sequence)
      row = database[:couch_sequence].where(:name => name).first
      row ? row[:seq] : 0
    end

    def update_sequence(seq)
      logger.debug "Updating sequence number for #{@name} to #{seq}"
      database[:couch_sequence].where(:name => @name).update(:seq => seq)
    end

    def create_sequence_table(name)
      logger.debug "Creating :couch_sequence table..."
      database.create_table :couch_sequence do
        String :name, :primary_key => true
        Bignum :seq, :default => 0
        DateTime :created_at
        DateTime :updated_at
      end
      # Add first row
      database[:couch_sequence].insert(:name => name)
    end

    def logger
      CouchTap.logger
    end
  end
end
