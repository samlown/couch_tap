
require 'couch_tap/query_buffer'

module CouchTap
  class QueryExecutor

    attr_reader :database, :seq

    def initialize(name, queue, metrics, data)
      logger.debug "Connecting to db at #{data.fetch :db}"
      @database = Sequel.connect(data.fetch(:db))
      @database.loggers << logger
      @database.sql_log_level = :debug

      @batch_size = data.fetch(:batch_size, 1)
      logger.debug "Batch size set to #{@batch_size}"

      @schemas = {}
      @name = name
      @metrics = metrics
      @queue = queue

      @buffer = QueryBuffer.new

      @transaction_open = false
      @timer_fired = false
      @last_transaction_ran_at = Time.at(0)

      @timeout_time = data.fetch(:timeout, 60)
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
          @transaction_open = true
        when Operations::EndTransactionOperation
          @transaction_open = false
          @seq = op.sequence
          if @buffer.size >= @batch_size || @timer_fired
            run_transaction(@seq)
            @timer_fired = false
          end
        when Operations::CloseQueueOperation
          logger.info "Queue closed, finishing..."
          break
        when Operations::TimerFiredSignal
          if @transaction_open
            @timer_fired = true
          else
            run_transaction(@seq)
          end
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
      if @buffer.size == 0
        logger.info "Skipping empty batch for #{@name}"
        @metrics.gauge('delay', 0)
        update_sequence(seq, Time.now)
        return
      end
      if @buffer.size < @batch_size
        # Transaction was fired by the timer
        return if (Time.now - @last_transaction_ran_at) < @timeout_time
      end
      @last_transaction_ran_at = Time.now
      logger.debug "Starting batch!"
      @metrics.increment('transactions')
      @metrics.gauge('queue.back_pressure', @queue.length)
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
              @metrics.histogram('delete.time', delta, table_name: entity.name)
              @metrics.gauge('delete.latency.unit', delta/entity.deletes.size.to_f, table_name: entity.name)
              batch_summary[entity.name] << "Deleted #{entity.deletes.size} in #{delta} ms."
              logger.debug "#{entity.name}: #{entity.deletes.size} rows deleted in #{delta} ms."
            end
            if entity.any_insert?
              keys = columns(entity.name)
              values = entity.insert_values(keys)
              delta = measure do
                database[entity.name].import(keys, values)
              end
              @metrics.histogram('insert.time', delta, table_name: entity.name)
              @metrics.gauge('insert.latency.unit', delta/values.size.to_f, table_name: entity.name)
              batch_summary[entity.name] << "Inserted #{values.size} in #{delta} ms."
              logger.debug "#{entity.name}:  #{values.size} rows inserted in #{delta} ms."
            end
          end

          logger.debug "Changes applied, updating sequence number now to #{@seq}"
          @metrics.gauge('delay', (Time.now - (@buffer.newest_updated_at || Time.now)).round)
          update_sequence(seq, @buffer.newest_updated_at)
          logger.debug "#{@name}'s new sequence: #{seq}"
        end
      end
      @metrics.histogram('transactions.time', total_timing)
      logger.info "#{(@buffer.size < @batch_size) ? 'TIMED ' : '' }Batch applied at #{@name} in #{total_timing} ms. Sequence: #{seq}"
      logger.info "Summary: #{batch_summary}"

      logger.debug "Clearing buffer"
      @buffer.clear
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

    def update_sequence(seq, last_transaction_at)
      logger.debug "Updating sequence number for #{@name} to #{seq} and timestamp #{last_transaction_at}"
      database[:couch_sequence].where(:name => @name).update(:seq => seq, :last_transaction_at => last_transaction_at)
    end

    def create_sequence_table(name)
      logger.debug "Creating :couch_sequence table..."
      database.create_table :couch_sequence do
        String :name, :primary_key => true
        Bignum :seq, :default => 0
        DateTime :created_at
        DateTime :updated_at
        DateTime :last_transaction_at
      end
      # Add first row
      database[:couch_sequence].insert(:name => name)
    end

    def logger
      CouchTap.logger
    end
  end
end
