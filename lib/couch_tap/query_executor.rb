
require 'couch_tap/query_buffer'

module CouchTap
  class QueryExecutor

    attr_reader :database, :seq

    def initialize(name, data)
      logger.debug "Connecting to db at #{data.fetch :db}"
      @database = Sequel.connect(data.fetch(:db))
      @database.loggers << logger
      @database.sql_log_level = :debug

      @batch_size = data.fetch(:batch_size, 1)
      logger.debug "Batch size set to #{@batch_size}"

      @buffer = QueryBuffer.new
      @ready_to_run = false
      @processing_row = false
      @schemas = {}
      @name = name

      @seq = find_or_create_sequence_number(name)
      logger.info "QueryExecutor successfully initialised with sequence: #{@seq}"
    end

    def insert(db, top_level, id, attributes)
      raise "Cannot insert outside a row" unless @processing_row
      logger.info "Inserting a #{top_level ? 'top_level' : 'child' } record with id #{id} into #{db}"
      size = @buffer.insert(db, top_level, id, attributes) 
      trigger_batch(size)
    end

    def delete(db, top_level, filter)
      raise "Cannot delete outside a row" unless @processing_row
      logger.info "Deleting a #{top_level ? 'top_level' : 'child' } record with filter #{filter} from #{db}"
      size =  @buffer.delete(db, top_level, filter.keys.first, filter.values.first)
      trigger_batch(size)
    end

    def row(seq, &block)
      @seq = seq
      logger.debug "Processing document with sequence: #{@seq}"
      @processing_row = true
      yield
      @processing_row = false
      if @ready_to_run
        logger.info "Starting batch!"
        @database.transaction do
          @buffer.each do |entity|
            logger.debug "Processing queries for #{entity.name}"
            if entity.any_delete?
              t0 = Time.now
              database[entity.name].where({ entity.primary_key => entity.deletes }).delete
              logger.info "#{entity.name}: #{entity.deletes.size} rows deleted in #{(Time.now - t0) * 1000} ms."
            end
            if entity.any_insert?
              t0 = Time.now
              keys = columns(entity.name)
              values = entity.insert_values(keys)
              database[entity.name].import(keys, values)
              logger.info "#{entity.name}:  #{values.size} rows inserted in #{(Time.now - t0) * 1000} ms."
            end
          end
        end

        logger.debug "Changes applied, updating sequence number now to #{@seq}"
        update_sequence(seq)
        logger.info "#{@name}'s new sequence: #{seq}"

        logger.debug "Clearing buffer..."
        @buffer.clear
        @ready_to_run = false
      end
    end

    private

    def trigger_batch(size)
      logger.debug "New buffer's size: #{size}"
      if size >= @batch_size
        logger.info "Buffer's size: #{size} reached max size: #{@batch_size}. Triggering batch!!"
        @ready_to_run = true
      end
    end

    # TODO unite this cache and the one in changes.rb:48
    # in a common one
    def columns(table_name)
      schema = @schemas[table_name] ||= Schema.new(database, table_name)
      schema.column_names
    end

    def find_or_create_sequence_number(name)
      create_sequence_table(name) unless database.table_exists?(:couch_sequence)
      database[:couch_sequence].where(:name => name).first[:seq]
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
