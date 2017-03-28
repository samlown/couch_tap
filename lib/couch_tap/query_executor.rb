
require 'couch_tap/query_buffer'

module CouchTap
  class QueryExecutor

    attr_reader :database

    def initialize(data)
      @database = Sequel.connect(data.fetch(:db))
      @batch_size = data.fetch(:batch_size, 1)
      @buffer = QueryBuffer.new
      @ready_to_run = false
      @processing_row = false
      @schemas = {}
    end

    def insert(db, top_level, id, attributes)
      raise "Cannot insert outside a row" unless @processing_row
      if @buffer.insert(db, top_level, id, attributes) >= @batch_size
        @ready_to_run = true
      end
    end

    def delete(db, top_level, filter)
      raise "Cannot delete outside a row" unless @processing_row
      if @buffer.delete(db, top_level, filter.keys.first, filter.values.first) >= @batch_size
        @ready_to_run = true
      end
    end

    def update_sequence(seq)
      database[:couch_sequence].where(:name => @name).update(:seq => seq)
      return seq
    end

    def find_or_create_sequence_number(name)
      @name = name
      create_sequence_table(name) unless database.table_exists?(:couch_sequence)
      row = database[:couch_sequence].where(:name => name).first
      return (row ? row[:seq] : 0)
    end

    def row(&block)
      @processing_row = true
      seq = yield
      @processing_row = false
      if @ready_to_run
        @database.transaction do
          @buffer.each do |entity|
            if entity.any_delete?
              database[entity.name].where({ entity.primary_key => entity.deletes }).delete
              logger.info "#{entity.name}: #{entity.deletes.size} rows deleted."
            end
            if entity.any_insert?
              keys = columns(entity)
              values = entity.insert_values(keys)
              database[entity.name].import(keys, values)
              logger.info "#{entity.name}:  #{values.size} rows inserted."
            end
          end
        end

        update_sequence(seq)
        logger.info "#{@name} sequence: #{seq}"

        @buffer.clear
        @ready_to_run = false
      end
    end

    private

    # TODO unite this cache and the one in changes.rb:48
    # in a common one
    def columns(table_name)
      @schemas[table_name] ||= Schema.new(database, table_name)
    end

    def create_sequence_table(name)
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
