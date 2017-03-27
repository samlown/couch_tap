
module CouchTap
  class QueryExecutor

    attr_reader :database

    def initialize(db)
      @database = Sequel.connect(db)
    end

    def insert(db, attributes)
      database[db].insert(attributes)
    end

    def delete(db, filter)
      database[db].where(filter).delete
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

    private 

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
  end
end
