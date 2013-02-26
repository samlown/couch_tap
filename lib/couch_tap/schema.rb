module CouchTap

  # Wrapper around a sequel table definition
  # to allow easy access to column names.
  class Schema

    attr_accessor :name, :database, :columns, :column_names

    def initialize(database, name)
      self.name         = name.to_sym
      self.database     = database
      self.columns      = {}
      self.column_names = []
      parse_schema
    end

    def dataset
      database[name]
    end

    protected

    def parse_schema
      database.schema(name).each do |row|
        column_names << row[0]
        columns[row[0]] = row[1]
      end
    end

  end

end
