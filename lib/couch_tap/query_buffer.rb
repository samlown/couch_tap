
module CouchTap
  class QueryBuffer
    include Enumerable

    attr_reader :size, :newest_updated_at

    def initialize()
      @buffer = {}
      @size = 0
    end

    def insert(operation)
      get_or_create(operation.table, operation.top_level).insert(operation.id, operation.attributes)
      if operation.attributes['updated_at']
        t = Time.parse(operation.attributes['updated_at'])
        @newest_updated_at = t if @newest_updated_at.nil? || @newest_updated_at < t
      end
      @size += 1
    end

    def delete(operation)
      get_or_create(operation.table, operation.top_level).delete(operation.primary_key, operation.id)
      @size += 1
    end

    def clear
      @size = 0
      @buffer = {}
    end

    def each(&block)
      @buffer.values.each &block
    end

    private

    def get_or_create(entity, top_level)
      item = @buffer[entity] ||= Entity.new(entity, top_level)
      if item.top_level != top_level
        raise ArgumentError, "Cannot have entity #{entity} both as top level and as a dependent entity"
      end
      return item
    end
  end

  class Entity
    attr_reader :name, :primary_key, :top_level

    def initialize(name, top_level)
      @deletes = Set.new
      @primary_key = nil
      @top_level = top_level
      @inserts = {}
      @name = name
    end

    def insert(id, data)
      if @top_level
        @inserts[id] = [data]
      else
        (@inserts[id] ||= []) << data
      end
    end

    def delete(key, id)
      if @primary_key && @primary_key != key
        raise "More than one primary key used for deletion at #{@name}: [#{@primary_key}, #{key}]"
      end
      @primary_key = key
      @deletes << id
      @inserts.delete(id)
    end

    def deletes
      @deletes.to_a
    end

    def insert_values(keys)
      @inserts.values.flatten.map do |data|
        keys.map { |k| data[k] }
      end
    end

    def any_delete?
      @deletes.any?
    end

    def any_insert?
      @inserts.any?
    end
  end
end

