
module CouchTap
  class QueryBuffer
    include Enumerable

    def initialize()
      @buffer = {}
      @size = 0
    end

    def insert(entity, top_level, id, attributes)
      get_or_create(entity, top_level).insert(id, attributes)
      @size += 1
    end

    def delete(entity, top_level, key, id)
      get_or_create(entity, top_level).delete(key, id)
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
      @inserts = top_level ? {} : []
      @name = name
    end

    def insert(id, data)
      if @top_level
        @inserts[id] = data
      else
        @inserts << data
      end
    end

    def delete(key, id)
      if @primary_key && @primary_key != key
        raise "More than one primary key used for deletion at #{@name}: [#{@primary_key}, #{key}]"
      end
      @primary_key = key
      @deletes << id
      if top_level
        @inserts.delete id
      end
    end

    def deletes
      @deletes.to_a
    end

    def insert_values(keys)
      (@top_level ? @inserts.values : @inserts).map do |data|
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

