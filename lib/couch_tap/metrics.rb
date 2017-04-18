
require 'datadog/statsd'

module CouchTap
  class Metrics

    def initialize(tags = {})
      @statsd = Datadog::Statsd.new('localhost', 8125)
      @tags = tags
    end

    def increment(key, value = 1, tags = {})
      if value == 1
        @statsd.increment(key, tags: build_tags(tags))
      else
        @statsd.count(key, value, tags: build_tags(tags))
      end
    end

    def histogram(key, value, tags = {})
      @statsd.histogram(key, value, tags: build_tags(tags))
    end

    private

    def build_tags(tags)
      @tags.merge(tags).map do |k, v|
        "#{k}:#{v}"
      end
    end
  end
end

