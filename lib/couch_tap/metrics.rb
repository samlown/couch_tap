
require 'datadog/statsd'

module CouchTap
  class Metrics
    PREFIX = 'couch_tap'

    def initialize(tags = {})
      @statsd = Datadog::Statsd.new('localhost', 8125)
      @tags = tags
    end

    def increment(key, tags = {})
      @statsd.increment(prefix_key(key), tags: build_tags(tags))
    end

    def histogram(key, value, tags = {})
      @statsd.histogram(prefix_key(key), value, tags: build_tags(tags))
    end

    def gauge(key, value, tags = {})
      @statsd.gauge(prefix_key(key), value, tags: build_tags(tags))
    end

    private

    def prefix_key(key)
      "#{PREFIX}.#{key}"
    end

    def build_tags(tags)
      @tags.merge(tags).map do |k, v|
        "#{k}:#{v}"
      end
    end
  end
end

